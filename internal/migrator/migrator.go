package migrator

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
	"golang.org/x/sync/errgroup"

	"s3-migrate/config"
	"s3-migrate/internal/gaps"
	"s3-migrate/internal/s3client"
)

// Source is one ordered block data source used by migration.
type Source struct {
	Name   string
	Prefix string
	Client *s3client.Client
}

// Options configures optional migration fallbacks.
type Options struct {
	NeardataBaseURL string
	NeardataAPIKey  string
}

type Migrator struct {
	cfg        *config.Config
	sources    []Source
	destClient *s3client.Client // R2, B2, or S3-compatible destination (upload)
	state      *State
	opts       Options
	httpClient *http.Client
}

// New creates a migrator. dest may be nil only when using RunDownloadOnly (download to disk without upload).
func New(cfg *config.Config, source, dest *s3client.Client) (*Migrator, error) {
	return NewWithSources(cfg, []Source{{
		Name:   "source",
		Prefix: cfg.SourceObjectPrefix(),
		Client: source,
	}}, dest, Options{})
}

// NewWithSources creates a migrator with ordered sources. dest may be nil only for RunDownloadOnly.
func NewWithSources(cfg *config.Config, sources []Source, dest *s3client.Client, opts Options) (*Migrator, error) {
	state, err := LoadState(cfg.StateFile)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("load state: %w", err)
	}
	if state == nil {
		state = &State{}
	}

	if err := os.MkdirAll(cfg.WorkDir, 0755); err != nil {
		return nil, fmt.Errorf("create work dir: %w", err)
	}

	var cleanSources []Source
	for _, src := range sources {
		if src.Client == nil {
			continue
		}
		if strings.TrimSpace(src.Name) == "" {
			src.Name = "source"
		}
		cleanSources = append(cleanSources, src)
	}
	if len(cleanSources) == 0 {
		return nil, fmt.Errorf("migration requires at least one source client")
	}

	return &Migrator{
		cfg:        cfg,
		sources:    cleanSources,
		destClient: dest,
		state:      state,
		opts:       opts,
		httpClient: &http.Client{Timeout: 60 * time.Second},
	}, nil
}

// dirPrefix returns the source object prefix for a directory number (e.g. 9820210 -> "000009820210/")
func (m *Migrator) dirPrefix(num int64) string {
	return dirPrefix(m.cfg.SourceObjectPrefix(), m.cfg.PadWidth, num)
}

func dirPrefix(sourcePrefix string, padWidth int, num int64) string {
	dir := fmt.Sprintf("%0*d", padWidth, num)
	base := strings.TrimSuffix(sourcePrefix, "/")
	if base != "" {
		base += "/"
	}
	return base + dir + "/"
}

// currentNum returns the number to start iterating from (resume or config)
func (m *Migrator) currentNum() int64 {
	if m.state.LastProcessedNumber > 0 {
		return m.state.LastProcessedNumber + 1
	}
	return m.cfg.StartFrom
}

// archiveKey returns the destination key for a batch archive (e.g. archives/1000001-2000000.tar.zst)
func (m *Migrator) archiveKey(firstNum, lastNum int64) string {
	ext := ".tar.gz"
	if m.cfg.Compression == "zstd" {
		ext = ".tar.zst"
	}
	archiveName := fmt.Sprintf("%d-%d%s", firstNum, lastNum, ext)
	prefix := m.cfg.ArchivePrefix
	if prefix != "" {
		return prefix + "/" + archiveName
	}
	return archiveName
}

// Run executes the migration: process fixed-size batches with exact boundaries (e.g. 42400001-42500000)
func (m *Migrator) Run(ctx context.Context) error {
	if m.destClient == nil {
		return fmt.Errorf("migrate run requires a destination client; use RunDownloadOnly for download-only mode")
	}
	return m.run(ctx, false)
}

// RunDownloadOnly downloads numbered directories to local batch folders under work_dir like Run, but does not pack or upload.
// Batch directories that contain files are left on disk (path: work_dir/batch_<first>_<last>/...).
func (m *Migrator) RunDownloadOnly(ctx context.Context) error {
	return m.run(ctx, true)
}

// batchWorkDir returns a deterministic path for this batch range so retries and download resume use the same tree.
func batchWorkDir(workDir string, batchStart, batchEnd int64) string {
	return filepath.Join(workDir, fmt.Sprintf("batch_%d_%d", batchStart, batchEnd))
}

func (m *Migrator) run(ctx context.Context, downloadOnly bool) error {
	batchStart := m.currentNum()
	if downloadOnly {
		slog.Info("Starting download-only", "from", batchStart, "resumed", m.state.LastProcessedNumber > 0,
			"directory_concurrency", m.cfg.DirectoryConcurrency)
	} else {
		slog.Info("Starting migration", "from", batchStart, "resumed", m.state.LastProcessedNumber > 0,
			"directory_concurrency", m.cfg.DirectoryConcurrency)
	}

	dirConcurrency := m.cfg.DirectoryConcurrency
	if dirConcurrency < 1 {
		dirConcurrency = 20
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if m.cfg.StopAt > 0 && batchStart > m.cfg.StopAt {
			slog.Info("Reached stop_at")
			return nil
		}

		// Fixed batch boundaries: [batchStart, batchEnd] inclusive
		batchEnd := batchStart + m.cfg.BatchDirs - 1
		if m.cfg.StopAt > 0 && batchEnd > m.cfg.StopAt {
			batchEnd = m.cfg.StopAt
		}

		if !downloadOnly {
			// Skip batch if archive already exists in destination (e.g. from prior run or manual upload)
			archiveKey := m.archiveKey(batchStart, batchEnd)
			exists, err := m.destClient.Exists(ctx, archiveKey)
			if err != nil {
				return fmt.Errorf("check destination for %s: %w", archiveKey, err)
			}
			if exists {
				slog.Info("Batch already in destination, skipping",
					"range", fmt.Sprintf("%d-%d", batchStart, batchEnd),
					"key", archiveKey)
				m.state.LastProcessedNumber = batchEnd
				if err := m.state.Save(m.cfg.StateFile); err != nil {
					return fmt.Errorf("save state: %w", err)
				}
				if batchEnd >= m.cfg.StopAt && m.cfg.StopAt > 0 {
					slog.Info("Reached stop_at")
					return nil
				}
				batchStart = batchEnd + 1
				continue
			}
		}

		batchDir := batchWorkDir(m.cfg.WorkDir, batchStart, batchEnd)
		if err := os.MkdirAll(batchDir, 0755); err != nil {
			return err
		}

		downloadStart := time.Now()
		var nextInBatch atomic.Int64
		nextInBatch.Store(batchStart - 1)
		var done atomic.Bool
		var consecutiveEmpty atomic.Int64
		var batchSizeBytes atomic.Int64
		var dirsDownloaded atomic.Int64

		g, gctx := errgroup.WithContext(ctx)
		for i := 0; i < dirConcurrency; i++ {
			g.Go(func() error {
				for {
					if done.Load() {
						return nil
					}
					select {
					case <-gctx.Done():
						return gctx.Err()
					default:
					}

					n := nextInBatch.Add(1)
					if n > batchEnd {
						return nil
					}

					res, err := m.downloadHeightFromSources(gctx, n, batchDir, downloadOnly)
					if err != nil {
						return err
					}

					if !res.found {
						dirSize, fetched, err := m.fillHeightFromNeardata(gctx, batchDir, n)
						if err != nil {
							return err
						}
						if fetched {
							consecutiveEmpty.Store(0)
							total := batchSizeBytes.Add(dirSize)
							dirs := dirsDownloaded.Add(1)
							slog.Info("Downloaded directory from neardata",
								"height", n,
								"dir", m.dirPrefix(n),
								"dir_size_mb", dirSize/(1024*1024),
								"batch_size_mb", total/(1024*1024),
								"dirs", dirs)
							continue
						}

						empty := consecutiveEmpty.Add(1)
						if empty >= int64(m.cfg.ConsecutiveEmpty) {
							done.Store(true)
						}
						slog.Debug("Skipping empty directory", "height", n, "prefix", m.dirPrefix(n))
						continue
					}
					consecutiveEmpty.Store(0)

					total := batchSizeBytes.Add(res.dirSize)
					dirs := dirsDownloaded.Add(1)
					if downloadOnly && res.filesFetched == 0 && res.filesSkipped > 0 {
						slog.Info("Skipped directory (already on disk)",
							"dir", res.prefix,
							"source", res.source.Name,
							"files_skipped", res.filesSkipped,
							"dir_size_mb", res.dirSize/(1024*1024),
							"batch_size_mb", total/(1024*1024),
							"dirs", dirs)
					} else if downloadOnly && res.filesSkipped > 0 {
						slog.Info("Downloaded directory",
							"dir", res.prefix,
							"source", res.source.Name,
							"files_fetched", res.filesFetched,
							"files_skipped", res.filesSkipped,
							"dir_size_mb", res.dirSize/(1024*1024),
							"batch_size_mb", total/(1024*1024),
							"dirs", dirs)
					} else {
						slog.Info("Downloaded directory",
							"dir", res.prefix,
							"source", res.source.Name,
							"dir_size_mb", res.dirSize/(1024*1024),
							"batch_size_mb", total/(1024*1024),
							"dirs", dirs)
					}
				}
			})
		}

		if err := g.Wait(); err != nil {
			os.RemoveAll(batchDir)
			return err
		}

		// Flush batch (exact range batchStart-batchEnd)
		hasFiles, _ := hasContent(batchDir)
		if hasFiles {
			downloadSec := time.Since(downloadStart).Seconds()
			uncompressedMB := batchSizeBytes.Load() / (1024 * 1024)
			if downloadOnly {
				slog.Info("Download batch complete (kept on disk, no pack/upload)",
					"range", fmt.Sprintf("%d-%d", batchStart, batchEnd),
					"dir", batchDir,
					"uncompressed_mb", uncompressedMB,
					"download_sec", downloadSec)
				if m.cfg.StatsFile != "" {
					st := DownloadBatchStats{
						Kind:           "download_batch",
						Batch:          fmt.Sprintf("%d-%d", batchStart, batchEnd),
						LocalDir:       batchDir,
						UncompressedMB: uncompressedMB,
						DownloadSec:    downloadSec,
						DirsDownloaded: dirsDownloaded.Load(),
					}
					if err := appendDownloadBatchStats(m.cfg.StatsFile, st); err != nil {
						slog.Warn("Failed to write stats file", "err", err)
					}
				}
			} else {
				slog.Info("Packing and uploading batch",
					"range", fmt.Sprintf("%d-%d", batchStart, batchEnd),
					"uncompressed_mb", uncompressedMB)
				stats, err := m.flushBatch(ctx, batchDir, batchStart, batchEnd, batchSizeBytes.Load())
				if err != nil {
					os.RemoveAll(batchDir)
					return err
				}
				stats.Batch = fmt.Sprintf("%d-%d", batchStart, batchEnd)
				stats.DownloadSec = downloadSec
				if m.cfg.StatsFile != "" {
					if err := appendBatchStats(m.cfg.StatsFile, *stats); err != nil {
						slog.Warn("Failed to write stats file", "err", err)
					}
				}
			}
		}
		m.state.LastProcessedNumber = batchEnd
		if err := m.state.Save(m.cfg.StateFile); err != nil {
			return fmt.Errorf("save state: %w", err)
		}

		if downloadOnly && hasFiles {
			// Leave batchDir on disk for user to process offline
		} else {
			os.RemoveAll(batchDir)
		}
		if done.Load() {
			if downloadOnly {
				slog.Info("Download complete", "reason", "consecutive_empty limit reached")
			} else {
				slog.Info("Migration complete", "reason", "consecutive_empty limit reached")
			}
			return nil
		}
		batchStart = batchEnd + 1
	}
}

func hasContent(dir string) (bool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false, err
	}
	return len(entries) > 0, nil
}

type sourceDownloadResult struct {
	source       Source
	prefix       string
	dirSize      int64
	filesSkipped int64
	filesFetched int64
	found        bool
}

func (m *Migrator) downloadHeightFromSources(ctx context.Context, height int64, batchDir string, resumeDownload bool) (sourceDownloadResult, error) {
	for _, src := range m.sources {
		prefix := dirPrefix(src.Prefix, m.cfg.PadWidth, height)
		objects, err := src.Client.ListObjects(ctx, prefix)
		if err != nil {
			if s3client.IsNotFoundError(err) {
				slog.Info("Source directory missing, trying next source", "source", src.Name, "height", height, "prefix", prefix)
				continue
			}
			return sourceDownloadResult{}, fmt.Errorf("list objects from %s under %s: %w", src.Name, prefix, err)
		}
		if len(objects) == 0 {
			slog.Debug("Source has no directory", "source", src.Name, "height", height, "prefix", prefix)
			continue
		}

		dirSize, filesSkipped, filesFetched, err := m.downloadDirectory(ctx, src, objects, batchDir, resumeDownload)
		if err != nil {
			if s3client.IsNotFoundError(err) {
				slog.Info("Source object missing during download, trying next source",
					"source", src.Name, "height", height, "prefix", prefix, "err", err)
				if rmErr := os.RemoveAll(filepath.Join(batchDir, fmt.Sprintf("%0*d", m.cfg.PadWidth, height))); rmErr != nil {
					slog.Warn("Failed to remove partial height directory", "height", height, "err", rmErr)
				}
				continue
			}
			return sourceDownloadResult{}, fmt.Errorf("download directory %s from %s: %w", prefix, src.Name, err)
		}

		return sourceDownloadResult{
			source:       src,
			prefix:       prefix,
			dirSize:      dirSize,
			filesSkipped: filesSkipped,
			filesFetched: filesFetched,
			found:        true,
		}, nil
	}
	return sourceDownloadResult{}, nil
}

func (m *Migrator) fillHeightFromNeardata(ctx context.Context, batchDir string, height int64) (int64, bool, error) {
	base := strings.TrimSpace(m.opts.NeardataBaseURL)
	if base == "" {
		return 0, false, nil
	}
	if err := gaps.FillHeightFromNeardata(ctx, m.httpClient, base, m.opts.NeardataAPIKey,
		batchDir, "", m.cfg.PadWidth, height); err != nil {
		if errors.Is(err, gaps.ErrNeardataNotFound) {
			slog.Debug("Neardata has no block", "height", height, "url", gaps.NeardataBlockURL(base, height, ""))
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("fetch height %d from neardata: %w", height, err)
	}
	dir := filepath.Join(batchDir, fmt.Sprintf("%0*d", m.cfg.PadWidth, height))
	size, err := directorySize(dir)
	if err != nil {
		return 0, false, err
	}
	return size, true, nil
}

// downloadDirectory downloads all objects under prefix to destDir in parallel, returns total bytes
// and per-file skip/fetch counts for logging. When resumeDownload is true (download subcommand),
// an existing local file with size > 0 is not re-downloaded; 0-byte files are re-fetched.
func (m *Migrator) downloadDirectory(ctx context.Context, src Source, objects []s3client.ObjectInfo, destDir string, resumeDownload bool) (totalBytes int64, filesSkipped int64, filesFetched int64, err error) {
	concurrency := m.cfg.DownloadConcurrency
	if concurrency < 1 {
		concurrency = 10
	}
	sem := make(chan struct{}, concurrency)

	g, gctx := errgroup.WithContext(ctx)
	var totalMu sync.Mutex
	var total int64
	var skipped atomic.Int64
	var fetched atomic.Int64

	for _, obj := range objects {
		obj := obj
		g.Go(func() error {
			select {
			case <-gctx.Done():
				return gctx.Err()
			case sem <- struct{}{}:
				defer func() { <-sem }()
			}

			relKey := trimSourcePrefix(obj.Key, src.Prefix)
			localPath := filepath.Join(destDir, relKey)
			if resumeDownload {
				fi, err := os.Stat(localPath)
				if err == nil {
					if !fi.IsDir() && fi.Size() > 0 {
						slog.Debug("Skipping existing file", "path", localPath, "size", fi.Size())
						skipped.Add(1)
						totalMu.Lock()
						total += fi.Size()
						totalMu.Unlock()
						return nil
					}
				} else if !os.IsNotExist(err) {
					return fmt.Errorf("stat %s: %w", localPath, err)
				}
			}

			slog.Debug("Downloading", "key", obj.Key)
			if err := src.Client.Download(gctx, obj.Key, localPath); err != nil {
				return fmt.Errorf("download %s: %w", obj.Key, err)
			}
			fetched.Add(1)
			totalMu.Lock()
			total += obj.Size
			totalMu.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return 0, 0, 0, err
	}
	return total, skipped.Load(), fetched.Load(), nil
}

func trimSourcePrefix(key, prefix string) string {
	prefix = strings.TrimPrefix(strings.TrimSpace(prefix), "/")
	prefix = strings.TrimSuffix(prefix, "/")
	if prefix == "" {
		return strings.TrimPrefix(key, "/")
	}
	if key == prefix {
		return ""
	}
	return strings.TrimPrefix(strings.TrimPrefix(key, prefix+"/"), "/")
}

func directorySize(root string) (int64, error) {
	var size int64
	err := filepath.Walk(root, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("measure directory %s: %w", root, err)
	}
	return size, nil
}

// BatchStats records per-batch compression and timing
type BatchStats struct {
	Batch          string  `json:"batch"`
	UncompressedMB int64   `json:"uncompressed_mb"`
	CompressedMB   int64   `json:"compressed_mb"`
	ReductionPct   float64 `json:"reduction_pct"`
	DownloadSec    float64 `json:"download_sec"`
	CompressSec    float64 `json:"compress_sec"`
	UploadSec      float64 `json:"upload_sec"`
}

// DownloadBatchStats is a JSONL line for download-only batches (kind: download_batch).
type DownloadBatchStats struct {
	Kind           string  `json:"kind"`
	Batch          string  `json:"batch"`
	LocalDir       string  `json:"local_dir"`
	UncompressedMB int64   `json:"uncompressed_mb"`
	DownloadSec    float64 `json:"download_sec"`
	DirsDownloaded int64   `json:"dirs_downloaded"`
}

func appendBatchStats(path string, s BatchStats) error {
	line, err := json.Marshal(s)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(append(line, '\n'))
	return err
}

func appendDownloadBatchStats(path string, s DownloadBatchStats) error {
	line, err := json.Marshal(s)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(append(line, '\n'))
	return err
}

// flushBatch packs batchDir and uploads to R2 (archive named e.g. 1000001-2000000.tar.gz or .tar.zst)
func (m *Migrator) flushBatch(ctx context.Context, batchDir string, firstNum, lastNum int64, uncompressedBytes int64) (*BatchStats, error) {
	ext := ".tar.gz"
	if m.cfg.Compression == "zstd" {
		ext = ".tar.zst"
	}
	archiveName := fmt.Sprintf("%d-%d%s", firstNum, lastNum, ext)
	archivePath := filepath.Join(m.cfg.WorkDir, archiveName)
	defer os.Remove(archivePath)

	slog.Info("Packing batch", "archive", archiveName)
	compressStart := time.Now()
	if err := createTarCompressed(batchDir, archivePath, m.cfg.Compression, m.cfg.CompressionLevel); err != nil {
		return nil, fmt.Errorf("create archive: %w", err)
	}
	compressSec := time.Since(compressStart).Seconds()

	fi, err := os.Stat(archivePath)
	compressedBytes := int64(0)
	if err == nil {
		compressedBytes = fi.Size()
	}
	uncompressedMB := uncompressedBytes / (1024 * 1024)
	compressedMB := compressedBytes / (1024 * 1024)
	ratioPct := 0.0
	if uncompressedBytes > 0 {
		ratioPct = 100.0 * (1 - float64(compressedBytes)/float64(uncompressedBytes))
	}
	slog.Info("Archive created",
		"archive", archiveName,
		"uncompressed_mb", uncompressedMB,
		"compressed_mb", compressedMB,
		"reduction_pct", fmt.Sprintf("%.1f%%", ratioPct))

	slog.Info("Verifying archive integrity", "archive", archiveName)
	if err := verifyArchive(archivePath, m.cfg.Compression); err != nil {
		return nil, fmt.Errorf("archive verification failed: %w", err)
	}

	archiveKey := m.archiveKey(firstNum, lastNum)
	// Note: ChecksumSHA256 is not used - multipart uploads (files >5MB) expect per-part checksums,
	// not whole-object SHA256. B2 rejects with BadDigest. Archive verification (zstd -t) above
	// still validates integrity before upload.
	slog.Info("Uploading to destination", "key", archiveKey, "size_mb", compressedMB)
	uploadStart := time.Now()
	if err := m.destClient.Upload(ctx, archiveKey, archivePath); err != nil {
		return nil, err
	}
	uploadSec := time.Since(uploadStart).Seconds()

	return &BatchStats{
		UncompressedMB: uncompressedMB,
		CompressedMB:   compressedMB,
		ReductionPct:   ratioPct,
		CompressSec:    compressSec,
		UploadSec:      uploadSec,
	}, nil
}

// verifyArchive streams through the compressed archive to verify it's not corrupted (like zstd -t / gzip -t).
func verifyArchive(path, compression string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	var r io.Reader = f
	if compression == "zstd" {
		zr, err := zstd.NewReader(f)
		if err != nil {
			return fmt.Errorf("zstd reader: %w", err)
		}
		defer zr.Close()
		r = zr
	} else {
		gzr, err := gzip.NewReader(f)
		if err != nil {
			return fmt.Errorf("gzip reader: %w", err)
		}
		defer gzr.Close()
		r = gzr
	}

	_, err = io.Copy(io.Discard, r)
	return err
}

func createTarCompressed(srcDir, destPath, compression string, zstdLevel int) error {
	f, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer f.Close()

	var w io.Writer = f
	if compression == "zstd" {
		level := zstd.EncoderLevelFromZstd(zstdLevel)
		zw, err := zstd.NewWriter(f, zstd.WithEncoderLevel(level))
		if err != nil {
			return err
		}
		defer zw.Close()
		w = zw
	} else {
		gz := gzip.NewWriter(f)
		defer gz.Close()
		w = gz
	}

	tw := tar.NewWriter(w)
	defer tw.Close()

	return filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		rel, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}

		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		header.Name = rel
		if info.IsDir() {
			header.Name += "/"
		}

		if err := tw.WriteHeader(header); err != nil {
			return err
		}

		if !info.IsDir() {
			file, err := os.Open(path)
			if err != nil {
				return err
			}
			_, err = io.Copy(tw, file)
			file.Close()
			return err
		}
		return nil
	})
}

// PackDirectoryToCompressedTar packs srcDir into a tar.gz or tar.zst at destPath (same layout as migrate flushBatch).
func PackDirectoryToCompressedTar(srcDir, destPath, compression string, zstdLevel int) error {
	return createTarCompressed(srcDir, destPath, compression, zstdLevel)
}

// VerifyCompressedTar streams a compressed archive to verify integrity (gzip or zstd).
func VerifyCompressedTar(archivePath, compression string) error {
	return verifyArchive(archivePath, compression)
}
