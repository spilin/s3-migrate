package migrator

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
	"golang.org/x/sync/errgroup"

	"s3-migrate/config"
	"s3-migrate/internal/s3client"
)

type Migrator struct {
	cfg          *config.Config
	sourceClient *s3client.Client // AWS S3 or B2 (list + download)
	destClient   *s3client.Client // R2 or B2 (upload)
	state        *State
}

// New creates a migrator. dest may be nil only when using RunDownloadOnly (download to disk without upload).
func New(cfg *config.Config, source, dest *s3client.Client) (*Migrator, error) {
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

	return &Migrator{cfg: cfg, sourceClient: source, destClient: dest, state: state}, nil
}

// dirPrefix returns the source object prefix for a directory number (e.g. 9820210 -> "000009820210/")
func (m *Migrator) dirPrefix(num int64) string {
	return dirPrefix(m.cfg, num)
}

func dirPrefix(cfg *config.Config, num int64) string {
	dir := fmt.Sprintf("%0*d", cfg.PadWidth, num)
	base := strings.TrimSuffix(cfg.SourceObjectPrefix(), "/")
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

					prefix := m.dirPrefix(n)
					objects, err := m.sourceClient.ListObjects(gctx, prefix)
					if err != nil {
						return fmt.Errorf("list objects under %s: %w", prefix, err)
					}

					if len(objects) == 0 {
						empty := consecutiveEmpty.Add(1)
						if empty >= int64(m.cfg.ConsecutiveEmpty) {
							done.Store(true)
						}
						slog.Debug("Skipping empty directory", "prefix", prefix)
						continue
					}
					consecutiveEmpty.Store(0)

					dirSize, nSkip, nFetch, err := m.downloadDirectory(gctx, prefix, objects, batchDir, downloadOnly)
					if err != nil {
						return fmt.Errorf("download directory %s: %w", prefix, err)
					}
					total := batchSizeBytes.Add(dirSize)
					dirs := dirsDownloaded.Add(1)
					if downloadOnly && nFetch == 0 && nSkip > 0 {
						slog.Info("Skipped directory (already on disk)",
							"dir", prefix,
							"files_skipped", nSkip,
							"dir_size_mb", dirSize/(1024*1024),
							"batch_size_mb", total/(1024*1024),
							"dirs", dirs)
					} else if downloadOnly && nSkip > 0 {
						slog.Info("Downloaded directory",
							"dir", prefix,
							"files_fetched", nFetch,
							"files_skipped", nSkip,
							"dir_size_mb", dirSize/(1024*1024),
							"batch_size_mb", total/(1024*1024),
							"dirs", dirs)
					} else {
						slog.Info("Downloaded directory",
							"dir", prefix,
							"dir_size_mb", dirSize/(1024*1024),
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

// downloadDirectory downloads all objects under prefix to destDir in parallel, returns total bytes
// and per-file skip/fetch counts for logging. When resumeDownload is true (download subcommand),
// an existing local file with size > 0 is not re-downloaded; 0-byte files are re-fetched.
func (m *Migrator) downloadDirectory(ctx context.Context, prefix string, objects []s3client.ObjectInfo, destDir string, resumeDownload bool) (totalBytes int64, filesSkipped int64, filesFetched int64, err error) {
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

			relKey := strings.TrimPrefix(obj.Key, m.cfg.SourceObjectPrefix())
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
			if err := m.sourceClient.Download(gctx, obj.Key, localPath); err != nil {
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
