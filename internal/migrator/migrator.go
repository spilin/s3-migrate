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
	cfg      *config.Config
	s3Client *s3client.Client
	r2Client *s3client.Client
	state    *State
}

func New(cfg *config.Config, s3, r2 *s3client.Client) (*Migrator, error) {
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

	return &Migrator{cfg: cfg, s3Client: s3, r2Client: r2, state: state}, nil
}

// dirPrefix returns the S3 prefix for a directory number (e.g. 9820210 -> "000009820210/")
func (m *Migrator) dirPrefix(num int64) string {
	dir := fmt.Sprintf("%0*d", m.cfg.PadWidth, num)
	base := strings.TrimSuffix(m.cfg.S3Prefix, "/")
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

// Run executes the migration: process fixed-size batches with exact boundaries (e.g. 42400001-42500000)
func (m *Migrator) Run(ctx context.Context) error {
	batchStart := m.currentNum()
	slog.Info("Starting migration", "from", batchStart, "resumed", m.state.LastProcessedNumber > 0,
		"directory_concurrency", m.cfg.DirectoryConcurrency)

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

		batchDir := filepath.Join(m.cfg.WorkDir, fmt.Sprintf("batch_%d", time.Now().Unix()))
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
					objects, err := m.s3Client.ListObjects(gctx, prefix)
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

					dirSize, err := m.downloadDirectory(gctx, prefix, objects, batchDir)
					if err != nil {
						return fmt.Errorf("download directory %s: %w", prefix, err)
					}
					total := batchSizeBytes.Add(dirSize)
					dirs := dirsDownloaded.Add(1)
					slog.Info("Downloaded directory",
						"dir", prefix,
						"dir_size_mb", dirSize/(1024*1024),
						"batch_size_mb", total/(1024*1024),
						"dirs", dirs)
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
		m.state.LastProcessedNumber = batchEnd
		if err := m.state.Save(m.cfg.StateFile); err != nil {
			return fmt.Errorf("save state: %w", err)
		}

		os.RemoveAll(batchDir)
		if done.Load() {
			slog.Info("Migration complete", "reason", "consecutive_empty limit reached")
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
func (m *Migrator) downloadDirectory(ctx context.Context, prefix string, objects []s3client.ObjectInfo, destDir string) (int64, error) {
	concurrency := m.cfg.DownloadConcurrency
	if concurrency < 1 {
		concurrency = 10
	}
	sem := make(chan struct{}, concurrency)

	g, gctx := errgroup.WithContext(ctx)
	var totalMu sync.Mutex
	var total int64

	for _, obj := range objects {
		obj := obj
		g.Go(func() error {
			select {
			case <-gctx.Done():
				return gctx.Err()
			case sem <- struct{}{}:
				defer func() { <-sem }()
			}

			relKey := strings.TrimPrefix(obj.Key, m.cfg.S3Prefix)
			localPath := filepath.Join(destDir, relKey)
			slog.Debug("Downloading", "key", obj.Key)
			if err := m.s3Client.Download(gctx, obj.Key, localPath); err != nil {
				return fmt.Errorf("download %s: %w", obj.Key, err)
			}
			totalMu.Lock()
			total += obj.Size
			totalMu.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return 0, err
	}
	return total, nil
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

// flushBatch packs batchDir and uploads to R2 (archive named e.g. 1000001-2000000.tar.gz or .tar.zst)
func (m *Migrator) flushBatch(ctx context.Context, batchDir string, firstNum, lastNum int64, uncompressedBytes int64) (*BatchStats, error) {
	firstDir := fmt.Sprintf("%d", firstNum)
	lastDir := fmt.Sprintf("%d", lastNum)
	ext := ".tar.gz"
	if m.cfg.Compression == "zstd" {
		ext = ".tar.zst"
	}
	archiveName := fmt.Sprintf("%s-%s%s", firstDir, lastDir, ext)
	archivePath := filepath.Join(m.cfg.WorkDir, archiveName)
	defer os.Remove(archivePath)

	slog.Info("Packing batch", "archive", archiveName)
	compressStart := time.Now()
	if err := createTarCompressed(batchDir, archivePath, m.cfg.Compression); err != nil {
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

	r2Key := archiveName
	if m.cfg.S3Prefix != "" {
		r2Key = strings.TrimSuffix(m.cfg.S3Prefix, "/") + "/" + archiveName
	}
	slog.Info("Uploading to R2", "key", r2Key, "size_mb", compressedMB)
	uploadStart := time.Now()
	if err := m.r2Client.Upload(ctx, r2Key, archivePath); err != nil {
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

func createTarCompressed(srcDir, destPath, compression string) error {
	f, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer f.Close()

	var w io.Writer = f
	if compression == "zstd" {
		zw, err := zstd.NewWriter(f)
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

