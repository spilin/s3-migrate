package archivegaps

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
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"golang.org/x/sync/errgroup"

	"s3-migrate/config"
	"s3-migrate/internal/gaps"
	"s3-migrate/internal/migrator"
	"s3-migrate/internal/s3client"
)

// Same key pattern as migrate / validate-ranges / copy-archives.
var batchArchiveKeyRe = regexp.MustCompile(`(?i)^(?:(.+)/)?([0-9]+)-([0-9]+)\.tar\.(gz|zst)$`)

// Options configures archive-gaps.
type Options struct {
	NeardataBaseURL string
	NeardataAPIKey  string
	DryRun          bool
	// LogFile is JSONL path (archive_problem / archive_repaired / archive_repair_failed). If empty, uses cfg.ArchiveGapsLog.
	LogFile string
}

type archiveEntry struct {
	key   string
	first int64
	last  int64
}

type heightShardGap struct {
	Height          int64    `json:"height"`
	MissingShardIDs []string `json:"missing_shard_ids"`
}

type archiveProblemLine struct {
	Kind                     string           `json:"kind"`
	GeneratedAt              string           `json:"generated_at"`
	ArchiveKey               string           `json:"archive_key"`
	BatchStart               int64            `json:"batch_start"`
	BatchEnd                 int64            `json:"batch_end"`
	Error                    string           `json:"error,omitempty"`
	MissingHeights           []int64          `json:"missing_heights,omitempty"`
	HeightsWithMissingShards []heightShardGap `json:"heights_with_missing_shards,omitempty"`
	DryRun                   bool             `json:"dry_run,omitempty"`
}

type archiveRepairedLine struct {
	Kind        string `json:"kind"`
	GeneratedAt string `json:"generated_at"`
	ArchiveKey  string `json:"archive_key"`
	BatchStart  int64  `json:"batch_start"`
	BatchEnd    int64  `json:"batch_end"`
}

type archiveRepairFailedLine struct {
	Kind        string `json:"kind"`
	GeneratedAt string `json:"generated_at"`
	ArchiveKey  string `json:"archive_key"`
	BatchStart  int64  `json:"batch_start"`
	BatchEnd    int64  `json:"batch_end"`
	Error       string `json:"error"`
}

// neardataBlockNotFoundLine is logged when neardata returns 404 for a height (data missing from API).
type neardataBlockNotFoundLine struct {
	Kind         string `json:"kind"`
	GeneratedAt  string `json:"generated_at"`
	ArchiveKey   string `json:"archive_key"`
	BatchStart   int64  `json:"batch_start"`
	BatchEnd     int64  `json:"batch_end"`
	Height       int64  `json:"height"`
	NeardataURL  string `json:"neardata_url"` // path only; apiKey omitted
}

type archiveRepairSkippedIncompleteLine struct {
	Kind                     string           `json:"kind"`
	GeneratedAt              string           `json:"generated_at"`
	ArchiveKey               string           `json:"archive_key"`
	BatchStart               int64            `json:"batch_start"`
	BatchEnd                 int64            `json:"batch_end"`
	MissingHeights           []int64          `json:"missing_heights,omitempty"`
	HeightsWithMissingShards []heightShardGap `json:"heights_with_missing_shards,omitempty"`
	Reason                   string           `json:"reason,omitempty"`
}

type jsonlLogger struct {
	path string
	mu   sync.Mutex
}

func newJSONLLogger(path string) *jsonlLogger {
	return &jsonlLogger{path: strings.TrimSpace(path)}
}

func (j *jsonlLogger) appendLine(v interface{}) {
	if j.path == "" {
		return
	}
	line, err := json.Marshal(v)
	if err != nil {
		slog.Warn("archive-gaps log marshal failed", "err", err)
		return
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	dir := filepath.Dir(j.path)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			slog.Warn("archive-gaps log mkdir failed", "path", dir, "err", err)
			return
		}
	}
	f, err := os.OpenFile(j.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		slog.Warn("archive-gaps log open failed", "path", j.path, "err", err)
		return
	}
	defer f.Close()
	if _, err := f.Write(append(line, '\n')); err != nil {
		slog.Warn("archive-gaps log write failed", "path", j.path, "err", err)
	}
}

// Run lists batch archives on the destination bucket, downloads each (within optional [start_from, stop_at] scope),
// verifies block heights and shard files, logs every problematic archive to JSONL, and optionally refetches from
// neardata, repacks, and uploads over the same object key.
func Run(ctx context.Context, cfg *config.Config, dest *s3client.Client, opts Options) error {
	logPath := strings.TrimSpace(opts.LogFile)
	if logPath == "" {
		logPath = cfg.ArchiveGapsLog
	}
	if logPath == "" {
		return fmt.Errorf("archive-gaps requires a log file: set --log-file or archive_gaps.log_file in config")
	}
	jl := newJSONLLogger(logPath)

	prefix := strings.TrimSuffix(cfg.ArchivePrefix, "/")
	listPrefix := prefix
	if listPrefix != "" {
		listPrefix += "/"
	}

	slog.Info("archive-gaps: listing destination archives", "bucket", cfg.DestB2Bucket(), "prefix", listPrefix)
	objects, err := dest.ListObjects(ctx, listPrefix)
	if err != nil {
		return fmt.Errorf("list objects under %q: %w", listPrefix, err)
	}

	entries := filterArchives(cfg, objects)
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].first == entries[j].first {
			return entries[i].last < entries[j].last
		}
		return entries[i].first < entries[j].first
	})

	httpClient := &http.Client{Timeout: 120 * time.Second}
	stagingRoot := filepath.Join(cfg.WorkDir, ".archive-gaps-staging")
	if err := os.MkdirAll(stagingRoot, 0755); err != nil {
		return err
	}

	var processed int
	for _, ent := range entries {
		if !archiveOverlapsScope(ent.first, ent.last, cfg.StartFrom, cfg.StopAt) {
			continue
		}
		processed++
		if err := processOneArchive(ctx, cfg, dest, httpClient, jl, stagingRoot, ent, opts); err != nil {
			return err
		}
	}

	slog.Info("archive-gaps done", "archives_processed", processed, "batch_archives_found", len(entries))
	return nil
}

func filterArchives(cfg *config.Config, objects []s3client.ObjectInfo) []archiveEntry {
	prefix := strings.TrimSuffix(cfg.ArchivePrefix, "/")
	var out []archiveEntry
	for _, obj := range objects {
		key := obj.Key
		if key == "" {
			continue
		}
		m := batchArchiveKeyRe.FindStringSubmatch(key)
		if m == nil {
			continue
		}
		keyPrefix := strings.TrimSuffix(m[1], "/")
		if prefix != "" && keyPrefix != prefix {
			continue
		}
		if prefix == "" && keyPrefix != "" {
			continue
		}
		first, err1 := strconv.ParseInt(m[2], 10, 64)
		last, err2 := strconv.ParseInt(m[3], 10, 64)
		if err1 != nil || err2 != nil || first < 0 || last < first {
			continue
		}
		out = append(out, archiveEntry{key: key, first: first, last: last})
	}
	return out
}

func archiveOverlapsScope(batchStart, batchEnd, startFrom, stopAt int64) bool {
	if batchEnd < startFrom {
		return false
	}
	if stopAt > 0 && batchStart > stopAt {
		return false
	}
	return true
}

func compressionFromKey(key string) string {
	if strings.HasSuffix(strings.ToLower(key), ".tar.zst") {
		return "zstd"
	}
	return "gzip"
}

func processOneArchive(
	ctx context.Context,
	cfg *config.Config,
	dest *s3client.Client,
	httpClient *http.Client,
	jl *jsonlLogger,
	stagingRoot string,
	ent archiveEntry,
	opts Options,
) error {
	stage := filepath.Join(stagingRoot, fmt.Sprintf("%d-%d", ent.first, ent.last))
	if err := os.RemoveAll(stage); err != nil {
		return fmt.Errorf("clear staging %s: %w", stage, err)
	}
	if err := os.MkdirAll(stage, 0755); err != nil {
		return err
	}
	defer func() {
		if err := os.RemoveAll(stage); err != nil {
			slog.Warn("archive-gaps: remove staging failed", "dir", stage, "err", err)
		}
	}()

	localArchive := filepath.Join(stage, filepath.Base(ent.key))
	slog.Info("archive-gaps: downloading", "key", ent.key)
	if err := dest.Download(ctx, ent.key, localArchive); err != nil {
		jl.appendLine(archiveRepairFailedLine{
			Kind:        "archive_repair_failed",
			GeneratedAt: time.Now().UTC().Format(time.RFC3339),
			ArchiveKey:  ent.key,
			BatchStart:  ent.first,
			BatchEnd:    ent.last,
			Error:       fmt.Sprintf("download: %v", err),
		})
		return nil // continue other archives
	}

	comp := compressionFromKey(ent.key)
	extractDir := filepath.Join(stage, "extract")
	if err := extractFullTarArchive(localArchive, extractDir, comp); err != nil {
		jl.appendLine(archiveProblemLine{
			Kind:        "archive_problem",
			GeneratedAt: time.Now().UTC().Format(time.RFC3339),
			ArchiveKey:  ent.key,
			BatchStart:  ent.first,
			BatchEnd:    ent.last,
			Error:       fmt.Sprintf("extract: %v", err),
			DryRun:      opts.DryRun,
		})
		jl.appendLine(archiveRepairFailedLine{
			Kind:        "archive_repair_failed",
			GeneratedAt: time.Now().UTC().Format(time.RFC3339),
			ArchiveKey:  ent.key,
			BatchStart:  ent.first,
			BatchEnd:    ent.last,
			Error:       fmt.Sprintf("extract: %v", err),
		})
		return nil
	}

	relPrefix, heights, err := gaps.ScanBatchHeights(extractDir, cfg.PadWidth, ent.first, ent.last)
	if err != nil {
		jl.appendLine(archiveProblemLine{
			Kind:        "archive_problem",
			GeneratedAt: time.Now().UTC().Format(time.RFC3339),
			ArchiveKey:  ent.key,
			BatchStart:  ent.first,
			BatchEnd:    ent.last,
			Error:       fmt.Sprintf("scan layout: %v", err),
			DryRun:      opts.DryRun,
		})
		jl.appendLine(archiveRepairFailedLine{
			Kind:        "archive_repair_failed",
			GeneratedAt: time.Now().UTC().Format(time.RFC3339),
			ArchiveKey:  ent.key,
			BatchStart:  ent.first,
			BatchEnd:    ent.last,
			Error:       fmt.Sprintf("scan layout: %v", err),
		})
		return nil
	}

	var missingHeights []int64
	for h := ent.first; h <= ent.last; h++ {
		if !heights[h] {
			missingHeights = append(missingHeights, h)
		}
	}

	shardGaps, err := findMissingShards(extractDir, relPrefix, cfg, ent.first, ent.last)
	if err != nil {
		jl.appendLine(archiveProblemLine{
			Kind:        "archive_problem",
			GeneratedAt: time.Now().UTC().Format(time.RFC3339),
			ArchiveKey:  ent.key,
			BatchStart:  ent.first,
			BatchEnd:    ent.last,
			Error:       fmt.Sprintf("shard scan: %v", err),
			DryRun:      opts.DryRun,
		})
		jl.appendLine(archiveRepairFailedLine{
			Kind:        "archive_repair_failed",
			GeneratedAt: time.Now().UTC().Format(time.RFC3339),
			ArchiveKey:  ent.key,
			BatchStart:  ent.first,
			BatchEnd:    ent.last,
			Error:       fmt.Sprintf("shard scan: %v", err),
		})
		return nil
	}

	if len(missingHeights) == 0 && len(shardGaps) == 0 {
		slog.Info("archive-gaps: archive OK", "key", ent.key, "range", fmt.Sprintf("%d-%d", ent.first, ent.last))
		return nil
	}

	jl.appendLine(archiveProblemLine{
		Kind:                     "archive_problem",
		GeneratedAt:              time.Now().UTC().Format(time.RFC3339),
		ArchiveKey:               ent.key,
		BatchStart:               ent.first,
		BatchEnd:                 ent.last,
		MissingHeights:           missingHeights,
		HeightsWithMissingShards: shardGaps,
		DryRun:                   opts.DryRun,
	})

	if opts.DryRun {
		slog.Info("archive-gaps: dry-run, not repairing", "key", ent.key,
			"missing_heights", len(missingHeights), "heights_with_shard_gaps", len(shardGaps))
		return nil
	}

	toRefetch := make(map[int64]struct{})
	for _, h := range missingHeights {
		toRefetch[h] = struct{}{}
	}
	for _, g := range shardGaps {
		toRefetch[g.Height] = struct{}{}
	}

	concurrency := cfg.DownloadConcurrency
	if concurrency < 1 {
		concurrency = 10
	}
	sem := make(chan struct{}, concurrency)
	eg, egCtx := errgroup.WithContext(ctx)
	for h := range toRefetch {
		h := h
		eg.Go(func() error {
			select {
			case <-egCtx.Done():
				return egCtx.Err()
			case sem <- struct{}{}:
				defer func() { <-sem }()
			}
			if err := gaps.FillHeightFromNeardata(egCtx, httpClient, opts.NeardataBaseURL, opts.NeardataAPIKey,
				extractDir, relPrefix, cfg.PadWidth, h); err != nil {
				if errors.Is(err, gaps.ErrNeardataNotFound) {
					jl.appendLine(neardataBlockNotFoundLine{
						Kind:        "neardata_block_not_found",
						GeneratedAt: time.Now().UTC().Format(time.RFC3339),
						ArchiveKey:  ent.key,
						BatchStart:  ent.first,
						BatchEnd:    ent.last,
						Height:      h,
						NeardataURL: gaps.NeardataBlockURL(opts.NeardataBaseURL, h, ""),
					})
					slog.Info("archive-gaps: neardata 404 for height, skipping", "key", ent.key, "height", h)
					return nil
				}
				return fmt.Errorf("height %d: %w", h, err)
			}
			slog.Info("archive-gaps: refetched height from neardata", "key", ent.key, "height", h)
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		jl.appendLine(archiveRepairFailedLine{
			Kind:        "archive_repair_failed",
			GeneratedAt: time.Now().UTC().Format(time.RFC3339),
			ArchiveKey:  ent.key,
			BatchStart:  ent.first,
			BatchEnd:    ent.last,
			Error:       err.Error(),
		})
		return fmt.Errorf("archive %s: %w", ent.key, err)
	}

	_, heightsAfter, err := gaps.ScanBatchHeights(extractDir, cfg.PadWidth, ent.first, ent.last)
	if err != nil {
		jl.appendLine(archiveRepairFailedLine{
			Kind:        "archive_repair_failed",
			GeneratedAt: time.Now().UTC().Format(time.RFC3339),
			ArchiveKey:  ent.key,
			BatchStart:  ent.first,
			BatchEnd:    ent.last,
			Error:       fmt.Sprintf("rescan after repair: %v", err),
		})
		return fmt.Errorf("rescan %s: %w", ent.key, err)
	}
	var stillMissing []int64
	for h := ent.first; h <= ent.last; h++ {
		if !heightsAfter[h] {
			stillMissing = append(stillMissing, h)
		}
	}
	shardGapsAfter, err := findMissingShards(extractDir, relPrefix, cfg, ent.first, ent.last)
	if err != nil {
		jl.appendLine(archiveRepairFailedLine{
			Kind:        "archive_repair_failed",
			GeneratedAt: time.Now().UTC().Format(time.RFC3339),
			ArchiveKey:  ent.key,
			BatchStart:  ent.first,
			BatchEnd:    ent.last,
			Error:       fmt.Sprintf("shard rescan after repair: %v", err),
		})
		return fmt.Errorf("shard rescan %s: %w", ent.key, err)
	}
	if len(stillMissing) > 0 || len(shardGapsAfter) > 0 {
		jl.appendLine(archiveRepairSkippedIncompleteLine{
			Kind:                     "archive_repair_skipped_incomplete",
			GeneratedAt:              time.Now().UTC().Format(time.RFC3339),
			ArchiveKey:               ent.key,
			BatchStart:               ent.first,
			BatchEnd:                 ent.last,
			MissingHeights:           stillMissing,
			HeightsWithMissingShards: shardGapsAfter,
			Reason:                   "gaps remain after neardata refetch (e.g. 404); object not replaced on destination",
		})
		slog.Warn("archive-gaps: not uploading — archive still incomplete",
			"key", ent.key, "missing_heights", len(stillMissing), "shard_gap_heights", len(shardGapsAfter))
		return nil
	}

	outPath := filepath.Join(stage, filepath.Base(ent.key))
	if err := migrator.PackDirectoryToCompressedTar(extractDir, outPath, comp, cfg.CompressionLevel); err != nil {
		jl.appendLine(archiveRepairFailedLine{
			Kind:        "archive_repair_failed",
			GeneratedAt: time.Now().UTC().Format(time.RFC3339),
			ArchiveKey:  ent.key,
			BatchStart:  ent.first,
			BatchEnd:    ent.last,
			Error:       fmt.Sprintf("pack: %v", err),
		})
		return fmt.Errorf("pack %s: %w", ent.key, err)
	}
	if err := migrator.VerifyCompressedTar(outPath, comp); err != nil {
		jl.appendLine(archiveRepairFailedLine{
			Kind:        "archive_repair_failed",
			GeneratedAt: time.Now().UTC().Format(time.RFC3339),
			ArchiveKey:  ent.key,
			BatchStart:  ent.first,
			BatchEnd:    ent.last,
			Error:       fmt.Sprintf("verify packed: %v", err),
		})
		return fmt.Errorf("verify %s: %w", ent.key, err)
	}

	if err := dest.Upload(ctx, ent.key, outPath); err != nil {
		jl.appendLine(archiveRepairFailedLine{
			Kind:        "archive_repair_failed",
			GeneratedAt: time.Now().UTC().Format(time.RFC3339),
			ArchiveKey:  ent.key,
			BatchStart:  ent.first,
			BatchEnd:    ent.last,
			Error:       fmt.Sprintf("upload: %v", err),
		})
		return fmt.Errorf("upload %s: %w", ent.key, err)
	}

	jl.appendLine(archiveRepairedLine{
		Kind:        "archive_repaired",
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		ArchiveKey:  ent.key,
		BatchStart:  ent.first,
		BatchEnd:    ent.last,
	})
	slog.Info("archive-gaps: repaired and replaced object", "key", ent.key)
	return nil
}

func findMissingShards(batchDir, relPrefix string, cfg *config.Config, batchStart, batchEnd int64) ([]heightShardGap, error) {
	root := filepath.Join(batchDir, relPrefix)
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var out []heightShardGap
	for _, ent := range entries {
		if !ent.IsDir() {
			continue
		}
		base := ent.Name()
		if !isPaddedDigits(base, cfg.PadWidth) {
			continue
		}
		h, err := strconv.ParseInt(base, 10, 64)
		if err != nil || h < batchStart || h > batchEnd {
			continue
		}
		heightDir := filepath.Join(root, base)
		blockPath := filepath.Join(heightDir, "block.json")
		st, err := os.Stat(blockPath)
		if err != nil || st.IsDir() || st.Size() == 0 {
			continue
		}
		expected, err := gaps.ExpectedShardIDsFromBlockJSON(blockPath)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", blockPath, err)
		}
		if len(expected) == 0 {
			continue
		}
		var missingIDs []string
		for _, sid := range expected {
			shardPath := filepath.Join(heightDir, fmt.Sprintf("shard_%s.json", sid))
			sst, err := os.Stat(shardPath)
			if err == nil && !sst.IsDir() && sst.Size() > 0 {
				continue
			}
			missingIDs = append(missingIDs, sid)
		}
		if len(missingIDs) > 0 {
			out = append(out, heightShardGap{Height: h, MissingShardIDs: missingIDs})
		}
	}
	return out, nil
}

func isPaddedDigits(s string, width int) bool {
	if len(s) != width {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func extractFullTarArchive(archivePath, destDir, compression string) error {
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return err
	}
	f, err := os.Open(archivePath)
	if err != nil {
		return err
	}
	defer f.Close()

	var r io.Reader = f
	switch strings.ToLower(compression) {
	case "zstd":
		zr, err := zstd.NewReader(f)
		if err != nil {
			return err
		}
		defer zr.Close()
		r = zr
	default:
		gr, err := gzip.NewReader(f)
		if err != nil {
			return err
		}
		defer gr.Close()
		r = gr
	}

	tr := tar.NewReader(r)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if h == nil {
			continue
		}
		name := strings.TrimPrefix(h.Name, "./")
		if name == "" {
			continue
		}
		target, err := safeTarPath(destDir, name)
		if err != nil {
			return err
		}

		switch h.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0755); err != nil {
				return err
			}
		default:
			if !isTarRegularFile(h) {
				continue
			}
			if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
				return err
			}
			tmp := target + ".tmp"
			out, err := os.Create(tmp)
			if err != nil {
				return err
			}
			_, cErr := io.Copy(out, tr)
			closeErr := out.Close()
			if cErr != nil {
				os.Remove(tmp)
				return cErr
			}
			if closeErr != nil {
				os.Remove(tmp)
				return closeErr
			}
			if err := os.Rename(tmp, target); err != nil {
				return err
			}
		}
	}
}

func isTarRegularFile(h *tar.Header) bool {
	if h == nil {
		return false
	}
	if h.Typeflag == tar.TypeReg || h.Typeflag == tar.TypeRegA {
		return true
	}
	// Some writers leave type unset (NUL) for regular files.
	return h.Typeflag == 0 && h.Size > 0
}

func safeTarPath(destRoot, name string) (string, error) {
	rel := filepath.ToSlash(name)
	if strings.Contains(rel, "..") {
		return "", fmt.Errorf("invalid tar entry %q", name)
	}
	rel = strings.TrimPrefix(rel, "/")
	target := filepath.Join(destRoot, filepath.FromSlash(rel))
	rootClean := filepath.Clean(destRoot)
	tClean := filepath.Clean(target)
	if tClean != rootClean && !strings.HasPrefix(tClean, rootClean+string(os.PathSeparator)) {
		return "", fmt.Errorf("tar entry escapes root: %q", name)
	}
	return target, nil
}
