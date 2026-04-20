package gaps

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"golang.org/x/sync/errgroup"

	"s3-migrate/config"
	"s3-migrate/internal/s3client"
)

const defaultNeardataBase = "https://mainnet.neardata.xyz/v0/block"

// Options configures fix-gaps HTTP and behavior.
type Options struct {
	NeardataBaseURL string // e.g. https://mainnet.neardata.xyz/v0/block (no trailing slash)
	// NeardataAPIKey, if set, is passed as ?apiKey=... to neardata.xyz.
	NeardataAPIKey string
	DryRun          bool
	// LogFile is JSONL path for gap_found / gap_filled / gap_fill_failed (empty = disabled).
	LogFile string
	// ArchiveLocalDir, if set, points to a directory holding batch archives named like run uploads
	// (e.g. 1000001-2000000.tar.zst). It is used to repair missing shard_<id>.json files.
	//
	// If empty and destination credentials are present in config, fix-gaps will download needed
	// batch archives from destination into work_dir/.archives-cache/.
	ArchiveLocalDir string
}

// Run scans work_dir for batch_<first>_<last> directories, finds missing padded height folders,
// fetches each block from neardata API, and writes block.json + shard_*.json like download_fastnear.py.
func Run(ctx context.Context, cfg *config.Config, opts Options) error {
	base := strings.TrimSuffix(strings.TrimSpace(opts.NeardataBaseURL), "/")
	if base == "" {
		base = defaultNeardataBase
	}
	apiKey := strings.TrimSpace(opts.NeardataAPIKey)

	workDir := cfg.WorkDir
	entries, err := os.ReadDir(workDir)
	if err != nil {
		return fmt.Errorf("read work_dir %q: %w", workDir, err)
	}

	client := &http.Client{Timeout: 120 * time.Second}
	logPath := strings.TrimSpace(opts.LogFile)
	jl := newJSONLLogger(logPath)

	var destClient *s3client.Client
	if cfg.UseDestB2() {
		db := cfg.Destination.B2
		c, err := s3client.NewB2Client(ctx, db.Region, db.AccessKeyID, db.SecretKey, db.Bucket)
		if err != nil {
			return err
		}
		destClient = c
		slog.Info("fix-gaps: archive repair enabled (B2 destination)", "bucket", db.Bucket)
	} else if cfg.UseDestR2() {
		dr := cfg.Destination.R2
		c, err := s3client.NewR2Client(ctx, dr.AccountID, dr.AccessKeyID, dr.SecretKey, dr.Bucket)
		if err != nil {
			return err
		}
		destClient = c
		slog.Info("fix-gaps: archive repair enabled (R2 destination)", "bucket", dr.Bucket)
	} else if cfg.UseDestS3() {
		ds := cfg.Destination.S3
		region := strings.TrimSpace(ds.Region)
		if region == "" {
			region = "us-east-1"
		}
		c, err := s3client.NewS3Client(ctx, region, strings.TrimSpace(ds.Endpoint), ds.Bucket, ds.AccessKeyID, ds.SecretKey)
		if err != nil {
			return err
		}
		destClient = c
		slog.Info("fix-gaps: archive repair enabled (S3-compatible destination)", "endpoint", ds.Endpoint, "bucket", ds.Bucket)
	}

	var batches int
	var gapsFilled int64
	var shardsRestored int64
	for _, ent := range entries {
		if !ent.IsDir() {
			continue
		}
		name := ent.Name()
		if !strings.HasPrefix(name, "batch_") {
			continue
		}
		rest := strings.TrimPrefix(name, "batch_")
		parts := strings.SplitN(rest, "_", 2)
		if len(parts) != 2 {
			continue
		}
		batchStart, err1 := strconv.ParseInt(parts[0], 10, 64)
		batchEnd, err2 := strconv.ParseInt(parts[1], 10, 64)
		if err1 != nil || err2 != nil || batchStart < 0 || batchEnd < batchStart {
			slog.Debug("Skipping non-batch directory", "name", name)
			continue
		}
		batches++

		batchDir := filepath.Join(workDir, name)
		relPrefix, existing, err := scanBatchHeights(batchDir, cfg.PadWidth, batchStart, batchEnd)
		if err != nil {
			return fmt.Errorf("batch %s: %w", name, err)
		}

		var missing []int64
		for h := batchStart; h <= batchEnd; h++ {
			if !existing[h] {
				missing = append(missing, h)
			}
		}
		if len(missing) == 0 {
			slog.Info("Batch has no block gaps", "batch", name, "range", fmt.Sprintf("%d-%d", batchStart, batchEnd))
		} else {
			slog.Info("Batch gaps", "batch", name, "missing_count", len(missing), "rel_prefix", relPrefix)
		}

		if len(missing) > 0 {
			for _, h := range missing {
				padded := fmt.Sprintf("%0*d", cfg.PadWidth, h)
				destDir := filepath.Join(batchDir, relPrefix, padded)
				jl.appendLine(gapFoundLine{
					Kind:        "gap_found",
					GeneratedAt: time.Now().UTC().Format(time.RFC3339),
					Batch:       name,
					BatchStart:  batchStart,
					BatchEnd:    batchEnd,
					Height:      h,
					DestDir:     destDir,
					DryRun:      opts.DryRun,
				})
			}
		}

		if len(missing) > 0 {
			concurrency := cfg.DownloadConcurrency
			if concurrency < 1 {
				concurrency = 10
			}
			sem := make(chan struct{}, concurrency)
			g, gctx := errgroup.WithContext(ctx)

			for _, h := range missing {
				h := h
				g.Go(func() error {
					select {
					case <-gctx.Done():
						return gctx.Err()
					case sem <- struct{}{}:
						defer func() { <-sem }()
					}
					padded := fmt.Sprintf("%0*d", cfg.PadWidth, h)
					destDir := filepath.Join(batchDir, relPrefix, padded)
					blockURL := neardataBlockURL(base, h, apiKey)
					if opts.DryRun {
						slog.Info("dry-run: would fetch gap", "height", h, "dir", destDir)
						return nil
					}
					if err := fetchAndWriteBlock(gctx, client, base, apiKey, destDir, h); err != nil {
						jl.appendLine(gapFillFailedLine{
							Kind:        "gap_fill_failed",
							GeneratedAt: time.Now().UTC().Format(time.RFC3339),
							Batch:       name,
							Height:      h,
							DestDir:     destDir,
							NeardataURL: blockURL,
							Error:       err.Error(),
						})
						return fmt.Errorf("height %d: %w", h, err)
					}
					jl.appendLine(gapFilledLine{
						Kind:        "gap_filled",
						GeneratedAt: time.Now().UTC().Format(time.RFC3339),
						Batch:       name,
						Height:      h,
						DestDir:     destDir,
						NeardataURL: blockURL,
					})
					slog.Info("Filled gap", "height", h, "dir", destDir)
					return nil
				})
			}
			if err := g.Wait(); err != nil {
				return err
			}
			gapsFilled += int64(len(missing))
		}

		restored, err := repairMissingShards(ctx, cfg, opts, destClient, jl, name, batchDir, relPrefix, batchStart, batchEnd)
		if err != nil {
			return err
		}
		shardsRestored += restored
	}

	slog.Info("fix-gaps done", "batch_dirs_scanned", batches, "heights_fetched", gapsFilled, "shards_restored", shardsRestored)
	return nil
}

func scanBatchHeights(batchDir string, padWidth int, batchStart, batchEnd int64) (relPrefix string, heights map[int64]bool, err error) {
	heights = make(map[int64]bool)
	var prefixes []string

	err = filepath.WalkDir(batchDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == batchDir {
			return nil
		}
		if !d.IsDir() {
			return nil
		}
		rel, e := filepath.Rel(batchDir, path)
		if e != nil {
			return e
		}
		base := filepath.Base(path)
		if !isPaddedDigits(base, padWidth) {
			return nil
		}
		h, e := strconv.ParseInt(base, 10, 64)
		if e != nil || h < batchStart || h > batchEnd {
			return filepath.SkipDir
		}
		blockJSON := filepath.Join(path, "block.json")
		st, e := os.Stat(blockJSON)
		if e != nil || st.IsDir() || st.Size() == 0 {
			return filepath.SkipDir
		}
		parent := filepath.Dir(rel)
		if parent == "." {
			parent = ""
		}
		heights[h] = true
		prefixes = append(prefixes, parent)
		return filepath.SkipDir
	})
	if err != nil {
		return "", nil, err
	}

	if len(prefixes) == 0 {
		return "", heights, nil
	}
	relPrefix = prefixes[0]
	for _, p := range prefixes[1:] {
		if p != relPrefix {
			return "", nil, fmt.Errorf("inconsistent path layout under batch dir (found prefixes %q and %q); fix manually", relPrefix, p)
		}
	}
	return relPrefix, heights, nil
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

type neardataEnvelope struct {
	Block  json.RawMessage   `json:"block"`
	Shards []json.RawMessage `json:"shards"`
}

func neardataBlockURL(baseURL string, height int64, apiKey string) string {
	u, err := url.Parse(strings.TrimSuffix(baseURL, "/") + "/" + strconv.FormatInt(height, 10))
	if err != nil || u == nil {
		return strings.TrimSuffix(baseURL, "/") + "/" + strconv.FormatInt(height, 10)
	}
	if apiKey != "" {
		q := u.Query()
		q.Set("apiKey", apiKey)
		u.RawQuery = q.Encode()
	}
	return u.String()
}

func shardIDString(raw json.RawMessage) (string, error) {
	if len(raw) == 0 {
		return "", fmt.Errorf("missing shard_id")
	}
	var n json.Number
	if err := json.Unmarshal(raw, &n); err == nil && n != "" {
		return n.String(), nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s, nil
	}
	var f float64
	if err := json.Unmarshal(raw, &f); err == nil {
		return strconv.FormatInt(int64(f), 10), nil
	}
	return "", fmt.Errorf("unsupported shard_id JSON: %s", string(raw))
}

func fetchAndWriteBlock(ctx context.Context, client *http.Client, baseURL, apiKey, destDir string, height int64) error {
	body, status, err := httpGetWithRetry(ctx, client, neardataBlockURL(baseURL, height, apiKey))
	if err != nil {
		return err
	}
	if status == http.StatusNotFound {
		return fmt.Errorf("neardata 404 for height %d", height)
	}
	if status != http.StatusOK {
		return fmt.Errorf("neardata status %d for height %d", status, height)
	}

	var env neardataEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return fmt.Errorf("decode JSON height %d: %w", height, err)
	}

	if err := os.MkdirAll(destDir, 0755); err != nil {
		return fmt.Errorf("mkdir %s: %w", destDir, err)
	}

	blockPath := filepath.Join(destDir, "block.json")
	if err := writeJSONIndentFile(blockPath, env.Block); err != nil {
		return err
	}

	for _, shardRaw := range env.Shards {
		var meta struct {
			ShardID json.RawMessage `json:"shard_id"`
		}
		if err := json.Unmarshal(shardRaw, &meta); err != nil {
			return fmt.Errorf("shard parse height %d: %w", height, err)
		}
		sidStr, err := shardIDString(meta.ShardID)
		if err != nil {
			return fmt.Errorf("height %d: %w", height, err)
		}
		name := fmt.Sprintf("shard_%s.json", sidStr)
		shardPath := filepath.Join(destDir, name)
		if err := writeJSONIndentFile(shardPath, shardRaw); err != nil {
			return err
		}
	}
	return nil
}

func writeJSONIndentFile(path string, raw json.RawMessage) error {
	var v interface{}
	if len(raw) == 0 {
		v = map[string]interface{}{}
	} else if err := json.Unmarshal(raw, &v); err != nil {
		return fmt.Errorf("unmarshal for %s: %w", path, err)
	}
	out, err := json.MarshalIndent(v, "", "    ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(out, '\n'), 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func httpGetWithRetry(ctx context.Context, client *http.Client, url string) ([]byte, int, error) {
	sleep := 200 * time.Millisecond
	for attempt := 0; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, 0, err
		}
		resp, err := client.Do(req)
		if err != nil {
			if attempt >= 12 {
				return nil, 0, err
			}
			slog.Warn("HTTP error, retrying", "url", url, "err", err, "attempt", attempt+1)
			select {
			case <-ctx.Done():
				return nil, 0, ctx.Err()
			case <-time.After(sleep):
			}
			if sleep < 30*time.Second {
				sleep = time.Duration(float64(sleep) * 1.3)
			}
			continue
		}
		body, rerr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if rerr != nil {
			return nil, resp.StatusCode, rerr
		}
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			if attempt >= 20 {
				return body, resp.StatusCode, fmt.Errorf("giving up after status %d", resp.StatusCode)
			}
			slog.Warn("HTTP status, retrying", "url", url, "status", resp.StatusCode, "attempt", attempt+1)
			select {
			case <-ctx.Done():
				return nil, 0, ctx.Err()
			case <-time.After(sleep):
			}
			if sleep < 30*time.Second {
				sleep = time.Duration(float64(sleep) * 1.3)
			}
			continue
		}
		return body, resp.StatusCode, nil
	}
}

type gapFoundLine struct {
	Kind        string `json:"kind"`
	GeneratedAt string `json:"generated_at"`
	Batch       string `json:"batch"`
	BatchStart  int64  `json:"batch_start"`
	BatchEnd    int64  `json:"batch_end"`
	Height      int64  `json:"height"`
	DestDir     string `json:"dest_dir"`
	DryRun      bool   `json:"dry_run,omitempty"`
}

type gapFilledLine struct {
	Kind        string `json:"kind"`
	GeneratedAt string `json:"generated_at"`
	Batch       string `json:"batch"`
	Height      int64  `json:"height"`
	DestDir     string `json:"dest_dir"`
	NeardataURL string `json:"neardata_url"`
}

type gapFillFailedLine struct {
	Kind        string `json:"kind"`
	GeneratedAt string `json:"generated_at"`
	Batch       string `json:"batch"`
	Height      int64  `json:"height"`
	DestDir     string `json:"dest_dir"`
	NeardataURL string `json:"neardata_url"`
	Error       string `json:"error"`
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
		slog.Warn("fix-gaps log marshal failed", "err", err)
		return
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	f, err := os.OpenFile(j.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		slog.Warn("fix-gaps log open failed", "path", j.path, "err", err)
		return
	}
	defer f.Close()
	if _, err := f.Write(append(line, '\n')); err != nil {
		slog.Warn("fix-gaps log write failed", "path", j.path, "err", err)
	}
}

type blockForShardCheck struct {
	Chunks []struct {
		ShardID json.RawMessage `json:"shard_id"`
	} `json:"chunks"`
}

func expectedShardIDsFromBlockJSON(path string) ([]string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var blk blockForShardCheck
	if err := json.Unmarshal(b, &blk); err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(blk.Chunks))
	var ids []string
	for _, ch := range blk.Chunks {
		s, err := shardIDString(ch.ShardID)
		if err != nil {
			return nil, err
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		ids = append(ids, s)
	}
	return ids, nil
}

func archiveKeyForBatch(cfg *config.Config, batchStart, batchEnd int64) string {
	ext := ".tar.gz"
	if strings.ToLower(cfg.Compression) == "zstd" {
		ext = ".tar.zst"
	}
	name := fmt.Sprintf("%d-%d%s", batchStart, batchEnd, ext)
	prefix := strings.TrimSuffix(cfg.ArchivePrefix, "/")
	if prefix != "" {
		return prefix + "/" + name
	}
	return name
}

func ensureBatchArchive(ctx context.Context, cfg *config.Config, opts Options, destClient *s3client.Client, batchStart, batchEnd int64) (string, string, error) {
	archiveKey := archiveKeyForBatch(cfg, batchStart, batchEnd)
	archiveFile := filepath.Base(archiveKey)

	if dir := strings.TrimSpace(opts.ArchiveLocalDir); dir != "" {
		p := filepath.Join(dir, archiveFile)
		if st, err := os.Stat(p); err == nil && !st.IsDir() && st.Size() > 0 {
			return p, archiveKey, nil
		}
		return "", archiveKey, fmt.Errorf("archive not found in archive_local_dir: %s", p)
	}
	if destClient == nil {
		return "", archiveKey, fmt.Errorf("no archive_local_dir and no destination credentials for archive download")
	}

	cacheDir := filepath.Join(cfg.WorkDir, ".archives-cache")
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return "", archiveKey, err
	}
	localPath := filepath.Join(cacheDir, archiveFile)
	if st, err := os.Stat(localPath); err == nil && !st.IsDir() && st.Size() > 0 {
		return localPath, archiveKey, nil
	}

	slog.Info("Downloading batch archive for shard repair", "key", archiveKey, "path", localPath)
	if err := destClient.Download(ctx, archiveKey, localPath); err != nil {
		return "", archiveKey, err
	}
	return localPath, archiveKey, nil
}

func extractOneFromArchive(archivePath string, compression string, wantedRelPath string, destPath string) error {
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
	want := filepath.ToSlash(wantedRelPath)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			return fmt.Errorf("file %s not found in archive", want)
		}
		if err != nil {
			return err
		}
		if h == nil || h.Name == "" || h.FileInfo().IsDir() {
			continue
		}
		if h.Name != want {
			continue
		}

		if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
			return err
		}
		tmp := destPath + ".tmp"
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
		return os.Rename(tmp, destPath)
	}
}

func repairMissingShards(
	ctx context.Context,
	cfg *config.Config,
	opts Options,
	destClient *s3client.Client,
	jl *jsonlLogger,
	batchName string,
	batchDir string,
	relPrefix string,
	batchStart int64,
	batchEnd int64,
) (int64, error) {
	var restored int64

	// Walk only directories that look like padded heights directly under relPrefix.
	root := filepath.Join(batchDir, relPrefix)
	entries, err := os.ReadDir(root)
	if err != nil {
		// If relPrefix is empty and batchDir has no dirs, treat as no-op.
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}

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

		expected, err := expectedShardIDsFromBlockJSON(blockPath)
		if err != nil {
			return restored, fmt.Errorf("parse %s: %w", blockPath, err)
		}
		if len(expected) == 0 {
			continue
		}

		for _, sid := range expected {
			shardName := fmt.Sprintf("shard_%s.json", sid)
			shardPath := filepath.Join(heightDir, shardName)
			sst, err := os.Stat(shardPath)
			if err == nil && !sst.IsDir() && sst.Size() > 0 {
				continue
			}

			archivePath, archiveKey, aerr := ensureBatchArchive(ctx, cfg, opts, destClient, batchStart, batchEnd)
			jl.appendLine(shardMissingLine{
				Kind:        "shard_missing",
				GeneratedAt: time.Now().UTC().Format(time.RFC3339),
				Batch:       batchName,
				Height:      h,
				ShardID:     sid,
				DestPath:    shardPath,
				ArchiveKey:  archiveKey,
				DryRun:      opts.DryRun,
			})
			if aerr != nil {
				jl.appendLine(shardFillFailedLine{
					Kind:        "shard_fill_failed",
					GeneratedAt: time.Now().UTC().Format(time.RFC3339),
					Batch:       batchName,
					Height:      h,
					ShardID:     sid,
					DestPath:    shardPath,
					ArchiveKey:  archiveKey,
					Error:       aerr.Error(),
				})
				return restored, aerr
			}

			if opts.DryRun {
				slog.Info("dry-run: would restore shard from archive", "height", h, "shard_id", sid, "dest", shardPath, "archive", archivePath)
				continue
			}

			relInArchive := filepath.Join(relPrefix, base, shardName)
			if err := extractOneFromArchive(archivePath, cfg.Compression, relInArchive, shardPath); err != nil {
				jl.appendLine(shardFillFailedLine{
					Kind:        "shard_fill_failed",
					GeneratedAt: time.Now().UTC().Format(time.RFC3339),
					Batch:       batchName,
					Height:      h,
					ShardID:     sid,
					DestPath:    shardPath,
					ArchiveKey:  archiveKey,
					Error:       err.Error(),
				})
				return restored, err
			}
			restored++
			jl.appendLine(shardFilledLine{
				Kind:        "shard_filled",
				GeneratedAt: time.Now().UTC().Format(time.RFC3339),
				Batch:       batchName,
				Height:      h,
				ShardID:     sid,
				DestPath:    shardPath,
				ArchiveKey:  archiveKey,
			})
			slog.Info("Restored shard from archive", "height", h, "shard_id", sid, "path", shardPath)
		}
	}

	return restored, nil
}

type shardMissingLine struct {
	Kind        string `json:"kind"`
	GeneratedAt string `json:"generated_at"`
	Batch       string `json:"batch"`
	Height      int64  `json:"height"`
	ShardID     string `json:"shard_id"`
	DestPath    string `json:"dest_path"`
	ArchiveKey  string `json:"archive_key,omitempty"`
	DryRun      bool   `json:"dry_run,omitempty"`
}

type shardFilledLine struct {
	Kind        string `json:"kind"`
	GeneratedAt string `json:"generated_at"`
	Batch       string `json:"batch"`
	Height      int64  `json:"height"`
	ShardID     string `json:"shard_id"`
	DestPath    string `json:"dest_path"`
	ArchiveKey  string `json:"archive_key,omitempty"`
}

type shardFillFailedLine struct {
	Kind        string `json:"kind"`
	GeneratedAt string `json:"generated_at"`
	Batch       string `json:"batch"`
	Height      int64  `json:"height"`
	ShardID     string `json:"shard_id"`
	DestPath    string `json:"dest_path"`
	ArchiveKey  string `json:"archive_key,omitempty"`
	Error       string `json:"error"`
}
