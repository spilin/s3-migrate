package config

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	// S3 source
	S3Bucket   string
	S3Prefix   string
	S3Region   string
	S3Endpoint string // empty = default AWS
	AWSProfile string
	AWSRegion  string

	// R2 destination
	R2Bucket      string
	R2AccountID   string
	R2AccessKeyID string
	R2SecretKey   string
	R2Region      string

	// B2 destination (Backblaze) - if set, used instead of R2
	B2Bucket      string
	B2Region      string
	B2AccessKeyID string
	B2SecretKey   string

	// Migration settings
	StartFrom            int64  // directory number to start from (e.g. 9820210 -> 000009820210)
	StopAt               int64  // optional max number to process (0 = no limit)
	PadWidth             int    // zero-pad width for directory names (e.g. 12 -> 000009820210)
	DownloadConcurrency  int    // parallel file downloads per directory (default 10)
	DirectoryConcurrency int    // parallel directory processing (default 20)
	BatchDirs            int64  // pack and upload when this many directories accumulated (e.g. 1000000)
	Compression          string // gzip or zstd (default gzip; zstd gives ~15-25% better ratio for JSON)
	CompressionLevel     int    // zstd level 1-22 (default 6; good for JSON; gzip ignores this)
	ConsecutiveEmpty     int    // stop after this many consecutive empty dirs (default 1000)
	WorkDir              string // local temp dir for downloads
	StateFile            string // track last processed number for resume
	StatsFile            string // append JSONL: migrate batch stats; validate-ranges appends missing-range line on failure; empty = disabled
	ArchivePrefix        string // destination prefix for archives (e.g. archives -> archives/1000001-2000000.tar.zst)
}

func Load() (*Config, error) {
	v := viper.New()

	// Env var support
	v.SetEnvPrefix("S3MIGRATE")
	v.AutomaticEnv()

	// Config file (optional)
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(".")
	v.AddConfigPath("/etc/s3-migrate")

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("config error: %w", err)
		}
	}

	cfg := &Config{
		S3Bucket:             v.GetString("s3.bucket"),
		S3Prefix:             v.GetString("s3.prefix"),
		S3Region:             v.GetString("s3.region"),
		S3Endpoint:           v.GetString("s3.endpoint"),
		AWSProfile:           v.GetString("aws.profile"),
		AWSRegion:            v.GetString("aws.region"),
		R2Bucket:             v.GetString("r2.bucket"),
		R2AccountID:          v.GetString("r2.account_id"),
		R2AccessKeyID:        v.GetString("r2.access_key_id"),
		R2SecretKey:          v.GetString("r2.secret_key"),
		R2Region:             v.GetString("r2.region"),
		B2Bucket:             v.GetString("b2.bucket"),
		B2Region:             v.GetString("b2.region"),
		B2AccessKeyID:        v.GetString("b2.access_key_id"),
		B2SecretKey:          v.GetString("b2.secret_key"),
		WorkDir:              v.GetString("work_dir"),
		StateFile:            v.GetString("state_file"),
		StatsFile:            v.GetString("stats_file"),
		StartFrom:            v.GetInt64("start_from"),
		StopAt:               v.GetInt64("stop_at"),
		PadWidth:             v.GetInt("pad_width"),
		DownloadConcurrency:  v.GetInt("download_concurrency"),
		DirectoryConcurrency: v.GetInt("directory_concurrency"),
		BatchDirs:            v.GetInt64("batch_dirs"),
		Compression:          v.GetString("compression"),
		CompressionLevel:     v.GetInt("compression_level"),
		ConsecutiveEmpty:     v.GetInt("consecutive_empty"),
		ArchivePrefix:        v.GetString("archive_prefix"),
	}

	// Defaults (relative to current dir)
	if cfg.R2Region == "" {
		cfg.R2Region = "auto"
	}
	if cfg.WorkDir == "" {
		cfg.WorkDir = "."
	}
	if cfg.StateFile == "" {
		cfg.StateFile = "state.json"
	}
	// stats_file default "stats.jsonl"; empty = disabled
	if cfg.StatsFile == "" {
		cfg.StatsFile = "stats.jsonl"
	}
	if cfg.PadWidth == 0 {
		cfg.PadWidth = 12
	}
	if cfg.DownloadConcurrency <= 0 {
		cfg.DownloadConcurrency = 10
	}
	if cfg.DirectoryConcurrency <= 0 {
		cfg.DirectoryConcurrency = 20
	}
	if cfg.BatchDirs <= 0 {
		cfg.BatchDirs = 1000000
	}
	cfg.Compression = strings.ToLower(cfg.Compression)
	if cfg.Compression == "" {
		cfg.Compression = "gzip"
	}
	if cfg.Compression != "gzip" && cfg.Compression != "zstd" {
		return nil, fmt.Errorf("compression must be gzip or zstd, got %q", cfg.Compression)
	}
	if cfg.Compression == "zstd" && cfg.CompressionLevel <= 0 {
		cfg.CompressionLevel = 6
	}
	if cfg.Compression == "zstd" && (cfg.CompressionLevel < 1 || cfg.CompressionLevel > 22) {
		return nil, fmt.Errorf("compression_level must be 1-22 when using zstd, got %d", cfg.CompressionLevel)
	}
	if cfg.ConsecutiveEmpty <= 0 {
		cfg.ConsecutiveEmpty = 1000
	}
	if cfg.ArchivePrefix == "" {
		cfg.ArchivePrefix = "archives"
	}
	cfg.ArchivePrefix = strings.TrimSuffix(cfg.ArchivePrefix, "/")

	// Validation
	if cfg.S3Bucket == "" {
		return nil, fmt.Errorf("s3.bucket is required")
	}
	if cfg.StartFrom < 0 {
		return nil, fmt.Errorf("start_from must be >= 0")
	}
	useB2 := cfg.B2Bucket != "" && cfg.B2AccessKeyID != "" && cfg.B2SecretKey != ""
	useR2 := cfg.R2Bucket != "" && cfg.R2AccountID != "" && cfg.R2AccessKeyID != "" && cfg.R2SecretKey != ""
	if !useB2 && !useR2 {
		return nil, fmt.Errorf("either r2 (account_id, access_key_id, secret_key, bucket) or b2 (bucket, access_key_id, secret_key) destination is required")
	}
	if useB2 && useR2 {
		return nil, fmt.Errorf("cannot specify both r2 and b2 destination; use one")
	}

	// Resolve relative paths to absolute (relative to process cwd at load time)
	if !filepath.IsAbs(cfg.WorkDir) {
		abs, err := filepath.Abs(cfg.WorkDir)
		if err != nil {
			return nil, fmt.Errorf("resolve work_dir: %w", err)
		}
		cfg.WorkDir = abs
	}
	if !filepath.IsAbs(cfg.StateFile) {
		abs, err := filepath.Abs(cfg.StateFile)
		if err != nil {
			return nil, fmt.Errorf("resolve state_file: %w", err)
		}
		cfg.StateFile = abs
	}
	if cfg.StatsFile != "" && !filepath.IsAbs(cfg.StatsFile) {
		abs, err := filepath.Abs(cfg.StatsFile)
		if err != nil {
			return nil, fmt.Errorf("resolve stats_file: %w", err)
		}
		cfg.StatsFile = abs
	}

	return cfg, nil
}
