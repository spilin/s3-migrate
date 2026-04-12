package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/viper"
)

// SourceConfig selects where numbered directories are read from (exactly one of AWS or B2).
type SourceConfig struct {
	AWS *SourceAWS
	B2  *SourceB2
}

// SourceAWS is S3-compatible source (AWS or custom endpoint).
type SourceAWS struct {
	Bucket   string
	Prefix   string // optional prefix before padded dir names (e.g. "data/")
	Region   string
	Endpoint string // empty = default AWS
}

// SourceB2 is Backblaze B2 as read source (S3-compatible API).
type SourceB2 struct {
	Bucket      string
	Prefix      string // optional prefix before padded dir names
	Region      string
	AccessKeyID string
	SecretKey   string
}

// DestinationConfig selects where packed archives are written (exactly one of R2 or B2).
type DestinationConfig struct {
	R2 *DestR2
	B2 *DestB2
}

// DestR2 is Cloudflare R2.
type DestR2 struct {
	Bucket      string
	AccountID   string
	AccessKeyID string
	SecretKey   string
	Region      string
}

// DestB2 is Backblaze B2 destination.
type DestB2 struct {
	Bucket      string
	Region      string
	AccessKeyID string
	SecretKey   string
}

// Config holds migration settings and nested source/destination.
type Config struct {
	Source      SourceConfig
	Destination DestinationConfig

	// AWSProfile / AWSRegion are used for default credential chain when source is AWS (IAM on EC2, etc.).
	AWSProfile string
	AWSRegion  string

	StartFrom            int64
	StopAt               int64
	PadWidth             int
	DownloadConcurrency  int
	DirectoryConcurrency int
	BatchDirs            int64
	Compression          string
	CompressionLevel     int
	ConsecutiveEmpty     int
	WorkDir              string
	StateFile            string
	StatsFile            string
	ArchivePrefix        string
	// FixGapsLog is optional JSONL path for fix-gaps (gap_found / gap_filled / gap_fill_failed lines).
	FixGapsLog string
}

func Load() (*Config, error) {
	if p := strings.TrimSpace(os.Getenv("S3MIGRATE_CONFIG")); p != "" {
		return LoadFrom(p)
	}
	return loadFrom("", false, true, true)
}

func LoadFrom(path string) (*Config, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("config path is empty")
	}
	return loadFrom(path, true, true, true)
}

// LoadDownloadOnly loads config like Load but does not require destination (source-only commands).
func LoadDownloadOnly() (*Config, error) {
	if p := strings.TrimSpace(os.Getenv("S3MIGRATE_CONFIG")); p != "" {
		return LoadFromDownload(p)
	}
	return loadFrom("", false, true, false)
}

// LoadFromDownload loads config from path without requiring destination credentials.
func LoadFromDownload(path string) (*Config, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("config path is empty")
	}
	return loadFrom(path, true, true, false)
}

// LoadFixGaps loads config for fix-gaps (work_dir, pad_width, download_concurrency only; no source or destination required).
func LoadFixGaps() (*Config, error) {
	if p := strings.TrimSpace(os.Getenv("S3MIGRATE_CONFIG")); p != "" {
		return LoadFromFixGaps(p)
	}
	return loadFrom("", false, false, false)
}

// LoadFromFixGaps loads config from path for fix-gaps without requiring source or destination.
func LoadFromFixGaps(path string) (*Config, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("config path is empty")
	}
	return loadFrom(path, true, false, false)
}

func loadFrom(path string, explicit bool, requireSource, requireDestination bool) (*Config, error) {
	v := viper.New()
	v.SetEnvPrefix("S3MIGRATE")
	v.AutomaticEnv()

	if explicit {
		v.SetConfigFile(path)
		if err := v.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("config error (read %s): %w", path, err)
		}
	} else {
		v.SetConfigName("config")
		v.SetConfigType("yaml")
		v.AddConfigPath(".")
		v.AddConfigPath("/etc/s3-migrate")
		if err := v.ReadInConfig(); err != nil {
			if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
				return nil, fmt.Errorf("config error: %w", err)
			}
		}
	}

	cfg := &Config{
		AWSProfile:           v.GetString("aws.profile"),
		AWSRegion:            v.GetString("aws.region"),
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
		FixGapsLog:           v.GetString("fix_gaps.log_file"),
	}

	readNestedSourceDest(v, cfg)
	mergeLegacyFlat(v, cfg)

	if cfg.R2Region() == "" && cfg.Destination.R2 != nil {
		cfg.Destination.R2.Region = "auto"
	}

	if cfg.WorkDir == "" {
		cfg.WorkDir = "."
	}
	if cfg.StateFile == "" {
		cfg.StateFile = "state.json"
	}
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
	if requireSource {
		if err := validateSource(cfg); err != nil {
			return nil, err
		}
	}
	if requireDestination {
		if err := validateDestination(cfg); err != nil {
			return nil, err
		}
	}
	if requireSource && cfg.StartFrom < 0 {
		return nil, fmt.Errorf("start_from must be >= 0")
	}

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
	if cfg.FixGapsLog != "" && !filepath.IsAbs(cfg.FixGapsLog) {
		abs, err := filepath.Abs(cfg.FixGapsLog)
		if err != nil {
			return nil, fmt.Errorf("resolve fix_gaps.log_file: %w", err)
		}
		cfg.FixGapsLog = abs
	}

	return cfg, nil
}

func readNestedSourceDest(v *viper.Viper, cfg *Config) {
	if v.GetString("source.aws.bucket") != "" {
		cfg.Source.AWS = &SourceAWS{
			Bucket:   v.GetString("source.aws.bucket"),
			Prefix:   v.GetString("source.aws.prefix"),
			Region:   v.GetString("source.aws.region"),
			Endpoint: v.GetString("source.aws.endpoint"),
		}
	}
	if v.GetString("source.b2.bucket") != "" {
		cfg.Source.B2 = &SourceB2{
			Bucket:      v.GetString("source.b2.bucket"),
			Prefix:      v.GetString("source.b2.prefix"),
			Region:      v.GetString("source.b2.region"),
			AccessKeyID: v.GetString("source.b2.access_key_id"),
			SecretKey:   v.GetString("source.b2.secret_key"),
		}
	}
	if v.GetString("destination.r2.bucket") != "" {
		cfg.Destination.R2 = &DestR2{
			Bucket:      v.GetString("destination.r2.bucket"),
			AccountID:   v.GetString("destination.r2.account_id"),
			AccessKeyID: v.GetString("destination.r2.access_key_id"),
			SecretKey:   v.GetString("destination.r2.secret_key"),
			Region:      v.GetString("destination.r2.region"),
		}
	}
	if v.GetString("destination.b2.bucket") != "" {
		cfg.Destination.B2 = &DestB2{
			Bucket:      v.GetString("destination.b2.bucket"),
			Region:      v.GetString("destination.b2.region"),
			AccessKeyID: v.GetString("destination.b2.access_key_id"),
			SecretKey:   v.GetString("destination.b2.secret_key"),
		}
	}
}

// mergeLegacyFlat maps old top-level s3/b2/r2/b2_source keys into nested source/destination when nested is absent.
func mergeLegacyFlat(v *viper.Viper, cfg *Config) {
	if cfg.Source.AWS == nil && cfg.Source.B2 == nil {
		if v.GetString("b2_source.bucket") != "" {
			cfg.Source.B2 = &SourceB2{
				Bucket:      v.GetString("b2_source.bucket"),
				Prefix:      v.GetString("s3.prefix"),
				Region:      v.GetString("b2_source.region"),
				AccessKeyID: v.GetString("b2_source.access_key_id"),
				SecretKey:   v.GetString("b2_source.secret_key"),
			}
		} else if v.GetString("s3.bucket") != "" {
			cfg.Source.AWS = &SourceAWS{
				Bucket:   v.GetString("s3.bucket"),
				Prefix:   v.GetString("s3.prefix"),
				Region:   v.GetString("s3.region"),
				Endpoint: v.GetString("s3.endpoint"),
			}
		}
	}
	if cfg.Destination.B2 == nil && cfg.Destination.R2 == nil {
		if v.GetString("b2.bucket") != "" {
			cfg.Destination.B2 = &DestB2{
				Bucket:      v.GetString("b2.bucket"),
				Region:      v.GetString("b2.region"),
				AccessKeyID: v.GetString("b2.access_key_id"),
				SecretKey:   v.GetString("b2.secret_key"),
			}
		} else if v.GetString("r2.bucket") != "" {
			cfg.Destination.R2 = &DestR2{
				Bucket:      v.GetString("r2.bucket"),
				AccountID:   v.GetString("r2.account_id"),
				AccessKeyID: v.GetString("r2.access_key_id"),
				SecretKey:   v.GetString("r2.secret_key"),
				Region:      v.GetString("r2.region"),
			}
		}
	}
}

func validateSource(cfg *Config) error {
	aws := cfg.Source.AWS != nil && cfg.Source.AWS.Bucket != ""
	b2 := cfg.Source.B2 != nil && cfg.Source.B2.Bucket != "" &&
		cfg.Source.B2.AccessKeyID != "" && cfg.Source.B2.SecretKey != ""
	if aws && b2 {
		return fmt.Errorf("source: specify only one of source.aws or source.b2 (legacy: s3.* or b2_source.*)")
	}
	if !aws && !b2 {
		return fmt.Errorf("source: set source.aws or source.b2 (legacy: s3.bucket or b2_source.*)")
	}
	if cfg.Source.B2 != nil {
		partial := cfg.Source.B2.Bucket != "" || cfg.Source.B2.AccessKeyID != "" || cfg.Source.B2.SecretKey != ""
		if partial && !b2 {
			return fmt.Errorf("source.b2 requires bucket, access_key_id, and secret_key")
		}
	}
	return nil
}

func validateDestination(cfg *Config) error {
	useB2 := cfg.Destination.B2 != nil && cfg.Destination.B2.Bucket != "" &&
		cfg.Destination.B2.AccessKeyID != "" && cfg.Destination.B2.SecretKey != ""
	useR2 := cfg.Destination.R2 != nil && cfg.Destination.R2.Bucket != "" &&
		cfg.Destination.R2.AccountID != "" && cfg.Destination.R2.AccessKeyID != "" && cfg.Destination.R2.SecretKey != ""
	if !useB2 && !useR2 {
		return fmt.Errorf("destination: set destination.b2 or destination.r2 (legacy: b2.* or r2.*)")
	}
	if useB2 && useR2 {
		return fmt.Errorf("destination: specify only one of destination.b2 or destination.r2")
	}
	return nil
}

// R2Region returns destination R2 region if set (for defaults).
func (c *Config) R2Region() string {
	if c.Destination.R2 != nil {
		return c.Destination.R2.Region
	}
	return ""
}

// SourceObjectPrefix is the key prefix before padded directory names (e.g. "data/").
func (c *Config) SourceObjectPrefix() string {
	if c.Source.AWS != nil {
		return c.Source.AWS.Prefix
	}
	if c.Source.B2 != nil {
		return c.Source.B2.Prefix
	}
	return ""
}

// UseB2Source is true when reads use B2.
func (c *Config) UseB2Source() bool {
	b := c.Source.B2
	return b != nil && b.Bucket != "" && b.AccessKeyID != "" && b.SecretKey != ""
}

// UseAWSSource is true when reads use AWS S3.
func (c *Config) UseAWSSource() bool {
	a := c.Source.AWS
	return a != nil && a.Bucket != ""
}

// AWSSourceRegion for S3 client (fallback aws.region then us-east-1).
func (c *Config) AWSSourceRegion() string {
	if c.Source.AWS != nil && c.Source.AWS.Region != "" {
		return c.Source.AWS.Region
	}
	if c.AWSRegion != "" {
		return c.AWSRegion
	}
	return "us-east-1"
}

// UseDestB2 reports whether destination is B2.
func (c *Config) UseDestB2() bool {
	d := c.Destination.B2
	return d != nil && d.Bucket != "" && d.AccessKeyID != "" && d.SecretKey != ""
}

// UseDestR2 reports whether destination is R2.
func (c *Config) UseDestR2() bool {
	d := c.Destination.R2
	return d != nil && d.Bucket != "" && d.AccountID != "" && d.AccessKeyID != "" && d.SecretKey != ""
}

// DestB2Bucket for logging / validator.
func (c *Config) DestB2Bucket() string {
	if c.Destination.B2 != nil {
		return c.Destination.B2.Bucket
	}
	return ""
}
