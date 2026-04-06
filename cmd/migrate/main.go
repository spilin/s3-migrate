package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"s3-migrate/config"
	"s3-migrate/internal/migrator"
	"s3-migrate/internal/s3client"
	"s3-migrate/internal/validator"
)

func main() {
	// Log level: S3MIGRATE_LOG_LEVEL=debug for verbose (per-file downloads)
	level := slog.LevelInfo
	if lv := os.Getenv("S3MIGRATE_LOG_LEVEL"); lv == "debug" || lv == "DEBUG" {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level})))

	cfg, err := config.Load()
	if err != nil {
		slog.Error("Failed to load config", "err", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		slog.Info("Shutdown signal received, stopping...")
		cancel()
	}()

	if len(os.Args) > 1 && os.Args[1] == "validate-ranges" {
		if err := runValidateRanges(ctx, cfg); err != nil && err != context.Canceled {
			slog.Error("Validation failed", "err", err)
			os.Exit(1)
		}
		slog.Info("Validation completed")
		return
	}

	// S3 client (source - AWS)
	s3Region := cfg.S3Region
	if s3Region == "" {
		s3Region = cfg.AWSRegion
	}
	if s3Region == "" {
		s3Region = "us-east-1"
	}

	s3Client, err := s3client.NewS3Client(ctx, s3Region, cfg.S3Endpoint, cfg.S3Bucket)
	if err != nil {
		slog.Error("Failed to create S3 client", "err", err)
		os.Exit(1)
	}

	// Destination client (R2 or B2)
	var destClient *s3client.Client
	if cfg.B2Bucket != "" && cfg.B2AccessKeyID != "" && cfg.B2SecretKey != "" {
		destClient, err = s3client.NewB2Client(ctx,
			cfg.B2Region, cfg.B2AccessKeyID, cfg.B2SecretKey, cfg.B2Bucket)
		if err != nil {
			slog.Error("Failed to create B2 client", "err", err)
			os.Exit(1)
		}
		slog.Info("Using Backblaze B2 as destination")
	} else {
		destClient, err = s3client.NewR2Client(ctx,
			cfg.R2AccountID, cfg.R2AccessKeyID, cfg.R2SecretKey, cfg.R2Bucket)
		if err != nil {
			slog.Error("Failed to create R2 client", "err", err)
			os.Exit(1)
		}
		slog.Info("Using Cloudflare R2 as destination")
	}

	m, err := migrator.New(cfg, s3Client, destClient)
	if err != nil {
		slog.Error("Failed to create migrator", "err", err)
		os.Exit(1)
	}

	if err := m.Run(ctx); err != nil && err != context.Canceled {
		slog.Error("Migration failed", "err", err)
		os.Exit(1)
	}

	slog.Info("Migration completed")
}

func runValidateRanges(ctx context.Context, cfg *config.Config) error {
	// Important: validation checks block batches in B2 (not AWS).
	useB2 := cfg.B2Bucket != "" && cfg.B2AccessKeyID != "" && cfg.B2SecretKey != ""
	if !useB2 {
		return fmt.Errorf("validate-ranges requires b2 destination config (b2.bucket, b2.access_key_id, b2.secret_key)")
	}
	if cfg.StopAt <= 0 {
		return fmt.Errorf("validate-ranges requires stop_at > 0")
	}

	b2Client, err := s3client.NewB2Client(ctx, cfg.B2Region, cfg.B2AccessKeyID, cfg.B2SecretKey, cfg.B2Bucket)
	if err != nil {
		return fmt.Errorf("create b2 client: %w", err)
	}

	return validator.ValidateRanges(ctx, cfg, b2Client)
}
