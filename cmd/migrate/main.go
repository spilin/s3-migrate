package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"s3-migrate/config"
	"s3-migrate/internal/migrator"
	"s3-migrate/internal/s3client"
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
