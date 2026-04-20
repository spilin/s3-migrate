package cmd

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"s3-migrate/internal/migrator"
)

func runCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "run",
		Short: "Run the migration (AWS S3 or B2 source → R2, B2, or S3-compatible destination)",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return loadConfig(cmd)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
			go func() {
				<-sigCh
				slog.Info("Shutdown signal received, stopping...")
				cancel()
			}()

			cfg := loadedConfig

			sourceClient, err := newSourceClient(ctx, cfg)
			if err != nil {
				return err
			}

			destClient, err := newDestinationClient(ctx, cfg)
			if err != nil {
				return err
			}
			switch {
			case cfg.UseDestB2():
				slog.Info("Using Backblaze B2 as destination", "bucket", cfg.Destination.B2.Bucket)
			case cfg.UseDestR2():
				slog.Info("Using Cloudflare R2 as destination", "bucket", cfg.Destination.R2.Bucket)
			case cfg.UseDestS3():
				slog.Info("Using S3-compatible destination (e.g. MinIO)", "endpoint", cfg.Destination.S3.Endpoint, "bucket", cfg.Destination.S3.Bucket)
			}

			m, err := migrator.New(cfg, sourceClient, destClient)
			if err != nil {
				return err
			}

			if err := m.Run(ctx); err != nil && err != context.Canceled {
				return err
			}
			slog.Info("Migration completed")
			return nil
		},
	}
}
