package cmd

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"s3-migrate/internal/migrator"
	"s3-migrate/internal/s3client"
)

func runCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "run",
		Short: "Run the migration (AWS S3 or B2 source → R2/B2 destination)",
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

			var sourceClient *s3client.Client
			var err error
			if cfg.UseB2Source() {
				sb := cfg.Source.B2
				sourceClient, err = s3client.NewB2Client(ctx,
					sb.Region, sb.AccessKeyID, sb.SecretKey, sb.Bucket)
				if err != nil {
					return err
				}
				slog.Info("Using Backblaze B2 as source", "bucket", sb.Bucket)
			} else {
				sa := cfg.Source.AWS
				sourceClient, err = s3client.NewS3Client(ctx, cfg.AWSSourceRegion(), sa.Endpoint, sa.Bucket,
					sa.AccessKeyID, sa.SecretKey)
				if err != nil {
					return err
				}
				slog.Info("Using AWS S3 as source", "bucket", sa.Bucket)
			}

			var destClient *s3client.Client
			if cfg.UseDestB2() {
				db := cfg.Destination.B2
				destClient, err = s3client.NewB2Client(ctx,
					db.Region, db.AccessKeyID, db.SecretKey, db.Bucket)
				if err != nil {
					return err
				}
				slog.Info("Using Backblaze B2 as destination", "bucket", db.Bucket)
			} else {
				dr := cfg.Destination.R2
				destClient, err = s3client.NewR2Client(ctx,
					dr.AccountID, dr.AccessKeyID, dr.SecretKey, dr.Bucket)
				if err != nil {
					return err
				}
				slog.Info("Using Cloudflare R2 as destination")
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
