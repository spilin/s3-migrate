package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"s3-migrate/config"
	"s3-migrate/internal/migrator"
	"s3-migrate/internal/s3client"
)

func downloadCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "download",
		Short: "Download numbered directories to local disk only (no pack or upload)",
		Long: "Uses the same source settings and batching as run, but writes files under work_dir " +
			"(batch_<first>_<last>/..., same numeric range as each batch) and does not require destination credentials. State file advances like run.",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			configPath, err := cmd.Root().PersistentFlags().GetString("config")
			if err != nil {
				return err
			}
			if configPath != "" {
				loadedConfig, err = config.LoadFromDownload(configPath)
			} else {
				loadedConfig, err = config.LoadDownloadOnly()
			}
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}
			return nil
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

			m, err := migrator.New(cfg, sourceClient, nil)
			if err != nil {
				return err
			}

			if err := m.RunDownloadOnly(ctx); err != nil && err != context.Canceled {
				return err
			}
			slog.Info("Download completed")
			return nil
		},
	}
}
