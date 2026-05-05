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
	var neardataBase string
	var neardataAPIKey string

	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run the migration (B2/S3 sources → R2, B2, or S3-compatible destination)",
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
			dynamicStopAt := cfg.StopAt <= 0
			refreshStopAt := func(ctx context.Context) (migrator.StopAtRefresh, error) {
				latestHeight, err := latestNearBlockHeight(ctx, cfg.NearRPCURL)
				if err != nil {
					return migrator.StopAtRefresh{}, err
				}
				return migrator.StopAtRefresh{
					LatestBlockHeight: latestHeight,
					StopAt:            roundedStopAt(latestHeight, cfg.BatchDirs),
				}, nil
			}
			if dynamicStopAt {
				refreshed, err := refreshStopAt(ctx)
				if err != nil {
					return err
				}
				cfg.StopAt = refreshed.StopAt
				slog.Info("Resolved dynamic stop_at from NEAR RPC",
					"near_rpc_url", cfg.NearRPCURL,
					"latest_block_height", refreshed.LatestBlockHeight,
					"batch_dirs", cfg.BatchDirs,
					"stop_at", cfg.StopAt,
					"refresh_minutes", cfg.StopAtRefreshMinutes)
			}

			sources, err := newMigrationSources(ctx, cfg)
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

			opts := migrator.Options{
				NeardataBaseURL: effectiveNeardataBaseURL(cmd, neardataBase, cfg),
				NeardataAPIKey:  effectiveNeardataAPIKey(cmd, neardataAPIKey, cfg),
			}
			if dynamicStopAt {
				opts.RefreshStopAt = refreshStopAt
			}
			m, err := migrator.NewWithSources(cfg, sources, destClient, opts)
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
	cmd.Flags().StringVar(&neardataBase, "neardata-base", "",
		"neardata block API base URL fallback (overrides neardata.base_url; default https://mainnet.neardata.xyz/v0/block)")
	cmd.Flags().StringVar(&neardataAPIKey, "neardata-api-key", "", "neardata.xyz API key (?apiKey=...); optional")
	return cmd
}
