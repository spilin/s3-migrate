package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/spf13/cobra"

	"s3-migrate/internal/archivegaps"
)

func archiveGapsCmd() *cobra.Command {
	var neardataBase string
	var neardataAPIKey string
	var dryRun bool
	var logFile string

	cmd := &cobra.Command{
		Use:   "archive-gaps",
		Short: "Verify destination batch archives, log gaps, refetch from neardata, repack, and replace on S3",
		Long: "Lists objects under archive_prefix in the configured destination (same naming as migrate: " +
			"<prefix>/<first>-<last>.tar.gz or .tar.zst). For each archive overlapping [start_from, stop_at] " +
			"(stop_at 0 means no upper bound), downloads the object, extracts it, checks every block height " +
			"and shard_*.json file, appends a JSONL line to the log for any problematic archive, then " +
			"(unless --dry-run) fetches missing data from neardata.xyz, repacks, verifies, and uploads " +
			"replacing the same key.",
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
			if !cfg.UseDestB2() && !cfg.UseDestR2() && !cfg.UseDestS3() {
				return fmt.Errorf("archive-gaps requires destination.b2, destination.r2, or destination.s3 (or legacy b2.* / r2.*)")
			}

			logPath := strings.TrimSpace(logFile)
			if logPath == "" {
				logPath = cfg.ArchiveGapsLog
			}
			if logPath != "" && !filepath.IsAbs(logPath) {
				abs, err := filepath.Abs(logPath)
				if err != nil {
					return fmt.Errorf("resolve log file: %w", err)
				}
				logPath = abs
			}

			destClient, err := newDestinationClient(ctx, cfg)
			if err != nil {
				return err
			}

			opts := archivegaps.Options{
				NeardataBaseURL: neardataBase,
				NeardataAPIKey:  neardataAPIKey,
				DryRun:          dryRun,
				LogFile:         logPath,
			}
			if err := archivegaps.Run(ctx, cfg, destClient, opts); err != nil && err != context.Canceled {
				return err
			}
			slog.Info("archive-gaps finished")
			return nil
		},
	}
	cmd.Flags().StringVar(&neardataBase, "neardata-base", "https://mainnet.neardata.xyz/v0/block",
		"neardata block API base URL (no trailing slash); use https://testnet.neardata.xyz/v0/block for testnet")
	cmd.Flags().StringVar(&neardataAPIKey, "neardata-api-key", "", "neardata.xyz API key (?apiKey=...); optional")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "detect and log problems only; do not refetch, repack, or upload")
	cmd.Flags().StringVar(&logFile, "log-file", "", "append JSONL here (overrides archive_gaps.log_file); required if config has no archive_gaps.log_file")
	return cmd
}
