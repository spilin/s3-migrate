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

	"s3-migrate/config"
	"s3-migrate/internal/gaps"
)

func fixGapsCmd() *cobra.Command {
	var neardataBase string
	var neardataAPIKey string
	var dryRun bool
	var logFile string

	cmd := &cobra.Command{
		Use:   "fix-gaps",
		Short: "Fill missing block directories under batch_* using neardata.xyz API",
		Long: "Scans work_dir for batch_<first>_<last> folders from download, detects gaps in padded height " +
			"subdirectories, fetches each missing height from neardata (same layout as download_fastnear.py: " +
			"block.json + shard_<id>.json), and writes into the matching batch tree.",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			configPath, err := cmd.Root().PersistentFlags().GetString("config")
			if err != nil {
				return err
			}
			if configPath != "" {
				loadedConfig, err = config.LoadFromFixGaps(configPath)
			} else {
				loadedConfig, err = config.LoadFixGaps()
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

			logPath := strings.TrimSpace(logFile)
			if logPath == "" {
				logPath = loadedConfig.FixGapsLog
			}
			if logPath != "" && !filepath.IsAbs(logPath) {
				abs, err := filepath.Abs(logPath)
				if err != nil {
					return fmt.Errorf("resolve log file: %w", err)
				}
				logPath = abs
			}
			opts := gaps.Options{
				NeardataBaseURL: neardataBase,
				NeardataAPIKey:  neardataAPIKey,
				DryRun:          dryRun,
				LogFile:         logPath,
			}
			if err := gaps.Run(ctx, loadedConfig, opts); err != nil && err != context.Canceled {
				return err
			}
			slog.Info("fix-gaps finished")
			return nil
		},
	}
	cmd.Flags().StringVar(&neardataBase, "neardata-base", "https://mainnet.neardata.xyz/v0/block",
		"neardata block API base URL (no trailing slash); use https://testnet.neardata.xyz/v0/block for testnet")
	cmd.Flags().StringVar(&neardataAPIKey, "neardata-api-key", "", "neardata.xyz API key (passed as ?apiKey=...); optional")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "only log gaps, do not fetch or write")
	cmd.Flags().StringVar(&logFile, "log-file", "", "append JSONL audit log here (overrides fix_gaps.log_file in config); empty disables file log")
	return cmd
}
