package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"

	"s3-migrate/config"
	"s3-migrate/internal/migrator"
)

func cleanupPatternCmd() *cobra.Command {
	var from int64
	var to int64
	var maxShardID int
	var progressEvery int64
	var dryRun bool
	var stateFile string

	cmd := &cobra.Command{
		Use:   "cleanup-pattern",
		Short: "Delete source block files by known block/shard key pattern without listing",
		Long: "Deletes generated source object keys directly: <padded-height>/block.json and " +
			"<padded-height>/shard_<id>.json for shard IDs 0..max-shard-id. Missing keys are ignored.",
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

			return runCleanupPattern(ctx, loadedConfig, cleanupPatternOptions{
				From:          from,
				To:            to,
				MaxShardID:    maxShardID,
				ProgressEvery: progressEvery,
				DryRun:        dryRun,
				StateFile:     stateFile,
			})
		},
	}

	cmd.Flags().Int64Var(&from, "from", 0, "first block height to delete (default: cleanup state + 1, cleanup.start_from, or start_from)")
	cmd.Flags().Int64Var(&to, "to", 0, "last block height to delete (default: dynamic cleanup stop from NEAR RPC and cleanup.hot_window_blocks)")
	cmd.Flags().IntVar(&maxShardID, "max-shard-id", 10, "delete shard_0.json through shard_<max>.json for each block")
	cmd.Flags().Int64Var(&progressEvery, "progress-every", 10000, "log and save progress after this many block heights")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "print planned progress without deleting or saving state")
	cmd.Flags().StringVar(&stateFile, "state-file", "", "state file to update (default: cleanup.state_file)")
	return cmd
}

type cleanupPatternOptions struct {
	From          int64
	To            int64
	MaxShardID    int
	ProgressEvery int64
	DryRun        bool
	StateFile     string
}

func runCleanupPattern(ctx context.Context, cfg *config.Config, opts cleanupPatternOptions) error {
	if opts.MaxShardID < 0 {
		return fmt.Errorf("--max-shard-id must be >= 0")
	}
	if opts.ProgressEvery <= 0 {
		opts.ProgressEvery = 10000
	}

	stateFile := strings.TrimSpace(opts.StateFile)
	if stateFile == "" {
		stateFile = cfg.Cleanup.StateFile
	}

	start, err := cleanupPatternStart(cfg, stateFile, opts.From)
	if err != nil {
		return err
	}
	stop, err := cleanupPatternStop(ctx, cfg, opts.To)
	if err != nil {
		return err
	}
	if start <= 0 {
		return fmt.Errorf("cleanup-pattern start must be > 0")
	}
	if stop < start {
		slog.Info("Pattern cleanup has nothing to do", "from", start, "to", stop)
		return nil
	}

	sourceClient, err := newSourceClient(ctx, cfg)
	if err != nil {
		return err
	}

	sourcePrefix := cfg.SourceObjectPrefix()
	keysPerHeight := opts.MaxShardID + 2
	slog.Info("Pattern cleanup starting",
		"from", start,
		"to", stop,
		"source_prefix", sourcePrefix,
		"pad_width", cfg.PadWidth,
		"max_shard_id", opts.MaxShardID,
		"keys_per_height", keysPerHeight,
		"state_file", stateFile,
		"progress_every", opts.ProgressEvery,
		"dry_run", opts.DryRun)

	var keys []string
	var keysSent int64
	flush := func() error {
		if len(keys) == 0 {
			return nil
		}
		if !opts.DryRun {
			if err := sourceClient.DeleteObjects(ctx, keys); err != nil {
				return err
			}
		}
		keysSent += int64(len(keys))
		keys = keys[:0]
		return nil
	}

	for height := start; height <= stop; height++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		keys = append(keys, cleanupPatternKeys(sourcePrefix, cfg.PadWidth, height, opts.MaxShardID)...)
		if len(keys) >= 900 {
			if err := flush(); err != nil {
				return fmt.Errorf("delete generated keys through height %d: %w", height, err)
			}
		}

		processed := height - start + 1
		if height == stop || processed%opts.ProgressEvery == 0 {
			if err := flush(); err != nil {
				return fmt.Errorf("delete generated keys through height %d: %w", height, err)
			}
			if !opts.DryRun {
				state := &migrator.State{LastProcessedNumber: height}
				if err := state.Save(stateFile); err != nil {
					return fmt.Errorf("save cleanup-pattern state: %w", err)
				}
			}
			slog.Info("Pattern cleanup progress",
				"height", height,
				"to", stop,
				"heights_done", height-start+1,
				"keys_sent", keysSent,
				"dry_run", opts.DryRun)
		}
	}

	slog.Info("Pattern cleanup complete", "from", start, "to", stop, "keys_sent", keysSent, "dry_run", opts.DryRun)
	return nil
}

func cleanupPatternStart(cfg *config.Config, stateFile string, from int64) (int64, error) {
	if from > 0 {
		return from, nil
	}
	state, err := migrator.LoadState(stateFile)
	if err != nil && !os.IsNotExist(err) {
		return 0, fmt.Errorf("load cleanup-pattern state: %w", err)
	}
	if state != nil && state.LastProcessedNumber > 0 {
		return state.LastProcessedNumber + 1, nil
	}
	if cfg.Cleanup.StartFrom > 0 {
		return cfg.Cleanup.StartFrom, nil
	}
	return cfg.StartFrom, nil
}

func cleanupPatternStop(ctx context.Context, cfg *config.Config, to int64) (int64, error) {
	if to > 0 {
		return to, nil
	}
	if cfg.Cleanup.HotWindowBlocks <= 0 {
		return 0, fmt.Errorf("--to is required when cleanup.hot_window_blocks is not set")
	}
	latestHeight, err := latestNearBlockHeight(ctx, cfg.NearRPCURL)
	if err != nil {
		return 0, err
	}
	return roundedStopAt(latestHeight, cfg.BatchDirs, cfg.Cleanup.HotWindowBlocks), nil
}

func cleanupPatternKeys(sourcePrefix string, padWidth int, height int64, maxShardID int) []string {
	prefix := cleanupPatternBlockPrefix(sourcePrefix, padWidth, height)
	keys := make([]string, 0, maxShardID+2)
	keys = append(keys, prefix+"block.json")
	for shardID := 0; shardID <= maxShardID; shardID++ {
		keys = append(keys, fmt.Sprintf("%sshard_%d.json", prefix, shardID))
	}
	return keys
}

func cleanupPatternBlockPrefix(sourcePrefix string, padWidth int, height int64) string {
	dir := fmt.Sprintf("%0*d", padWidth, height)
	base := strings.TrimSuffix(sourcePrefix, "/")
	if base != "" {
		return base + "/" + dir + "/"
	}
	return dir + "/"
}
