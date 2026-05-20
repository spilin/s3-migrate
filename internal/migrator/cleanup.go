package migrator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"
)

// RunCleanupLoop periodically deletes source block directories whose matching
// batch archive already exists and whose height is outside the cleanup hot window.
func (m *Migrator) RunCleanupLoop(ctx context.Context, latestBlockHeight func(context.Context) (int64, error)) error {
	if !m.cfg.Cleanup.Enabled {
		return nil
	}
	if m.destClient == nil {
		return fmt.Errorf("cleanup requires a destination client")
	}
	if latestBlockHeight == nil {
		return fmt.Errorf("cleanup requires latest block height callback")
	}

	interval := time.Duration(m.cfg.Cleanup.IntervalMinutes) * time.Minute
	if interval <= 0 {
		interval = 5 * time.Minute
	}

	slog.Info("Starting source cleanup routine",
		"start_from", m.cleanupStartFrom(),
		"state_file", m.cfg.Cleanup.StateFile,
		"hot_window_blocks", m.cfg.Cleanup.HotWindowBlocks,
		"interval_minutes", int(interval/time.Minute),
		"dry_run", m.cfg.Cleanup.DryRun)

	for {
		if err := m.RunCleanupOnce(ctx, latestBlockHeight); err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			slog.Warn("Source cleanup pass failed", "err", err)
		}

		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// RunCleanupOnce performs one source cleanup pass. It is exported for focused tests
// and one-shot callers, while run uses RunCleanupLoop.
func (m *Migrator) RunCleanupOnce(ctx context.Context, latestBlockHeight func(context.Context) (int64, error)) error {
	state, err := LoadState(m.cfg.Cleanup.StateFile)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("load cleanup state: %w", err)
	}
	if state == nil {
		state = &State{}
	}

	latest, err := latestBlockHeight(ctx)
	if err != nil {
		return fmt.Errorf("latest block height for cleanup: %w", err)
	}
	cleanupStopAt := cleanupRoundedStopAt(latest, m.cfg.BatchDirs, m.cfg.Cleanup.HotWindowBlocks)
	next := m.cleanupStartFrom()
	if state.LastProcessedNumber > 0 {
		next = state.LastProcessedNumber + 1
	}
	if cleanupStopAt <= 0 || next > cleanupStopAt {
		slog.Info("Source cleanup has nothing eligible",
			"latest_block_height", latest,
			"hot_window_blocks", m.cfg.Cleanup.HotWindowBlocks,
			"cleanup_stop_at", cleanupStopAt,
			"next", next)
		return nil
	}

	slog.Info("Starting source cleanup pass",
		"latest_block_height", latest,
		"hot_window_blocks", m.cfg.Cleanup.HotWindowBlocks,
		"cleanup_stop_at", cleanupStopAt,
		"next", next,
		"dry_run", m.cfg.Cleanup.DryRun)

	for batchStart := next; batchStart <= cleanupStopAt; {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		batchEnd := batchStart + m.cfg.BatchDirs - 1
		if batchEnd > cleanupStopAt {
			batchEnd = cleanupStopAt
		}

		archiveKey := m.archiveKey(batchStart, batchEnd)
		exists, err := m.destClient.Exists(ctx, archiveKey)
		if err != nil {
			return fmt.Errorf("check archive before cleanup %s: %w", archiveKey, err)
		}
		if !exists {
			slog.Info("Source cleanup waiting for archive before deleting source blocks",
				"range", fmt.Sprintf("%d-%d", batchStart, batchEnd),
				"archive_key", archiveKey)
			return nil
		}

		objects, err := m.cleanupBatch(ctx, batchStart, batchEnd)
		if err != nil {
			return err
		}
		state.LastProcessedNumber = batchEnd
		if !m.cfg.Cleanup.DryRun {
			if err := state.Save(m.cfg.Cleanup.StateFile); err != nil {
				return fmt.Errorf("save cleanup state: %w", err)
			}
		}
		slog.Info("Source cleanup batch complete",
			"range", fmt.Sprintf("%d-%d", batchStart, batchEnd),
			"archive_key", archiveKey,
			"objects", objects,
			"dry_run", m.cfg.Cleanup.DryRun)

		batchStart = batchEnd + 1
	}

	return nil
}

func (m *Migrator) cleanupBatch(ctx context.Context, batchStart, batchEnd int64) (int64, error) {
	var total int64
	for height := batchStart; height <= batchEnd; height++ {
		for _, src := range m.sources {
			prefix := dirPrefix(src.Prefix, m.cfg.PadWidth, height)
			deleted, err := src.Client.DeleteObjectsWithPrefix(ctx, prefix, m.cfg.Cleanup.DryRun)
			if err != nil {
				return total, fmt.Errorf("cleanup source %s prefix %s: %w", src.Name, prefix, err)
			}
			total += deleted
			if deleted > 0 {
				slog.Debug("Source cleanup deleted block directory",
					"source", src.Name,
					"height", height,
					"prefix", prefix,
					"objects", deleted,
					"dry_run", m.cfg.Cleanup.DryRun)
			}
		}
	}
	return total, nil
}

func (m *Migrator) cleanupStartFrom() int64 {
	if m.cfg.Cleanup.StartFrom > 0 {
		return m.cfg.Cleanup.StartFrom
	}
	return m.cfg.StartFrom
}

func cleanupRoundedStopAt(latestHeight, batchDirs, hotWindowBlocks int64) int64 {
	if hotWindowBlocks > 0 {
		latestHeight -= hotWindowBlocks
	}
	if latestHeight < 0 {
		latestHeight = 0
	}
	if batchDirs <= 0 {
		return latestHeight
	}
	return (latestHeight / batchDirs) * batchDirs
}
