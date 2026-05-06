package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/spf13/cobra"

	"s3-migrate/config"
	"s3-migrate/internal/s3client"
)

func emptyBucketCmd() *cobra.Command {
	var dryRun bool
	var prefix string
	var yes bool

	cmd := &cobra.Command{
		Use:   "empty-bucket",
		Short: "Delete all objects from the configured destination bucket",
		Long: "Deletes objects from the configured destination bucket (destination.b2, destination.r2, or destination.s3). " +
			"Requires --yes unless --dry-run is set. Use --prefix to delete only objects under a prefix.",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return loadConfig(cmd)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}
			return runEmptyBucket(ctx, loadedConfig, emptyBucketOptions{
				DryRun: dryRun,
				Prefix: prefix,
				Yes:    yes,
			})
		},
	}

	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "list/count objects only; do not delete")
	cmd.Flags().StringVar(&prefix, "prefix", "", "only delete objects under this prefix (default: whole bucket)")
	cmd.Flags().BoolVar(&yes, "yes", false, "confirm deletion")
	return cmd
}

type emptyBucketOptions struct {
	DryRun bool
	Prefix string
	Yes    bool
}

func runEmptyBucket(ctx context.Context, cfg *config.Config, opts emptyBucketOptions) error {
	prefix := strings.TrimSpace(opts.Prefix)
	if !opts.DryRun && !opts.Yes {
		return fmt.Errorf("refusing to delete destination bucket objects without --yes (use --dry-run to preview)")
	}

	destClient, err := newDestinationClient(ctx, cfg)
	if err != nil {
		return err
	}

	bucket := cfg.DestB2Bucket()
	slog.Info("Empty bucket starting", "bucket", bucket, "prefix", prefix, "dry_run", opts.DryRun)

	var totalObjects int64
	var totalDeleted int64
	err = destClient.WalkObjectPages(ctx, prefix, func(pageIndex int, page []s3client.ObjectInfo, _ bool) error {
		keys := make([]string, 0, len(page))
		for _, obj := range page {
			keys = append(keys, obj.Key)
		}
		totalObjects += int64(len(keys))

		if opts.DryRun || len(keys) == 0 {
			slog.Info("Empty bucket page scanned",
				"bucket", bucket,
				"prefix", prefix,
				"page", pageIndex,
				"objects", len(keys),
				"objects_seen", totalObjects)
			return nil
		}

		if err := deleteKeysInBatches(ctx, destClient, keys); err != nil {
			return err
		}
		totalDeleted += int64(len(keys))
		slog.Info("Empty bucket page deleted",
			"bucket", bucket,
			"prefix", prefix,
			"page", pageIndex,
			"objects_deleted", len(keys),
			"objects_deleted_total", totalDeleted)
		return nil
	})
	if err != nil {
		return err
	}

	if opts.DryRun {
		slog.Info("Empty bucket dry-run complete", "bucket", bucket, "prefix", prefix, "objects_seen", totalObjects)
		return nil
	}
	slog.Info("Empty bucket complete", "bucket", bucket, "prefix", prefix, "objects_deleted", totalDeleted)
	return nil
}

func deleteKeysInBatches(ctx context.Context, client *s3client.Client, keys []string) error {
	const maxDeleteObjects = 1000
	for start := 0; start < len(keys); start += maxDeleteObjects {
		end := start + maxDeleteObjects
		if end > len(keys) {
			end = len(keys)
		}
		if err := client.DeleteObjects(ctx, keys[start:end]); err != nil {
			return err
		}
	}
	return nil
}
