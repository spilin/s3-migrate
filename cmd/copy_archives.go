package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"s3-migrate/internal/archivecopy"
)

func copyArchivesCmd() *cobra.Command {
	var dryRun, overwrite bool

	cmd := &cobra.Command{
		Use:   "copy-archives",
		Short: "Copy existing archive objects from source bucket to destination (no packing)",
		Long: "Lists objects under archive_copy.source_prefix in the configured source bucket (B2 or S3), " +
			"and streams each object to the destination (R2, B2, or S3-compatible). " +
			"Each object is verified: byte count vs HeadObject, optional MD5 when source ETag is a single-part hex digest, " +
			"and destination size after upload. Use archive_copy.max_bytes_per_second or max_megabits_per_second to cap total throughput. " +
			"By default only keys matching batch archive names like 1000001-2000000.tar.zst are copied; " +
			"set archive_copy.copy_all_under_prefix: true to copy every object under the prefix.",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if err := loadConfig(cmd); err != nil {
				return err
			}
			if loadedConfig.ArchiveCopy == nil || loadedConfig.ArchiveCopy.SourcePrefix == "" {
				return fmt.Errorf("copy-archives requires archive_copy.source_prefix in config")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}
			cfg := loadedConfig

			sourceClient, err := newSourceClient(ctx, cfg)
			if err != nil {
				return err
			}
			destClient, err := newDestinationClient(ctx, cfg)
			if err != nil {
				return err
			}

			return archivecopy.Run(ctx, cfg, sourceClient, destClient, archivecopy.Options{
				DryRun:    dryRun,
				Overwrite: overwrite,
			})
		},
	}

	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "list actions only, do not read or write objects")
	cmd.Flags().BoolVar(&overwrite, "overwrite", false, "re-copy even when destination object already exists")
	return cmd
}
