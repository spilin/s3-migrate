package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"s3-migrate/config"
	"s3-migrate/internal/s3client"
	"s3-migrate/internal/validator"
)

func validateRangesCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "validate-ranges",
		Short: "Verify B2 has all expected batch archives for [start_from, stop_at]",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return loadConfig(cmd)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}
			return runValidateRanges(ctx, loadedConfig)
		},
	}
}

func runValidateRanges(ctx context.Context, cfg *config.Config) error {
	if !cfg.UseDestB2() {
		return fmt.Errorf("validate-ranges requires b2 destination (destination.b2 or legacy b2.*)")
	}
	if cfg.StopAt <= 0 {
		return fmt.Errorf("validate-ranges requires stop_at > 0")
	}
	if cfg.StartFrom > cfg.StopAt {
		return fmt.Errorf("validate-ranges: start_from (%d) must be <= stop_at (%d)", cfg.StartFrom, cfg.StopAt)
	}

	d := cfg.Destination.B2
	b2Client, err := s3client.NewB2Client(ctx, d.Region, d.AccessKeyID, d.SecretKey, d.Bucket)
	if err != nil {
		return fmt.Errorf("create b2 client: %w", err)
	}

	return validator.ValidateRanges(ctx, cfg, b2Client)
}
