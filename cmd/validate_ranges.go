package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"s3-migrate/config"
	"s3-migrate/internal/validator"
)

func validateRangesCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "validate-ranges",
		Short: "Verify destination has all expected batch archives for [start_from, stop_at]",
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
	if !cfg.UseDestB2() && !cfg.UseDestR2() && !cfg.UseDestS3() {
		return fmt.Errorf("validate-ranges requires destination.b2, destination.r2, or destination.s3 (or legacy b2.* / r2.*)")
	}
	if cfg.StopAt <= 0 {
		return fmt.Errorf("validate-ranges requires stop_at > 0")
	}
	if cfg.StartFrom > cfg.StopAt {
		return fmt.Errorf("validate-ranges: start_from (%d) must be <= stop_at (%d)", cfg.StartFrom, cfg.StopAt)
	}

	destClient, err := newDestinationClient(ctx, cfg)
	if err != nil {
		return err
	}

	return validator.ValidateRanges(ctx, cfg, destClient)
}
