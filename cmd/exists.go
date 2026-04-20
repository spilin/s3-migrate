package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"s3-migrate/config"
)

func existsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "exists <object-key>",
		Short: "Check if an object exists in the destination store (B2, R2, or S3-compatible)",
		Args:  cobra.ExactArgs(1),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return loadConfig(cmd)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}
			return runExists(ctx, loadedConfig, args[0])
		},
	}
}

func runExists(ctx context.Context, cfg *config.Config, key string) error {
	if key == "" {
		return fmt.Errorf("object key is empty")
	}

	destClient, err := newDestinationClient(ctx, cfg)
	if err != nil {
		return err
	}

	ok, err := destClient.Exists(ctx, key)
	if err != nil {
		return err
	}
	if ok {
		fmt.Fprintln(os.Stdout, "FOUND")
		return nil
	}
	fmt.Fprintln(os.Stdout, "MISSING")
	return fmt.Errorf("object not found: %s", key)
}
