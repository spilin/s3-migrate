package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"s3-migrate/config"
)

// loadedConfig is set by each subcommand's PreRunE before RunE.
var loadedConfig *config.Config

func loadConfig(cmd *cobra.Command) error {
	configPath, err := cmd.Root().PersistentFlags().GetString("config")
	if err != nil {
		return err
	}
	if configPath != "" {
		loadedConfig, err = config.LoadFrom(configPath)
	} else {
		loadedConfig, err = config.Load()
	}
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	return nil
}

// RootCmd builds the CLI (root shows help by default, like cloud-console-import).
func RootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "migrate",
		Short: "S3 to R2/B2 migration tool",
		Long:  "Migrate numbered S3 prefixes into packed archives on Cloudflare R2 or Backblaze B2.",
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}

	root.PersistentFlags().String("config", "", "path to config.yaml (optional; else ./config.yaml, /etc/s3-migrate/config.yaml, or S3MIGRATE_CONFIG)")

	root.AddCommand(
		runCmd(),
		downloadCmd(),
		fixGapsCmd(),
		existsCmd(),
		validateRangesCmd(),
	)

	return root
}

// Execute runs the root command (for main).
func Execute() error {
	if err := RootCmd().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return err
	}
	return nil
}
