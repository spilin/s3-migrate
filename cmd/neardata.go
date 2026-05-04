package cmd

import (
	"strings"

	"github.com/spf13/cobra"

	"s3-migrate/config"
)

const defaultNeardataBaseURL = "https://mainnet.neardata.xyz/v0/block"

func effectiveNeardataBaseURL(cmd *cobra.Command, flagValue string, cfg *config.Config) string {
	if cmd.Flags().Changed("neardata-base") {
		return strings.TrimSpace(flagValue)
	}
	if cfg != nil && strings.TrimSpace(cfg.NeardataBaseURL) != "" {
		return strings.TrimSpace(cfg.NeardataBaseURL)
	}
	return defaultNeardataBaseURL
}

func effectiveNeardataAPIKey(cmd *cobra.Command, flagValue string, cfg *config.Config) string {
	if cmd.Flags().Changed("neardata-api-key") {
		return strings.TrimSpace(flagValue)
	}
	if cfg != nil {
		return strings.TrimSpace(cfg.NeardataAPIKey)
	}
	return ""
}
