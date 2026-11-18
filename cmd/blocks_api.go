package cmd

import (
	"log/slog"

	"s3-migrate/config"
	"s3-migrate/internal/blocksapi"
	"s3-migrate/internal/gaps"
)

func openBlocksAPI(cfg *config.Config) (gaps.BlockFallback, func(), error) {
	if cfg == nil || !cfg.BlocksAPI.Configured() {
		return nil, func() {}, nil
	}
	client, err := blocksapi.Dial(cfg.BlocksAPI)
	if err != nil {
		return nil, nil, err
	}
	slog.Info("blocks-api fallback enabled",
		"endpoint", cfg.BlocksAPI.Endpoint,
		"stream", cfg.BlocksAPI.Stream,
		"origin", cfg.BlocksAPI.Origin)
	return client, func() {
		if err := client.Close(); err != nil {
			slog.Warn("blocks-api close", "err", err)
		}
	}, nil
}
