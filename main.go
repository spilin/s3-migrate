package main

import (
	"log/slog"
	"os"

	"s3-migrate/cmd"
)

func main() {
	level := slog.LevelInfo
	if lv := os.Getenv("S3MIGRATE_LOG_LEVEL"); lv == "debug" || lv == "DEBUG" {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level})))

	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
