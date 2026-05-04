package cmd

import (
	"context"
	"log/slog"

	"s3-migrate/config"
	"s3-migrate/internal/migrator"
	"s3-migrate/internal/s3client"
)

func newSourceClient(ctx context.Context, cfg *config.Config) (*s3client.Client, error) {
	if cfg.UseB2Source() {
		sb := cfg.Source.B2
		c, err := s3client.NewB2Client(ctx, sb.Region, sb.AccessKeyID, sb.SecretKey, sb.Bucket)
		if err != nil {
			return nil, err
		}
		slog.Info("Using Backblaze B2 as source", "bucket", sb.Bucket)
		return c, nil
	}
	sa := cfg.Source.AWS
	c, err := s3client.NewS3Client(ctx, cfg.AWSSourceRegion(), sa.Endpoint, sa.Bucket, sa.AccessKeyID, sa.SecretKey)
	if err != nil {
		return nil, err
	}
	slog.Info("Using AWS S3 as source", "bucket", sa.Bucket)
	return c, nil
}

func newMigrationSources(ctx context.Context, cfg *config.Config) ([]migrator.Source, error) {
	var sources []migrator.Source

	if cfg.UseB2Source() {
		sb := cfg.Source.B2
		c, err := s3client.NewB2Client(ctx, sb.Region, sb.AccessKeyID, sb.SecretKey, sb.Bucket)
		if err != nil {
			return nil, err
		}
		slog.Info("Using Backblaze B2 as migration source", "bucket", sb.Bucket, "prefix", sb.Prefix)
		sources = append(sources, migrator.Source{
			Name:   "b2",
			Prefix: sb.Prefix,
			Client: c,
		})
	}

	if cfg.UseAWSSource() {
		sa := cfg.Source.AWS
		c, err := s3client.NewS3Client(ctx, cfg.AWSSourceRegion(), sa.Endpoint, sa.Bucket, sa.AccessKeyID, sa.SecretKey)
		if err != nil {
			return nil, err
		}
		slog.Info("Using AWS S3 as migration source", "bucket", sa.Bucket, "prefix", sa.Prefix)
		sources = append(sources, migrator.Source{
			Name:   "s3",
			Prefix: sa.Prefix,
			Client: c,
		})
	}

	return sources, nil
}
