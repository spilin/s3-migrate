package cmd

import (
	"context"
	"fmt"
	"strings"

	"s3-migrate/config"
	"s3-migrate/internal/s3client"
)

func newDestinationClient(ctx context.Context, cfg *config.Config) (*s3client.Client, error) {
	switch {
	case cfg.UseDestB2():
		d := cfg.Destination.B2
		return s3client.NewB2Client(ctx, d.Region, d.AccessKeyID, d.SecretKey, d.Bucket)
	case cfg.UseDestR2():
		dr := cfg.Destination.R2
		return s3client.NewR2Client(ctx, dr.AccountID, dr.AccessKeyID, dr.SecretKey, dr.Bucket)
	case cfg.UseDestS3():
		ds := cfg.Destination.S3
		region := strings.TrimSpace(ds.Region)
		if region == "" {
			region = "us-east-1"
		}
		endpoint := strings.TrimSpace(ds.Endpoint)
		return s3client.NewS3Client(ctx, region, endpoint, ds.Bucket, ds.AccessKeyID, ds.SecretKey)
	default:
		return nil, fmt.Errorf("destination: set destination.b2, destination.r2, or destination.s3")
	}
}
