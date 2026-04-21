package s3client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithy "github.com/aws/smithy-go"
)

type ObjectInfo struct {
	Key  string
	Size int64
}

type Client struct {
	client *s3.Client
	bucket string
}

// NewS3Client creates a client for AWS S3 (or compatible endpoint).
// If accessKeyID and secretKey are both non-empty, static credentials are used; otherwise the default
// credential chain applies (env, shared config/profile, IMDS, etc.).
func NewS3Client(ctx context.Context, region, endpoint, bucket, accessKeyID, secretKey string) (*Client, error) {
	opts := []func(*config.LoadOptions) error{
		config.WithRegion(region),
	}
	if region == "" {
		opts = append(opts, config.WithRegion("us-east-1"))
	}
	ak := strings.TrimSpace(accessKeyID)
	sk := strings.TrimSpace(secretKey)
	if ak != "" && sk != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(ak, sk, ""),
		))
	}

	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	clientOpts := []func(*s3.Options){
		func(o *s3.Options) { o.DisableLogOutputChecksumValidationSkipped = true },
	}
	if endpoint != "" {
		clientOpts = append(clientOpts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		})
	}

	return &Client{
		client: s3.NewFromConfig(cfg, clientOpts...),
		bucket: bucket,
	}, nil
}

// NewR2Client creates a client for Cloudflare R2 (S3-compatible)
func NewR2Client(ctx context.Context, accountID, accessKey, secretKey, bucket string) (*Client, error) {
	endpoint := fmt.Sprintf("https://%s.r2.cloudflarestorage.com", accountID)
	resolver := aws.EndpointResolverWithOptionsFunc(func(_, _ string, _ ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{URL: endpoint}, nil
	})

	cfg := aws.Config{
		Region: "auto",
		Credentials: credentials.NewStaticCredentialsProvider(
			accessKey,
			secretKey,
			"",
		),
		EndpointResolverWithOptions: resolver,
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.DisableLogOutputChecksumValidationSkipped = true
	})

	return &Client{client: client, bucket: bucket}, nil
}

// NewB2Client creates a client for Backblaze B2 (S3-compatible).
// Region is the B2 region (e.g. us-west-004, us-east-005).
func NewB2Client(ctx context.Context, region, accessKey, secretKey, bucket string) (*Client, error) {
	if region == "" {
		region = "us-west-004"
	}
	endpoint := fmt.Sprintf("https://s3.%s.backblazeb2.com", region)
	resolver := aws.EndpointResolverWithOptionsFunc(func(_, _ string, _ ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{URL: endpoint}, nil
	})

	cfg := aws.Config{
		Region: region,
		Credentials: credentials.NewStaticCredentialsProvider(
			accessKey,
			secretKey,
			"",
		),
		EndpointResolverWithOptions: resolver,
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.DisableLogOutputChecksumValidationSkipped = true
	})

	return &Client{client: client, bucket: bucket}, nil
}

// ListCommonPrefixes returns "directory" prefixes when delimiter is "/"
func (c *Client) ListCommonPrefixes(ctx context.Context, prefix, delimiter string) ([]string, error) {
	var prefixes []string
	paginator := s3.NewListObjectsV2Paginator(c.client, &s3.ListObjectsV2Input{
		Bucket:    aws.String(c.bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String(delimiter),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list objects: %w", err)
		}
		for _, cp := range page.CommonPrefixes {
			if cp.Prefix != nil {
				prefixes = append(prefixes, *cp.Prefix)
			}
		}
	}
	return prefixes, nil
}

// Exists returns true if the object exists in the bucket, false if not found.
// Returns error only for real failures (e.g. network, permissions).
func (c *Client) Exists(ctx context.Context, key string) (bool, error) {
	_, err := c.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			if apiErr.ErrorCode() == "NotFound" || apiErr.ErrorCode() == "NoSuchKey" {
				return false, nil
			}
		}
		return false, fmt.Errorf("head object %s: %w", key, err)
	}
	return true, nil
}

// ListObjectsProgressLogEveryN controls how often WalkObjectPages emits Info-level
// progress lines between the first and last page (Debug logs every page).
const ListObjectsProgressLogEveryN = 10

// WalkObjectPages calls fn once per ListObjectsV2 page, in lexicographic key order.
// morePages is true when additional list pages will follow this one.
// Logging: one Info line at start; Info every ListObjectsProgressLogEveryN pages plus first
// and last page; Debug for every page (page fetch latency and sizes). Use
// S3MIGRATE_LOG_LEVEL=debug to see per-page lines.
func (c *Client) WalkObjectPages(ctx context.Context, prefix string, fn func(pageIndex int, page []ObjectInfo, morePages bool) error) error {
	paginator := s3.NewListObjectsV2Paginator(c.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(prefix),
	})

	slog.Info("S3 ListObjectsV2 starting", "bucket", c.bucket, "prefix", prefix)
	listStart := time.Now()
	var pageIdx int
	var totalObjects int64

	for paginator.HasMorePages() {
		pageIdx++
		pageStart := time.Now()
		out, err := paginator.NextPage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return fmt.Errorf("S3 ListObjectsV2 page %d canceled (signal, docker stop, or parent deadline; objects_so_far=%d): %w",
					pageIdx, totalObjects, err)
			}
			return fmt.Errorf("S3 ListObjectsV2 page %d (objects_so_far=%d): %w", pageIdx, totalObjects, err)
		}

		batch := make([]ObjectInfo, 0, len(out.Contents))
		for _, obj := range out.Contents {
			if obj.Key == nil {
				continue
			}
			size := int64(0)
			if obj.Size != nil {
				size = *obj.Size
			}
			batch = append(batch, ObjectInfo{Key: *obj.Key, Size: size})
		}

		totalObjects += int64(len(batch))
		more := paginator.HasMorePages()
		pageMs := time.Since(pageStart).Milliseconds()
		apiKeyCount := int32(-1)
		if out.KeyCount != nil {
			apiKeyCount = *out.KeyCount
		}

		slog.Debug("S3 ListObjectsV2 page",
			"bucket", c.bucket, "prefix", prefix,
			"page", pageIdx, "page_objects", len(batch),
			"objects_so_far", totalObjects, "more_pages", more,
			"page_fetch_ms", pageMs,
			"api_key_count", apiKeyCount,
			"api_is_truncated", aws.ToBool(out.IsTruncated),
		)
		if pageIdx == 1 || pageIdx%ListObjectsProgressLogEveryN == 0 || !more {
			slog.Info("S3 ListObjectsV2 progress",
				"bucket", c.bucket, "prefix", prefix,
				"page", pageIdx, "page_objects", len(batch),
				"objects_so_far", totalObjects, "more_pages", more,
				"page_fetch_ms", pageMs,
				"api_is_truncated", aws.ToBool(out.IsTruncated),
			)
		}

		if err := fn(pageIdx, batch, more); err != nil {
			return err
		}
	}

	slog.Info("S3 ListObjectsV2 finished",
		"bucket", c.bucket, "prefix", prefix,
		"pages", pageIdx, "objects", totalObjects,
		"elapsed_ms", time.Since(listStart).Milliseconds(),
	)
	return nil
}

// ListObjects returns all objects under prefix with their sizes (no delimiter).
func (c *Client) ListObjects(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	var objects []ObjectInfo
	err := c.WalkObjectPages(ctx, prefix, func(_ int, page []ObjectInfo, _ bool) error {
		objects = append(objects, page...)
		return nil
	})
	return objects, err
}

// isRetryableS3Error returns true for 503 SlowDown, 500, 502, etc.
func isRetryableS3Error(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "SlowDown") || strings.Contains(s, "503") ||
		strings.Contains(s, "500") || strings.Contains(s, "502") ||
		strings.Contains(s, "ServiceUnavailable") || strings.Contains(s, "InternalError")
}

// Download fetches an object and writes to destPath (retries on 503 SlowDown)
func (c *Client) Download(ctx context.Context, key, destPath string) error {
	const maxRetries = 6
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		out, err := c.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(c.bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			lastErr = err
			if attempt < maxRetries-1 && isRetryableS3Error(err) {
				backoff := time.Duration(1<<uint(attempt)) * time.Second
				if backoff > 30*time.Second {
					backoff = 30 * time.Second
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(backoff):
				}
				continue
			}
			return fmt.Errorf("get object %s: %w", key, err)
		}

		if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
			out.Body.Close()
			return fmt.Errorf("mkdir: %w", err)
		}

		f, err := os.Create(destPath)
		if err != nil {
			out.Body.Close()
			return fmt.Errorf("create file: %w", err)
		}

		_, err = io.Copy(f, out.Body)
		out.Body.Close()
		f.Close()
		if err != nil {
			os.Remove(destPath)
			return fmt.Errorf("copy: %w", err)
		}
		return nil
	}
	return fmt.Errorf("get object %s: %w", key, lastErr)
}

// Upload sends a local file to the bucket using the SDK's upload manager.
// For files over ~100MB it automatically uses concurrent multipart upload
// for better reliability and throughput.
func (c *Client) Upload(ctx context.Context, key, localPath string) error {
	return c.UploadWithChecksum(ctx, key, localPath, "")
}

// UploadWithChecksum uploads a file with optional SHA256 checksum verification.
// If checksumBase64 is non-empty, it is sent as x-amz-checksum-sha256 and the
// server verifies integrity on receive (S3/R2/B2 compatible).
func (c *Client) UploadWithChecksum(ctx context.Context, key, localPath, checksumBase64 string) error {
	f, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	input := &s3.PutObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
		Body:   f,
	}
	if checksumBase64 != "" {
		input.ChecksumSHA256 = aws.String(checksumBase64)
	}

	uploader := manager.NewUploader(c.client)
	_, err = uploader.Upload(ctx, input)
	if err != nil {
		return fmt.Errorf("upload %s: %w", key, err)
	}
	return nil
}
