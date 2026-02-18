package s3client

import (
	"context"
	"errors"
	"fmt"
	"io"
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

// NewS3Client creates a client for AWS S3 (or compatible endpoint)
func NewS3Client(ctx context.Context, region, endpoint, bucket string) (*Client, error) {
	opts := []func(*config.LoadOptions) error{
		config.WithRegion(region),
	}
	if region == "" {
		opts = append(opts, config.WithRegion("us-east-1"))
	}

	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	clientOpts := []func(*s3.Options){}
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

// ListObjects returns all objects under prefix with their sizes (no delimiter)
func (c *Client) ListObjects(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	var objects []ObjectInfo
	paginator := s3.NewListObjectsV2Paginator(c.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list objects: %w", err)
		}
		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}
			size := int64(0)
			if obj.Size != nil {
				size = *obj.Size
			}
			objects = append(objects, ObjectInfo{Key: *obj.Key, Size: size})
		}
	}
	return objects, nil
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
	f, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	uploader := manager.NewUploader(c.client)
	_, err = uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
		Body:   f,
	})
	if err != nil {
		return fmt.Errorf("upload %s: %w", key, err)
	}
	return nil
}
