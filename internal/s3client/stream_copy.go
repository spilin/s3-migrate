package s3client

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/time/rate"
)

// StreamCopyOpts configures StreamCopyTo (throughput limit is shared across concurrent copies when the same limiter is passed).
type StreamCopyOpts struct {
	BytesPerSecLimiter *rate.Limiter
}

// NewThroughputLimiter returns a token bucket limiter for total average bytes/sec with ~250ms burst (nil if bps <= 0).
func NewThroughputLimiter(bytesPerSec int64) *rate.Limiter {
	if bytesPerSec <= 0 {
		return nil
	}
	burst := int(bytesPerSec / 4)
	if burst < 262144 {
		burst = 262144
	}
	const maxBurst = 16 << 20
	if burst > maxBurst {
		burst = maxBurst
	}
	return rate.NewLimiter(rate.Limit(float64(bytesPerSec)), burst)
}

// HeadObjectMeta returns Content-Length and raw ETag from HeadObject.
func (c *Client) HeadObjectMeta(ctx context.Context, key string) (size int64, etag string, err error) {
	out, err := c.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return 0, "", fmt.Errorf("head object %s: %w", key, err)
	}
	if out.ContentLength != nil {
		size = *out.ContentLength
	}
	if out.ETag != nil {
		etag = *out.ETag
	}
	return size, etag, nil
}

func stripETagQuotes(etag string) string {
	return strings.Trim(etag, `"`)
}

// etagLooksLikeSinglePartMD5 is true when ETag is a 32-hex MD5 (S3/B2 single-part object).
func etagLooksLikeSinglePartMD5(etag string) bool {
	s := stripETagQuotes(etag)
	if len(s) != 32 {
		return false
	}
	for i := 0; i < 32; i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

type throttleReadCloser struct {
	ctx context.Context
	r   io.ReadCloser
	lim *rate.Limiter
}

func (t *throttleReadCloser) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	if n > 0 && t.lim != nil {
		if werr := t.lim.WaitN(t.ctx, n); werr != nil {
			return n, werr
		}
	}
	return n, err
}

func (t *throttleReadCloser) Close() error {
	return t.r.Close()
}

type meterMD5ReadCloser struct {
	r io.ReadCloser
	n int64
	h hash.Hash
}

func (m *meterMD5ReadCloser) Read(p []byte) (int, error) {
	n, err := m.r.Read(p)
	if n > 0 {
		m.n += int64(n)
		if m.h != nil {
			_, _ = m.h.Write(p[:n])
		}
	}
	return n, err
}

func (m *meterMD5ReadCloser) Close() error {
	return m.r.Close()
}

func streamCopyBackoff(ctx context.Context, attempt int) error {
	backoff := time.Duration(1<<uint(attempt)) * time.Second
	if backoff > 30*time.Second {
		backoff = 30 * time.Second
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(backoff):
		return nil
	}
}

// StreamCopyTo streams an object from c to dest under destKey.
// It Heads the source for size/ETag, optionally verifies a single-part MD5 ETag over the bytes read,
// Heads the destination after upload to confirm size, and fails if streamed byte count does not match.
func (c *Client) StreamCopyTo(ctx context.Context, srcKey string, dest *Client, destKey string, opt StreamCopyOpts) error {
	const maxRetries = 6
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		headSize, headETag, err := c.HeadObjectMeta(ctx, srcKey)
		if err != nil {
			return err
		}

		out, err := c.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(c.bucket),
			Key:    aws.String(srcKey),
		})
		if err != nil {
			lastErr = err
			if attempt < maxRetries-1 && isRetryableS3Error(err) {
				if err := streamCopyBackoff(ctx, attempt); err != nil {
					return err
				}
				continue
			}
			return fmt.Errorf("get object %s: %w", srcKey, err)
		}

		if out.ContentLength != nil && *out.ContentLength != headSize {
			out.Body.Close()
			return fmt.Errorf("get %s content-length %d disagrees with head %d", srcKey, *out.ContentLength, headSize)
		}

		base := io.ReadCloser(out.Body)
		var h hash.Hash
		if etagLooksLikeSinglePartMD5(headETag) {
			h = md5.New()
		}
		meter := &meterMD5ReadCloser{r: base, h: h}

		var body io.ReadCloser = meter
		if opt.BytesPerSecLimiter != nil {
			body = &throttleReadCloser{ctx: ctx, r: meter, lim: opt.BytesPerSecLimiter}
		}

		uploader := manager.NewUploader(dest.client)
		_, err = uploader.Upload(ctx, &s3.PutObjectInput{
			Bucket: aws.String(dest.bucket),
			Key:    aws.String(destKey),
			Body:   body,
		})
		_ = body.Close()
		if err != nil {
			lastErr = err
			if attempt < maxRetries-1 && isRetryableS3Error(err) {
				if err := streamCopyBackoff(ctx, attempt); err != nil {
					return err
				}
				continue
			}
			return fmt.Errorf("upload %s: %w", destKey, err)
		}

		if meter.n != headSize {
			return fmt.Errorf("verify %s: read %d bytes, head said %d", srcKey, meter.n, headSize)
		}
		if meter.h != nil {
			sum := hex.EncodeToString(meter.h.Sum(nil))
			want := strings.ToLower(stripETagQuotes(headETag))
			if sum != want {
				return fmt.Errorf("verify %s: md5 %s != source etag %s", srcKey, sum, want)
			}
		}

		dsz, _, err := dest.HeadObjectMeta(ctx, destKey)
		if err != nil {
			return fmt.Errorf("post-upload head %s: %w", destKey, err)
		}
		if dsz != headSize {
			return fmt.Errorf("post-upload verify %s: destination size %d != source %d", destKey, dsz, headSize)
		}

		return nil
	}
	return fmt.Errorf("stream copy %s -> %s: %w", srcKey, destKey, lastErr)
}
