# S3 to R2 Migrator

A Go application that runs on EC2 to migrate data from an S3 bucket to Cloudflare R2. Designed for cost-effective transfer: S3→EC2 (same region) is free, then data is packed and uploaded to R2.

## How it works

1. **Iterates** by number: starts at `start_from`, increments (no S3 list - scales to hundreds of millions)
2. **Accumulates** directories until count reaches `batch_dirs` (e.g. 1M dirs → archive 1000001-2000000.tar.gz)
3. **Downloads** all files from those directories to local disk
4. **Packs** them into a tar.gz archive
5. **Uploads** the archive to R2
6. **Persists** progress in a state file for resume on restart

## Configuration

Copy `config.example.yaml` to `config.yaml` and fill in your values.

| Setting | Description |
|--------|-------------|
| `s3.bucket` | Source S3 bucket name |
| `s3.prefix` | Optional prefix to limit migration (e.g. `data/`) |
| `s3.region` | AWS region (use same as EC2 for free transfer) |
| `r2.bucket` | Destination R2 bucket |
| `r2.account_id` | Cloudflare account ID |
| `r2.access_key_id` | R2 API token access key |
| `r2.secret_key` | R2 API token secret |
| `start_from` | Directory number to start (e.g. `9820210` → `000009820210/`) |
| `stop_at` | Optional max number to process (0 = no limit) |
| `pad_width` | Zero-pad width for dir names (default 12) |
| `download_concurrency` | Parallel file downloads per directory (default 10) |
| `directory_concurrency` | Parallel directory processing (default 20) |
| `batch_dirs` | Pack & upload when this many directories accumulated |
| `compression` | gzip or zstd (zstd ~15-25% better for JSON) |
| `compression_level` | zstd level 1-22 (default 6; good for JSON; gzip ignores) |
| `consecutive_empty` | Stop after this many consecutive empty dirs (default 1000) |
| `work_dir` | Local temp directory for downloads |
| `state_file` | Path to store last processed number for resume |

## Environment variables

All settings can be overridden via env vars with `S3MIGRATE_` prefix.

**Logging:** `S3MIGRATE_LOG_LEVEL=debug` enables verbose output (per-file downloads, skipped dirs).

**S3 503 SlowDown:** Downloads automatically retry with exponential backoff (1s, 2s, 4s… up to 30s). If you still hit rate limits, lower `directory_concurrency` and `download_concurrency`.

## EC2 setup

1. Launch EC2 in the **same region** as your S3 bucket for free data transfer
2. Attach an IAM role with `s3:GetObject`, `s3:ListBucket` on the source bucket
3. Store R2 credentials in SSM Parameter Store or Secrets Manager for security
4. Build: `go build -o migrate ./cmd/migrate`
5. Run: `./migrate`

## R2 bucket

Create an R2 bucket and generate API tokens in Cloudflare dashboard (R2 → Manage R2 API Tokens).
