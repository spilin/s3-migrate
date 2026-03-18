# S3 to R2/B2 Migrator

A Go application that runs on EC2 to migrate data from an S3 bucket to Cloudflare R2 or Backblaze B2. Designed for cost-effective transfer: S3→EC2 (same region) is free, then data is packed and uploaded to R2 or B2. Archives are stored under a configurable prefix (default `archives/`).

## How it works

1. **Iterates** by number: starts at `start_from`, increments (no S3 list - scales to hundreds of millions)
2. **Accumulates** directories until count reaches `batch_dirs` (e.g. 1M dirs → archive 1000001-2000000.tar.zst)
3. **Downloads** all files from those directories to local disk
4. **Packs** them into a tar.gz or tar.zst archive
5. **Verifies** the archive (zstd -t / gzip -t style) before upload
6. **Uploads** the archive to R2 or B2 (under `archives/` prefix) with SHA256 checksum for server-side verification
7. **Persists** progress in a state file for resume on restart

## Configuration

Copy `config.example.yaml` to `config.yaml` and fill in your values.

| Setting | Description |
|--------|-------------|
| `s3.bucket` | Source S3 bucket name |
| `s3.prefix` | Optional prefix to limit migration (e.g. `data/`) |
| `s3.region` | AWS region (use same as EC2 for free transfer) |
| `r2.bucket` | Destination R2 bucket (use r2 OR b2) |
| `r2.account_id` | Cloudflare account ID |
| `r2.access_key_id` | R2 API token access key |
| `r2.secret_key` | R2 API token secret |
| `b2.bucket` | Destination B2 bucket (alternative to R2) |
| `b2.region` | B2 region (e.g. `us-west-004`, `us-east-005`) |
| `b2.access_key_id` | B2 application key ID |
| `b2.secret_key` | B2 application key |
| `start_from` | Directory number to start (e.g. `9820210` → `000009820210/`) |
| `stop_at` | Optional max number to process (0 = no limit) |
| `pad_width` | Zero-pad width for dir names (default 12) |
| `download_concurrency` | Parallel file downloads per directory (default 10) |
| `directory_concurrency` | Parallel directory processing (default 20) |
| `batch_dirs` | Pack & upload when this many directories accumulated |
| `compression` | gzip or zstd (zstd ~15-25% better for JSON) |
| `compression_level` | zstd level 1-22 (default 6; good for JSON; gzip ignores) |
| `consecutive_empty` | Stop after this many consecutive empty dirs (default 1000) |
| `archive_prefix` | Destination prefix for archives (default: `archives`) |
| `work_dir` | Local temp directory for downloads |
| `state_file` | Path to store last processed number for resume |

## Environment variables

All settings can be overridden via env vars with `S3MIGRATE_` prefix.

**Logging:** `S3MIGRATE_LOG_LEVEL=debug` enables verbose output (per-file downloads, skipped dirs).

**S3 503 SlowDown:** Downloads automatically retry with exponential backoff (1s, 2s, 4s… up to 30s). If you still hit rate limits, lower `directory_concurrency` and `download_concurrency`.

## Docker

```bash
# Create config and data directories
mkdir -p config data
cp config.example.yaml config/config.yaml
# Edit config/config.yaml with your settings

# Run with docker compose
docker compose up --build
```

Config is loaded from `./config/` (mounted to `/etc/s3-migrate`). Work dir, state, and stats are stored in `./data/`.

## EC2 setup

1. Launch EC2 in the **same region** as your S3 bucket for free data transfer
2. Attach an IAM role with `s3:GetObject`, `s3:ListBucket` on the source bucket
3. Store R2 credentials in SSM Parameter Store or Secrets Manager for security
4. Build: `go build -o migrate ./cmd/migrate`
5. Run: `./migrate`

## Destination (R2 or B2)

**R2:** Create an R2 bucket and generate API tokens in Cloudflare dashboard (R2 → Manage R2 API Tokens).

**B2:** Create a B2 bucket and application keys in Backblaze dashboard. Use the S3-compatible API credentials. Region format: `us-west-004`, `us-east-005`, etc.
