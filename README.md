# S3 to R2/B2 Migrator

A Go application that runs on EC2 to migrate data from **AWS S3** or **Backblaze B2** (source) into Cloudflare R2 or Backblaze B2 (destination). Designed for cost-effective transfer when the source is AWS S3 in the same region as EC2. Archives are stored under a configurable prefix (default `archives/`).

## How it works

1. **Iterates** by number: starts at `start_from`, increments (no S3 list - scales to hundreds of millions)
2. **Accumulates** directories until count reaches `batch_dirs` (e.g. 1M dirs → archive 1000001-2000000.tar.zst)
3. **Downloads** all files from those directories to local disk
4. **Packs** them into a tar.gz or tar.zst archive
5. **Verifies** the archive (zstd -t / gzip -t style) before upload
6. **Uploads** the archive to R2 or B2 (under `archives/` prefix)
7. **Persists** progress in a state file for resume on restart

## Configuration

Copy `config.example.yaml` to `config.yaml` and fill in your values.

CLI layout (similar to `cloud-console-import`): one root binary and subcommands:

```bash
./migrate help
./migrate run                              # AWS S3 or B2 source → R2/B2 destination
./migrate download                         # same as run, but only downloads to work_dir (no pack/upload)
./migrate fix-gaps                         # fill missing padded dirs under batch_* from neardata.xyz
./migrate validate-ranges                  # B2 batch coverage check
./migrate exists archives/1-1000.tar.zst   # destination object HeadObject check
```

Point at a specific config file (any subcommand):

```bash
./migrate --config /path/to/config.yaml run
./migrate --config /path/to/config.yaml download
./migrate --config /path/to/config.yaml fix-gaps
./migrate --config /path/to/config.yaml validate-ranges
./migrate --config /path/to/config.yaml exists archives/1000001-2000000.tar.zst
```

Or set `S3MIGRATE_CONFIG=/path/to/config.yaml`.

| Setting | Description |
|--------|-------------|
| **`source.aws`** | AWS S3 (or custom endpoint) read source — set `bucket` (required), optional `prefix`, `region`, `endpoint` |
| **`source.b2`** | Backblaze B2 read source — `bucket`, `prefix`, `region`, `access_key_id`, `secret_key` (omit `source.aws` when using B2) |
| **`destination.r2`** | Cloudflare R2 write destination — `bucket`, `account_id`, `access_key_id`, `secret_key`, optional `region` (default `auto`) |
| **`destination.b2`** | Backblaze B2 write destination — `bucket`, `region`, `access_key_id`, `secret_key` (use **either** R2 or B2, not both) |
| `aws.profile` / `aws.region` | Default credential chain / region fallback when `source.aws` is used (IAM on EC2, shared config, etc.) |
| **Legacy (still supported)** | Flat keys `s3.*`, `b2_source.*`, `r2.*`, `b2.*` are merged into `source` / `destination` when nested blocks are omitted |
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
| `stats_file` | JSONL: migrate batch stats; `download` appends lines with `kind: download_batch`; `validate-ranges` appends `kind: validate_ranges_missing` when batches are missing |

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
4. Build: `go build -o migrate .`
5. Run: `./migrate run`

## Download only (no pack or upload)

`download` uses the same source settings, `work_dir`, `state_file`, batching (`batch_dirs`), and concurrency as `run`, but does **not** read `destination` from config (R2/B2 credentials are optional and ignored). Each batch is written under a **deterministic** folder `work_dir/batch_<first>_<last>/` (the inclusive directory-number range for that batch, same bounds as the archive name in `run`). Layout under that folder matches the migrate download phase (padded directory names under the source prefix). Those folders are **not deleted** after a successful batch. Packing, verification, and upload are skipped.

If a target file already exists and is **non-empty**, it is **not** downloaded again (handy for resume); **0-byte** files are always re-downloaded.

Use a config that defines **source only** (or include destination if you share one file; it is not used). Prefer a dedicated `state_file` (e.g. `state-download.json`) if you also run `run` so progress does not overlap.

## fix-gaps (neardata)

For trees produced by `download`, scans `work_dir` for `batch_<first>_<last>/` directories, finds **missing** heights in that inclusive range (a height counts as present only if `<padded>/block.json` exists and is non-empty), then `GET`s `https://mainnet.neardata.xyz/v0/block/<height>` (configurable) and writes `block.json` plus `shard_<shard_id>.json` under `batch_.../<optional-prefix>/<padded>/`, matching `download_fastnear.py`.

- **Config:** `fix-gaps` uses `work_dir`, `pad_width`, and `download_concurrency` only; **source and destination are not required** (a minimal YAML with those keys is enough). Optional **`fix_gaps.log_file`**: append JSONL lines (`kind`: `gap_found`, `gap_filled`, `gap_fill_failed`) for each missing height and each successful or failed neardata fetch.
- **Flags:** `--neardata-base` (default mainnet block URL; use `https://testnet.neardata.xyz/v0/block` for testnet), `--dry-run` (no HTTP; still writes `gap_found` lines if a log file is set), `--log-file` (overrides `fix_gaps.log_file`).

## Validate ranges (B2)

To validate that all batch archives exist for the configured `[start_from, stop_at]` range (no missing batch ranges), run:

```bash
./migrate validate-ranges
```

In Docker Compose, use exec-form argv (one string per element), e.g. migration: `command: ["/app/migrate", "run"]`, validation: `command: ["/app/migrate", "validate-ranges"]`.

Notes:
- This command **checks B2** (destination) only. It does **not** query AWS S3.
- It expects a **B2 destination** (`destination.b2` or legacy `b2.*`) and requires `stop_at > 0`.
- If any batch archives are missing, one JSONL line is **appended to `stats_file`** (same as migrate stats). Lines have `"kind":"validate_ranges_missing"` so you can filter them from per-batch stats.

## Check if object exists (destination)

To check whether a specific object key exists in the configured destination store (B2 or R2), run:

```bash
./migrate exists <object-key>
```

It prints `FOUND` or `MISSING`. Missing objects exit non-zero.

## Destination (R2 or B2)

**R2:** Create an R2 bucket and generate API tokens in Cloudflare dashboard (R2 → Manage R2 API Tokens).

**B2:** Create a B2 bucket and application keys in Backblaze dashboard. Use the S3-compatible API credentials. Region format: `us-west-004`, `us-east-005`, etc.
