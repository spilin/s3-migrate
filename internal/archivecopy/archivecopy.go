package archivecopy

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"s3-migrate/config"
	"s3-migrate/internal/s3client"
)

// Same naming pattern as migrate-produced archives (optional filter).
var batchArchiveKeyRe = regexp.MustCompile(`(?i)^(?:(.+)/)?([0-9]+)-([0-9]+)\.tar\.(gz|zst)$`)

// Run copies objects from the source bucket (under archive_copy.source_prefix) to the destination.
func Run(ctx context.Context, cfg *config.Config, source, dest *s3client.Client, opts Options) error {
	if cfg.ArchiveCopy == nil || strings.TrimSpace(cfg.ArchiveCopy.SourcePrefix) == "" {
		return fmt.Errorf("archive_copy.source_prefix is required")
	}

	prefix := strings.TrimSpace(cfg.ArchiveCopy.SourcePrefix)
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	conc := cfg.ArchiveCopy.Concurrency
	if conc <= 0 {
		conc = cfg.DownloadConcurrency
	}
	if conc <= 0 {
		conc = 5
	}

	bps := cfg.ArchiveCopyMaxBytesPerSecond()
	lim := s3client.NewThroughputLimiter(bps)
	if lim != nil {
		slog.Info("Archive copy throughput cap (shared across concurrent transfers)", "max_bytes_per_sec", bps)
	}

	slog.Info("Archive copy: streaming source list by S3 page", "prefix", prefix)

	var nCopy, nSkipExists, nSkipFilter, nListed atomic.Int64
	sem := semaphore.NewWeighted(int64(conc))
	g, gctx := errgroup.WithContext(ctx)

	err := source.WalkObjectPages(ctx, prefix, func(pageIdx int, page []s3client.ObjectInfo, morePages bool) error {
		slog.Debug("Archive copy processing list page", "page", pageIdx, "keys_in_page", len(page))
		for _, obj := range page {
			nListed.Add(1)
			key := obj.Key
			if key == "" {
				continue
			}
			if !cfg.ArchiveCopy.CopyAllUnderPrefix {
				if !batchArchiveKeyRe.MatchString(key) {
					nSkipFilter.Add(1)
					continue
				}
			}

			destKey := cfg.ArchiveCopyDestinationKey(key)
			if opts.DryRun {
				slog.Info("dry-run: would copy", "from", key, "to", destKey, "size", obj.Size)
				nCopy.Add(1)
				continue
			}

			srcKey := key
			dstKey := destKey
			size := obj.Size

			if err := sem.Acquire(gctx, 1); err != nil {
				return err
			}
			g.Go(func() error {
				defer sem.Release(1)
				if !opts.Overwrite {
					ok, err := dest.Exists(gctx, dstKey)
					if err != nil {
						return fmt.Errorf("head %s: %w", dstKey, err)
					}
					if ok {
						slog.Debug("skip existing destination object", "key", dstKey)
						nSkipExists.Add(1)
						return nil
					}
				}
				slog.Info("Copying archive", "from", srcKey, "to", dstKey, "size_bytes", size)
				opt := s3client.StreamCopyOpts{BytesPerSecLimiter: lim}
				if err := source.StreamCopyTo(gctx, srcKey, dest, dstKey, opt); err != nil {
					return fmt.Errorf("copy %q -> %q: %w", srcKey, dstKey, err)
				}
				nCopy.Add(1)
				return nil
			})
		}
		// Align with s3client.ListObjectsProgressLogEveryN so Info lines stay readable on huge buckets.
		if pageIdx == 1 || pageIdx%s3client.ListObjectsProgressLogEveryN == 0 || !morePages {
			slog.Info("Archive copy queued jobs for list page",
				"page", pageIdx, "keys_in_page", len(page),
				"source_keys_seen", nListed.Load(),
				"more_list_pages", morePages)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("list source: %w", err)
	}

	if err := g.Wait(); err != nil {
		return err
	}

	slog.Info("Archive copy finished",
		"copied", nCopy.Load(),
		"skipped_existing", nSkipExists.Load(),
		"skipped_filter", nSkipFilter.Load(),
		"listed", nListed.Load())
	return nil
}

// Options tweaks copy-archives behavior.
type Options struct {
	DryRun    bool
	Overwrite bool
}
