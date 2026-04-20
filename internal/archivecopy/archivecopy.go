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

	slog.Info("Listing source objects for archive copy", "prefix", prefix)
	objects, err := source.ListObjects(ctx, prefix)
	if err != nil {
		return fmt.Errorf("list source: %w", err)
	}

	var nCopy, nSkipExists, nSkipFilter atomic.Int64
	sem := semaphore.NewWeighted(int64(conc))
	g, gctx := errgroup.WithContext(ctx)

	for _, obj := range objects {
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

	if err := g.Wait(); err != nil {
		return err
	}

	slog.Info("Archive copy finished",
		"copied", nCopy.Load(),
		"skipped_existing", nSkipExists.Load(),
		"skipped_filter", nSkipFilter.Load(),
		"listed", len(objects))
	return nil
}

// Options tweaks copy-archives behavior.
type Options struct {
	DryRun    bool
	Overwrite bool
}
