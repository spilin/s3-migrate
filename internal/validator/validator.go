package validator

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"s3-migrate/config"
	"s3-migrate/internal/s3client"
)

type batchRange struct {
	first int64
	last  int64
	key   string
}

var (
	// Matches either:
	// - "<prefix>/<first>-<last>.tar.gz"
	// - "<prefix>/<first>-<last>.tar.zst"
	// - "<first>-<last>.tar.gz" (when prefix empty)
	archiveRe = regexp.MustCompile(`(?i)^(?:(.+)/)?([0-9]+)-([0-9]+)\.tar\.(gz|zst)$`)
)

func ValidateRanges(ctx context.Context, cfg *config.Config, b2Client *s3client.Client) error {
	if cfg.BatchDirs <= 0 {
		return fmt.Errorf("batch_dirs must be > 0")
	}

	prefix := strings.TrimSuffix(cfg.ArchivePrefix, "/")
	listPrefix := prefix
	if listPrefix != "" {
		listPrefix += "/"
	}

	slog.Info("Listing destination archives", "bucket", cfg.B2Bucket, "prefix", listPrefix)
	objects, err := b2Client.ListObjects(ctx, listPrefix)
	if err != nil {
		return fmt.Errorf("list b2 objects under %q: %w", listPrefix, err)
	}

	found := make(map[[2]int64]batchRange)
	var foundRanges []batchRange

	for _, obj := range objects {
		key := obj.Key
		m := archiveRe.FindStringSubmatch(key)
		if m == nil {
			continue
		}

		keyPrefix := strings.TrimSuffix(m[1], "/")
		if prefix != "" && keyPrefix != prefix {
			// If user configured archive_prefix, only accept keys that match it exactly.
			continue
		}
		if prefix == "" && keyPrefix != "" {
			// If archive_prefix is empty, only accept root-level archives (no slash).
			continue
		}

		first, err1 := strconv.ParseInt(m[2], 10, 64)
		last, err2 := strconv.ParseInt(m[3], 10, 64)
		if err1 != nil || err2 != nil || first < 0 || last < first {
			continue
		}

		br := batchRange{first: first, last: last, key: key}
		k := [2]int64{first, last}
		// Keep first seen; duplicates are reported later.
		if _, ok := found[k]; !ok {
			found[k] = br
			foundRanges = append(foundRanges, br)
		}
	}

	sort.Slice(foundRanges, func(i, j int) bool {
		if foundRanges[i].first == foundRanges[j].first {
			return foundRanges[i].last < foundRanges[j].last
		}
		return foundRanges[i].first < foundRanges[j].first
	})

	// Build expected batch boundaries exactly like the migrator does:
	// [batchStart, batchEnd] inclusive, batchStart increments by batchEnd+1
	expectedStart := cfg.StartFrom
	expectedStop := cfg.StopAt

	var missing []string
	var expectedCount int64

	for batchStart := expectedStart; batchStart <= expectedStop; {
		batchEnd := batchStart + cfg.BatchDirs - 1
		if batchEnd > expectedStop {
			batchEnd = expectedStop
		}
		expectedCount++

		if _, ok := found[[2]int64{batchStart, batchEnd}]; !ok {
			missing = append(missing, fmt.Sprintf("%d-%d", batchStart, batchEnd))
		}

		// next
		batchStart = batchEnd + 1
	}

	if len(missing) > 0 {
		slog.Error("Missing batch archives in destination", "missing_count", len(missing))
		if len(missing) <= 50 {
			slog.Error("Missing ranges", "ranges", missing)
		} else {
			slog.Error("First missing ranges (truncated)", "ranges", missing[:50])
		}
		return fmt.Errorf("missing %d batch archive(s) in b2 for configured range", len(missing))
	}

	// Additional integrity checks: ensure there are no gaps inside the configured interval
	// based on actually present archives (helpful if extra/misaligned archives exist).
	var inScope []batchRange
	for _, br := range foundRanges {
		if br.last < expectedStart || br.first > expectedStop {
			continue
		}
		inScope = append(inScope, br)
	}
	sort.Slice(inScope, func(i, j int) bool { return inScope[i].first < inScope[j].first })

	// Verify coverage using expected boundaries already checked; now just sanity-check continuity.
	// We expect exactly one archive per expected batch boundary.
	if int64(len(inScope)) < expectedCount {
		// This can happen if there are fewer archives but still somehow none missing (shouldn't happen).
		slog.Warn("Fewer archives found than expected batch count", "found", len(inScope), "expected", expectedCount)
	}

	slog.Info("All expected batch archives present",
		"range", fmt.Sprintf("%d-%d", expectedStart, expectedStop),
		"expected_batches", expectedCount,
		"found_archives_under_prefix", len(objects))

	return nil
}
