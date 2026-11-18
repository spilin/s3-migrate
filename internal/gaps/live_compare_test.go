package gaps

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	"s3-migrate/config"
	"s3-migrate/internal/blocksapi"
)

// TestLiveNeardataAndBlocksAPI_WriteSameLakeFiles fetches one mainnet height from
// neardata.xyz and from Blocks API, writes each through writeNeardataBody, and
// checks the resulting block.json and shard_*.json files are byte-identical.
//
//	BLOCKS_API_TOKEN    required, otherwise the test is skipped
//	BLOCKS_API_ENDPOINT default producer005.nats.backend.aurora.dev:4300
//	BLOCKS_API_STREAM   default v2_mainnet_near_blocks
//	BLOCKS_API_ORIGIN   optional
//	BLOCK_HEIGHT        default 200000000
func TestLiveNeardataAndBlocksAPI_WriteSameLakeFiles(t *testing.T) {
	token := os.Getenv("BLOCKS_API_TOKEN")
	if token == "" {
		t.Skip("set BLOCKS_API_TOKEN to compare a live neardata block with Blocks API")
	}
	endpoint := envOr("BLOCKS_API_ENDPOINT", "producer005.nats.backend.aurora.dev:4300")
	stream := envOr("BLOCKS_API_STREAM", "v2_mainnet_near_blocks")
	origin := os.Getenv("BLOCKS_API_ORIGIN")
	height := int64(200000000)
	if raw := os.Getenv("BLOCK_HEIGHT"); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			t.Fatalf("BLOCK_HEIGHT: %v", err)
		}
		height = n
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	nearBody, err := fetchNeardataBlock(ctx, height)
	if err != nil {
		t.Fatalf("neardata height %d: %v", height, err)
	}
	client, err := blocksapi.Dial(config.BlocksAPIConfig{
		Endpoint: endpoint,
		Token:    token,
		Stream:   stream,
		Origin:   origin,
	})
	if err != nil {
		t.Fatalf("blocks-api dial: %v", err)
	}
	defer client.Close()
	apiBody, err := client.FetchBlockJSON(ctx, height)
	if err != nil {
		t.Fatalf("blocks-api height %d: %v", height, err)
	}

	nearDir := t.TempDir()
	apiDir := t.TempDir()
	if err := writeNeardataBody(nearDir, height, nearBody); err != nil {
		t.Fatalf("write neardata body: %v", err)
	}
	if err := writeNeardataBody(apiDir, height, apiBody); err != nil {
		t.Fatalf("write blocks-api body: %v", err)
	}

	nearFiles := listFiles(t, nearDir)
	apiFiles := listFiles(t, apiDir)
	if !reflect.DeepEqual(nearFiles, apiFiles) {
		t.Fatalf("lake file names differ\nneardata: %v\nblocks-api: %v", nearFiles, apiFiles)
	}
	if len(nearFiles) == 0 || nearFiles[0] != "block.json" {
		t.Fatalf("missing block.json: %v", nearFiles)
	}

	for _, name := range nearFiles {
		a, err := os.ReadFile(filepath.Join(nearDir, name))
		if err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(filepath.Join(apiDir, name))
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Equal(a, b) {
			continue
		}
		diff := firstJSONDiff(t, a, b)
		t.Errorf("%s differs (%d vs %d bytes): %s", name, len(a), len(b), diff)
	}
	if t.Failed() {
		t.Fatalf("height %d lake files are not identical", height)
	}
	t.Logf("height %d matches across %d files", height, len(nearFiles))
}

func fetchNeardataBlock(ctx context.Context, height int64) ([]byte, error) {
	url := neardataBlockURL("https://mainnet.neardata.xyz/v0/block", height, "")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := (&http.Client{Timeout: 2 * time.Minute}).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		snippet := body
		if len(snippet) > 300 {
			snippet = snippet[:300]
		}
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, snippet)
	}
	return body, nil
}

func listFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, ent := range entries {
		if ent.IsDir() {
			t.Fatalf("unexpected directory %s", ent.Name())
		}
		names = append(names, ent.Name())
	}
	sort.Strings(names)
	return names
}

func firstJSONDiff(t *testing.T, a, b []byte) string {
	t.Helper()
	var left, right any
	if err := json.Unmarshal(a, &left); err != nil {
		return fmt.Sprintf("neardata json: %v", err)
	}
	if err := json.Unmarshal(b, &right); err != nil {
		return fmt.Sprintf("blocks-api json: %v", err)
	}
	if diff := diffValue("$", left, right); diff != "" {
		return diff
	}
	return "values match after unmarshal; stored bytes differ only by formatting"
}

func diffValue(path string, a, b any) string {
	if reflect.DeepEqual(a, b) {
		return ""
	}
	am, aok := a.(map[string]any)
	bm, bok := b.(map[string]any)
	if aok && bok {
		seen := map[string]struct{}{}
		var keys []string
		for k := range am {
			keys = append(keys, k)
			seen[k] = struct{}{}
		}
		for k := range bm {
			if _, ok := seen[k]; !ok {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		for _, k := range keys {
			av, aHas := am[k]
			bv, bHas := bm[k]
			child := path + "." + k
			if !aHas {
				return fmt.Sprintf("%s missing from neardata", child)
			}
			if !bHas {
				return fmt.Sprintf("%s missing from blocks-api", child)
			}
			if diff := diffValue(child, av, bv); diff != "" {
				return diff
			}
		}
		return ""
	}
	aa, aok := a.([]any)
	ba, bok := b.([]any)
	if aok && bok {
		if len(aa) != len(ba) {
			return fmt.Sprintf("%s length %d vs %d", path, len(aa), len(ba))
		}
		for i := range aa {
			if diff := diffValue(fmt.Sprintf("%s[%d]", path, i), aa[i], ba[i]); diff != "" {
				return diff
			}
		}
		return ""
	}
	return fmt.Sprintf("%s neardata=%v blocks-api=%v", path, preview(a), preview(b))
}

func preview(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	if len(b) > 180 {
		return string(b[:180]) + "..."
	}
	return string(b)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
