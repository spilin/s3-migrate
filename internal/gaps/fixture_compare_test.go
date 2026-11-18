package gaps

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// Height 221267058, captured from mainnet.neardata.xyz and Blocks API stream
// v2_mainnet_near_blocks. The lake writer emits the same files except:
//   - Blocks API adds chunk header proposed_split: null where neardata omits the key
//   - Blocks API omits receipt_execution_outcomes[].tx_hash, which neardata sets on every outcome
func TestFixtures_NeardataAndBlocksAPI_WriteSameLakeFiles(t *testing.T) {
	nearBody := readFixture(t, "neardata", "block_221267058.json")
	apiBody := readFixture(t, "blocks-api", "block_221267058.json")

	nearDir := t.TempDir()
	apiDir := t.TempDir()
	if err := writeNeardataBody(nearDir, 221267058, nearBody); err != nil {
		t.Fatalf("write neardata: %v", err)
	}
	if err := writeNeardataBody(apiDir, 221267058, apiBody); err != nil {
		t.Fatalf("write blocks-api: %v", err)
	}

	nearFiles := listFiles(t, nearDir)
	apiFiles := listFiles(t, apiDir)
	if !reflect.DeepEqual(nearFiles, apiFiles) {
		t.Fatalf("lake file names differ\nneardata: %v\nblocks-api: %v", nearFiles, apiFiles)
	}
	if len(nearFiles) < 2 || nearFiles[0] != "block.json" {
		t.Fatalf("files: %v", nearFiles)
	}

	var rawDiffer bool
	for _, name := range nearFiles {
		a := mustRead(t, filepath.Join(nearDir, name))
		b := mustRead(t, filepath.Join(apiDir, name))
		if !bytes.Equal(a, b) {
			rawDiffer = true
		}
		na := normalizeLakeJSON(t, a)
		nb := normalizeLakeJSON(t, b)
		if !bytes.Equal(na, nb) {
			t.Errorf("%s still differs after dropping tx_hash and null proposed_split: %s", name, firstJSONDiff(t, na, nb))
		}
	}
	if !rawDiffer {
		t.Fatal("expected raw lake files to differ by tx_hash and proposed_split")
	}
}

func readFixture(t *testing.T, source, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "fixtures", source, name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// normalizeLakeJSON removes the two known source differences so the rest of the
// document can be compared exactly.
func normalizeLakeJSON(t *testing.T, raw []byte) []byte {
	t.Helper()
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	dropKnownSourceDiffs(v)
	out, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func dropKnownSourceDiffs(v any) {
	switch n := v.(type) {
	case map[string]any:
		if split, ok := n["proposed_split"]; ok && split == nil {
			delete(n, "proposed_split")
		}
		delete(n, "tx_hash")
		for _, child := range n {
			dropKnownSourceDiffs(child)
		}
	case []any:
		for _, child := range n {
			dropKnownSourceDiffs(child)
		}
	}
}
