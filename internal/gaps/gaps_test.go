package gaps

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"s3-migrate/config"
)

func TestNeardataBlockURL_AppendsAPIKey(t *testing.T) {
	got := neardataBlockURL("https://mainnet.neardata.xyz/v0/block", 123, "SECRET")
	if got != "https://mainnet.neardata.xyz/v0/block/123?apiKey=SECRET" {
		t.Fatalf("unexpected url: %s", got)
	}
}

func TestRun_RepairsMissingShardFromLocalArchive_WhenAllBlocksPresent(t *testing.T) {
	ctx := context.Background()
	workDir := t.TempDir()
	archivesDir := t.TempDir()

	cfg := &config.Config{
		WorkDir:             workDir,
		PadWidth:            3,
		DownloadConcurrency: 2,
		Compression:         "gzip",
		ArchivePrefix:       "archives",
	}

	// Create batch dir with one height present and block.json referencing shard ids 0 and 1.
	batchName := "batch_1_1"
	heightDir := filepath.Join(workDir, batchName, "001")
	if err := os.MkdirAll(heightDir, 0755); err != nil {
		t.Fatal(err)
	}

	block := map[string]any{
		"chunks": []any{
			map[string]any{"shard_id": 0},
			map[string]any{"shard_id": 1},
		},
	}
	blockBytes, _ := json.Marshal(block)
	if err := os.WriteFile(filepath.Join(heightDir, "block.json"), append(blockBytes, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
	// shard_0 exists; shard_1 is missing.
	if err := os.WriteFile(filepath.Join(heightDir, "shard_0.json"), []byte(`{"ok":true}`+"\n"), 0644); err != nil {
		t.Fatal(err)
	}

	// Create a gzip tar archive containing the missing shard file at path 001/shard_1.json.
	archivePath := filepath.Join(archivesDir, "1-1.tar.gz")
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gw)
	content := []byte(`{"from":"archive"}` + "\n")
	hdr := &tar.Header{
		Name: "001/shard_1.json",
		Mode: 0644,
		Size: int64(len(content)),
	}
	if err := tw.WriteHeader(hdr); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(archivePath, buf.Bytes(), 0644); err != nil {
		t.Fatal(err)
	}

	// Run with local archive repair enabled; should restore shard_1.json.
	if err := Run(ctx, cfg, Options{
		NeardataBaseURL:   defaultNeardataBase,
		NeardataAPIKey:    "",
		DryRun:            false,
		LogFile:           "",
		ArchiveLocalDir:   archivesDir,
	}); err != nil {
		t.Fatalf("Run error: %v", err)
	}

	restored := filepath.Join(heightDir, "shard_1.json")
	b, err := os.ReadFile(restored)
	if err != nil {
		t.Fatalf("expected restored shard file: %v", err)
	}
	if string(b) != string(content) {
		t.Fatalf("unexpected restored content: %q", string(b))
	}
}

func TestFetchAndWriteBlock_SendsAPIKeyQuery(t *testing.T) {
	// Minimal HTTP server that asserts apiKey is present.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("apiKey") != "K" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"block":{},"shards":[]}`))
	}))
	defer srv.Close()

	dir := t.TempDir()
	client := &http.Client{}
	if err := fetchAndWriteBlock(context.Background(), client, srv.URL, "K", dir, 10); err != nil {
		t.Fatalf("fetchAndWriteBlock error: %v", err)
	}
}

