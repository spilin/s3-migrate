package gaps

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestNeardataBlockURL_AppendsAPIKey(t *testing.T) {
	got := neardataBlockURL("https://mainnet.neardata.xyz/v0/block", 123, "SECRET")
	if got != "https://mainnet.neardata.xyz/v0/block/123?apiKey=SECRET" {
		t.Fatalf("unexpected url: %s", got)
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
	if _, err := fetchAndWriteBlock(context.Background(), client, srv.URL, "K", dir, 10, nil); err != nil {
		t.Fatalf("fetchAndWriteBlock error: %v", err)
	}
}

type stubFallback struct {
	body  []byte
	err   error
	calls int
}

func (s *stubFallback) FetchBlockJSON(context.Context, int64) ([]byte, error) {
	s.calls++
	return s.body, s.err
}

func TestFetchAndWriteBlock_FallsBackOn429(t *testing.T) {
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	fb := &stubFallback{body: []byte(`{"block":{"header":{"height":10}},"shards":[{"shard_id":0}]}`)}
	dir := t.TempDir()
	source, err := fetchAndWriteBlock(context.Background(), &http.Client{}, srv.URL, "", dir, 10, fb)
	if err != nil {
		t.Fatalf("fetchAndWriteBlock error: %v", err)
	}
	if source != "blocks-api" {
		t.Fatalf("source = %q", source)
	}
	if hits != 1 {
		t.Fatalf("neardata hits = %d, want 1", hits)
	}
	if fb.calls != 1 {
		t.Fatalf("fallback calls = %d, want 1", fb.calls)
	}
	if _, err := os.Stat(dir + "/block.json"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir + "/shard_0.json"); err != nil {
		t.Fatal(err)
	}
}
