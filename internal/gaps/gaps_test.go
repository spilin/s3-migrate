package gaps

import (
	"context"
	"net/http"
	"net/http/httptest"
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
	if err := fetchAndWriteBlock(context.Background(), client, srv.URL, "K", dir, 10); err != nil {
		t.Fatalf("fetchAndWriteBlock error: %v", err)
	}
}

