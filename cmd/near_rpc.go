package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

func latestNearBlockHeight(ctx context.Context, rpcURL string) (int64, error) {
	rpcURL = strings.TrimSpace(rpcURL)
	if rpcURL == "" {
		return 0, fmt.Errorf("near RPC URL is empty")
	}

	body := []byte(`{"jsonrpc":"2.0","id":"s3-migrate","method":"status","params":[]}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rpcURL, bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("create NEAR RPC request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("call NEAR RPC status: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("NEAR RPC status HTTP %d", resp.StatusCode)
	}

	var out struct {
		Result struct {
			SyncInfo struct {
				LatestBlockHeight int64 `json:"latest_block_height"`
			} `json:"sync_info"`
		} `json:"result"`
		Error any `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, fmt.Errorf("decode NEAR RPC status: %w", err)
	}
	if out.Error != nil {
		return 0, fmt.Errorf("NEAR RPC status error: %v", out.Error)
	}
	if out.Result.SyncInfo.LatestBlockHeight <= 0 {
		return 0, fmt.Errorf("NEAR RPC status returned invalid latest_block_height %d", out.Result.SyncInfo.LatestBlockHeight)
	}
	return out.Result.SyncInfo.LatestBlockHeight, nil
}

func roundedStopAt(latestHeight, batchDirs int64) int64 {
	if batchDirs <= 0 {
		return latestHeight
	}
	return (latestHeight / batchDirs) * batchDirs
}
