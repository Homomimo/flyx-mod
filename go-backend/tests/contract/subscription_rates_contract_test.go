package contract_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go-backend/internal/auth"
	"go-backend/internal/store/model"
)

// Real router, SQLite and node WebSocket; only the nft counters are synthetic.
func TestLiveSubscriptionRateStream(t *testing.T) {
	const key = "test-rate-key"
	const path = "/api/v1/subscription/rates"
	router, db := setupContractRouter(t, key)
	srv := httptest.NewServer(router)
	defer srv.Close()
	if out := kernelContractPost(t, router, "", path, "{}"); out.Code != 401 {
		t.Fatalf("public rate stream: %+v", out)
	}
	admin, _ := auth.GenerateToken(1, "admin_user", 0, key)
	node := seedKernelNode(t, db, "test-rates")
	if err := db.CreateNodeKernel(&model.NodeKernel{NodeID: node, KernelType: "xray"}); err != nil {
		t.Fatal(err)
	}
	var index int64
	stop := startMockNodeResponder(t, srv.URL, "test-rates-secret", nil, func(cmd string) any {
		if cmd != "SubscriptionCounters" {
			return nil
		}
		index++
		down := 1024 * min(index-1, 3)
		return map[string]any{"epoch": "one", "sampledAt": index * 1000, "users": []map[string]any{{"id": 1, "dir": "up", "bytes": 0}, {"id": 1, "dir": "down", "bytes": down}}}
	})
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), 9*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+path, nil)
	req.Header.Set("Authorization", admin)
	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	dec := json.NewDecoder(resp.Body)
	seen := false
	for {
		var rates map[string]struct {
			Down float64 `json:"down"`
		}
		if err := dec.Decode(&rates); err != nil {
			t.Fatal(err)
		}
		r, ok := rates["1"]
		if !ok {
			continue
		}
		if !seen && r.Down == 1024 {
			if time.Since(start) > 5*time.Second {
				t.Fatal("startup exceeded 5 seconds")
			}
			t.Logf("first download rate after %v", time.Since(start))
			seen = true
		}
		if seen && r.Down == 0 {
			t.Logf("idle rate after %v from start", time.Since(start))
			break
		}
	}
	if count := mustQueryInt(t, db, `SELECT COUNT(*) FROM subscription_counter`); count != 0 {
		t.Fatalf("live channel touched accounting: %d", count)
	}
}
