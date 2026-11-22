package contract_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gorilla/websocket"
	"go-backend/internal/auth"
	"go-backend/internal/security"
	"go-backend/internal/store/model"
	"go-backend/internal/ws"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// Real SQLite, encrypted node messages, JWT and streamed HTTP. Only the
// node's measured speeds are synthetic; this does not benchmark Linux nft.
func TestLiveSubscriptionRateStream(t *testing.T) {
	const key = "test-rate-key"
	const path = "/api/v1/subscription/rates?v=2"
	router, db := setupContractRouter(t, key)
	srv := httptest.NewServer(router)
	defer srv.Close()
	if out := kernelContractPost(t, router, "", path, "{}"); out.Code != 401 {
		t.Fatal("public rate stream", out)
	}
	admin, _ := auth.GenerateToken(1, "admin_user", 0, key)
	node := seedKernelNode(t, db, "rates")
	offline := seedKernelNode(t, db, "offline")
	for _, id := range []int64{node, offline} {
		k := model.NodeKernel{NodeID: id, KernelType: "xray"}
		if err := db.CreateNodeKernel(&k); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 2; i++ {
			if err := db.CreateNodeInbound(&model.NodeInbound{KernelID: k.ID, NodeID: id, Protocol: "vless", Port: 8100 + i}); err != nil {
				t.Fatal(err)
			}
		}
	}
	group := model.NodeGroup{Name: "all", SourceAll: 1}
	if err := db.DB().Create(&group).Error; err != nil {
		t.Fatal(err)
	}
	sub := model.Subscription{Name: "test", Token: "test", GroupIDs: fmt.Sprint(group.ID), Kind: model.SubscriptionShare, Enabled: 1}
	if err := db.DB().Create(&sub).Error; err != nil {
		t.Fatal(err)
	}
	const secret = "rates-secret"
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http")+"/system-info?type=1&secret="+secret, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	crypto, _ := security.NewAESCrypto(secret)
	conn.SetReadDeadline(time.Now().Add(6 * time.Second))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+path, nil)
	req.Header.Set("Authorization", admin)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	dec := json.NewDecoder(resp.Body)
	type row struct {
		Instant, Average              *ws.RatePair
		Expected, Valid, AverageValid int
	}
	next := func() row {
		t.Helper()
		var r map[int64]row
		if err := dec.Decode(&r); err != nil {
			t.Fatal(err)
		}
		v := r[sub.ID]
		if v.Expected != 2 {
			t.Fatalf("wrong per-subscription coverage: %+v", r)
		}
		return v
	}
	if v := next(); v.Valid != 0 || v.Instant != nil {
		t.Fatal("cold start", v)
	}
	var token string
	for token == "" {
		var wrap struct{ Data string }
		if err := conn.ReadJSON(&wrap); err != nil {
			t.Fatal(err)
		}
		raw, err := crypto.Decrypt(wrap.Data)
		if err != nil {
			continue
		}
		var cmd struct {
			Type, RequestID string
			Data            json.RawMessage
		}
		if json.Unmarshal(raw, &cmd) != nil || cmd.Type != "SubscriptionRateLease" {
			continue
		}
		if err := json.Unmarshal(cmd.Data, &token); err != nil {
			t.Fatal(err)
		}
		if err := conn.WriteJSON(map[string]any{"type": "SubscriptionRateLeaseResponse", "requestId": cmd.RequestID, "success": true}); err != nil {
			t.Fatal(err)
		}
	}
	anchor := time.Now()
	push := func(down float64, avg *ws.RatePair) {
		t.Helper()
		frame := ws.NodeRateFrame{Token: token, Elapsed: time.Since(anchor).Milliseconds(), Users: map[int64]ws.NodeUserRate{sub.ID: {Instant: &ws.RatePair{Down: down}, Average: avg}}}
		b, _ := json.Marshal(map[string]any{"type": "SubscriptionRateFrame", "data": frame})
		enc, _ := crypto.Encrypt(b)
		if err := conn.WriteJSON(map[string]any{"encrypted": true, "data": enc}); err != nil {
			t.Fatal(err)
		}
	}
	push(1024, nil)
	for {
		v := next()
		if v.Valid == 1 {
			if v.Instant.Down != 1024 || v.Average != nil {
				t.Fatal(v)
			}
			break
		}
	}
	t.Logf("first injected frame to stream: %v", time.Since(anchor))
	if time.Since(anchor) > 5*time.Second {
		t.Fatal("delivery stalled")
	}
	push(0, &ws.RatePair{Down: 340})
	for {
		v := next()
		if v.AverageValid == 1 {
			if v.Valid != 1 || v.Instant.Down != 0 || v.Average.Down != 340 {
				t.Fatal(v)
			}
			break
		}
	}
	conn.Close()
	for {
		v := next()
		if v.Valid == 0 {
			if v.Instant != nil || v.Average != nil {
				t.Fatal("offline is not zero", v)
			}
			break
		}
	}
	if count := mustQueryInt(t, db, `SELECT COUNT(*) FROM subscription_counter`); count != 0 {
		t.Fatalf("live channel touched accounting: %d", count)
	}
}
