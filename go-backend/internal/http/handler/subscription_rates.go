package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"go-backend/internal/http/response"
)

type rateSample struct {
	Epoch     string             `json:"epoch"`
	SampledAt int64              `json:"sampledAt"`
	Users     []agentUserCounter `json:"users"`
}

type subRate struct {
	Up   float64 `json:"up"`
	Down float64 `json:"down"`
}
type counterKey struct {
	id  int64
	dir string
}

// Rates are observational. An epoch change or a gap starts a new baseline;
// never interpret a total since boot as a one-second delta.
func subscriptionRates(prev, cur rateSample) map[int64]subRate {
	dt := cur.SampledAt - prev.SampledAt
	if prev.SampledAt <= 0 || prev.Epoch == "" || prev.Epoch != cur.Epoch || dt <= 0 || dt > 3500 {
		return nil
	}
	old := make(map[counterKey]int64, len(prev.Users))
	for _, c := range prev.Users {
		old[counterKey{c.ID, c.Dir}] = c.Bytes
	}
	mask := map[int64]int{}
	rates := map[int64]subRate{}
	for _, c := range cur.Users {
		before, ok := old[counterKey{c.ID, c.Dir}]
		if !ok || c.ID <= 0 || before < 0 || c.Bytes < before {
			continue
		}
		speed := float64(c.Bytes-before) * 1000 / float64(dt)
		r := rates[c.ID]
		switch c.Dir {
		case "up":
			r.Up = speed
			mask[c.ID] |= 1
		case "down":
			r.Down = speed
			mask[c.ID] |= 2
		default:
			continue
		}
		rates[c.ID] = r
	}
	for id := range rates {
		if mask[id] != 3 {
			delete(rates, id)
		}
	}
	return rates
}

type liveRateNode struct {
	viewers int
	cancel  context.CancelFunc
	updated time.Time
	rates   map[int64]subRate
}
type subscriptionRateHub struct {
	mu    sync.Mutex
	nodes map[int64]*liveRateNode
}

// Concurrent viewers share the same read. Each worker waits for its previous
// reply before sending another command; slow nodes do not block healthy ones.
func (hub *subscriptionRateHub) watch(ids []int64, sample func(int64) (rateSample, error)) func() {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if hub.nodes == nil {
		hub.nodes = map[int64]*liveRateNode{}
	}
	for _, id := range ids {
		n := hub.nodes[id]
		if n == nil {
			ctx, cancel := context.WithCancel(context.Background())
			n = &liveRateNode{cancel: cancel}
			hub.nodes[id] = n
			go hub.run(ctx, id, n, sample)
		}
		n.viewers++
	}
	return func() {
		hub.mu.Lock()
		defer hub.mu.Unlock()
		for _, id := range ids {
			n := hub.nodes[id]
			n.viewers--
			if n.viewers == 0 {
				n.cancel()
				delete(hub.nodes, id)
			}
		}
	}
}
func (hub *subscriptionRateHub) run(ctx context.Context, id int64, n *liveRateNode, sample func(int64) (rateSample, error)) {
	var prev rateSample
	for ctx.Err() == nil {
		start := time.Now()
		cur, err := sample(id)
		period := time.Second
		if err != nil {
			cur = rateSample{}
			period = 5 * time.Second
		}
		rates := subscriptionRates(prev, cur)
		prev = cur
		hub.mu.Lock()
		n.rates = rates
		n.updated = time.Now()
		hub.mu.Unlock()
		timer := time.NewTimer(max(50*time.Millisecond, period-time.Since(start)))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
func (hub *subscriptionRateHub) snapshot(now time.Time) map[int64]subRate {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	sum := map[int64]subRate{}
	for _, n := range hub.nodes {
		if now.Sub(n.updated) > 3*time.Second {
			continue
		}
		for id, r := range n.rates {
			total := sum[id]
			total.Up += r.Up
			total.Down += r.Down
			sum[id] = total
		}
	}
	return sum
}

// JWT middleware restricts this stream to the same administrator as /subscription/list.
func (h *Handler) subscriptionRateStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	ids, err := h.repo.ListKernelNodeIDs()
	if err != nil {
		response.WriteJSON(w, response.Err(-2, err.Error()))
		return
	}
	release := h.subRates.watch(ids, func(id int64) (rateSample, error) {
		var s rateSample
		res, err := h.wsServer.SendCommand(id, "SubscriptionCounters", nil, 1500*time.Millisecond)
		if err == nil {
			err = remarshal(res.Data, &s)
		}
		return s, err
	})
	defer release()
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Accel-Buffering", "no")
	enc := json.NewEncoder(w)
	rc := http.NewResponseController(w)
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	// Reauthenticate and refresh node membership on reconnect, below WriteTimeout.
	end := time.NewTimer(90 * time.Second)
	defer end.Stop()
	for {
		if err := enc.Encode(h.subRates.snapshot(time.Now())); err != nil {
			return
		}
		if err := rc.Flush(); err != nil {
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-end.C:
			return
		case <-tick.C:
		}
	}
}
