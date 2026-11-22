package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"

	"go-backend/internal/http/response"
	"go-backend/internal/kernel"
	"go-backend/internal/ws"

	"github.com/google/uuid"
)

// Membership is per share subscription, not all the panel's kernel nodes.
// Read topology once per stream, not in the one-second sample path.
type rateMembership map[int64][]int64

func (h *Handler) rateMembership() (rateMembership, error) {
	subs, err := h.repo.ListSubscriptions()
	if err != nil {
		return nil, err
	}
	c, err := h.loadSubContext()
	if err != nil {
		return nil, err
	}
	ks, err := h.repo.ListNodeKernels(0)
	if err != nil {
		return nil, err
	}
	nodes, err := h.repo.ListSubscriptionMeterNodes()
	if err != nil {
		return nil, err
	}
	local := map[int64]bool{}
	ssMeter := map[int64]bool{}
	for _, n := range nodes {
		local[n.ID] = !nftCannotMark(n.NftError)
		ssMeter[n.ID] = strings.TrimSpace(n.NftError) == ""
	}
	kernels := map[int64]int64{}
	for _, k := range ks {
		if k.Enabled == 1 && local[k.NodeID] {
			kernels[k.ID] = k.NodeID
		}
	}
	inbound := map[int64]int64{}
	for _, row := range c.rows {
		if row.SourceID != 0 || row.NodeID <= 0 || row.Enabled != 1 || kernels[row.KernelID] != row.NodeID {
			continue
		}
		in, e := inboundFromModel(row)
		if e == nil && in.SupportsUsers() && (!in.Settings.SSSubscriptionUsers || ssMeter[row.NodeID]) {
			inbound[row.ID] = row.NodeID
		}
	}
	groups := map[int64][]int64{}
	for _, g := range c.groups {
		seen := map[int64]bool{}
		for _, i := range c.groupInboundIDs(g) {
			if n := inbound[i]; n != 0 && !seen[n] {
				groups[g.ID] = append(groups[g.ID], n)
				seen[n] = true
			}
		}
	}
	m := rateMembership{}
	now := time.Now()
	for i := range subs {
		s := &subs[i]
		if !isShare(s) || !subActive(s, now) || kernel.UserMark(s.ID) == 0 {
			continue
		}
		seen := map[int64]bool{}
		m[s.ID] = nil
		for _, g := range c.subGroups(s) {
			for _, n := range groups[g.ID] {
				if !seen[n] {
					m[s.ID] = append(m[s.ID], n)
					seen[n] = true
				}
			}
		}
	}
	return m, nil
}

type liveRateNode struct {
	viewers         int
	cancel          context.CancelFunc
	renew           chan struct{}
	token           string
	issued, updated time.Time
	elapsed         int64
	rates           map[int64]ws.NodeUserRate
}
type subscriptionRateHub struct {
	mu        sync.Mutex
	nodes     map[int64]*liveRateNode
	listeners map[chan struct{}]bool
}

func signalRate(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}
func (hub *subscriptionRateHub) receive(id int64, f *ws.NodeRateFrame) {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	n := hub.nodes[id]
	if n == nil {
		return
	}
	if f == nil {
		n.token = ""
		n.rates = nil
		signalRate(n.renew)
	} else {
		// Panel send time + agent monotonic elapsed is a conservative estimate
		// of sample time; delayed frames cannot appear fresh after a backlog.
		if n.token != f.Token || f.Elapsed <= n.elapsed {
			return
		}
		sample := n.issued.Add(time.Duration(f.Elapsed) * time.Millisecond)
		age := time.Since(sample)
		if age < 0 || age >= 3*time.Second {
			return
		}
		n.updated = sample
		n.elapsed = f.Elapsed
		n.rates = f.Users
	}
	for ch := range hub.listeners {
		signalRate(ch)
	}
}
func (hub *subscriptionRateHub) watch(m rateMembership, lease func(int64, string)) (<-chan struct{}, func()) {
	ids := map[int64]bool{}
	for _, ns := range m {
		for _, id := range ns {
			ids[id] = true
		}
	}
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if hub.nodes == nil {
		hub.nodes = map[int64]*liveRateNode{}
		hub.listeners = map[chan struct{}]bool{}
	}
	ch := make(chan struct{}, 1)
	hub.listeners[ch] = true
	for id := range ids {
		n := hub.nodes[id]
		if n == nil {
			ctx, cancel := context.WithCancel(context.Background())
			n = &liveRateNode{cancel: cancel, renew: make(chan struct{}, 1)}
			hub.nodes[id] = n
			go hub.run(ctx, id, n, lease)
		}
		n.viewers++
	}
	return ch, func() {
		hub.mu.Lock()
		defer hub.mu.Unlock()
		delete(hub.listeners, ch)
		for id := range ids {
			n := hub.nodes[id]
			n.viewers--
			if n.viewers == 0 {
				n.cancel()
				delete(hub.nodes, id)
			}
		}
	}
}
func (hub *subscriptionRateHub) run(ctx context.Context, id int64, n *liveRateNode, lease func(int64, string)) {
	t := time.NewTicker(8 * time.Second)
	defer t.Stop()
	for ctx.Err() == nil {
		token := uuid.NewString()
		hub.mu.Lock()
		n.token = token
		n.issued = time.Now()
		n.elapsed = -1
		hub.mu.Unlock()
		lease(id, token)
		select {
		case <-ctx.Done():
			return
		case <-n.renew:
		case <-t.C:
		}
	}
}

type coveredRate struct {
	Instant      *ws.RatePair `json:"instant"`
	Average      *ws.RatePair `json:"average"`
	Expected     int          `json:"expected"`
	Valid        int          `json:"valid"`
	AverageValid int          `json:"averageValid"`
}

func addRate(to **ws.RatePair, p *ws.RatePair) {
	if *to == nil {
		*to = &ws.RatePair{}
	}
	(*to).Down += p.Down
	(*to).Up += p.Up
}
func (hub *subscriptionRateHub) snapshot(now time.Time, m rateMembership) map[int64]coveredRate {
	// Frame maps are immutable after delivery. Do not hold the ingestion
	// lock while aggregating a large viewer-specific membership.
	hub.mu.Lock()
	fresh := make(map[int64]map[int64]ws.NodeUserRate, len(hub.nodes))
	for id, n := range hub.nodes {
		if now.Sub(n.updated) < 3*time.Second {
			fresh[id] = n.rates
		}
	}
	hub.mu.Unlock()
	sum := make(map[int64]coveredRate, len(m))
	for id, nodes := range m {
		r := coveredRate{Expected: len(nodes)}
		for _, node := range nodes {
			s := fresh[node][id]
			if s.Instant == nil {
				continue
			}
			addRate(&r.Instant, s.Instant)
			r.Valid++
			if s.Average != nil {
				addRate(&r.Average, s.Average)
				r.AverageValid++
			}
		}
		sum[id] = r
	}
	return sum
}

// Existing administrator-only route; v1 clients get only valid instant pairs.
func (h *Handler) subscriptionRateStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	m, err := h.rateMembership()
	if err != nil {
		response.WriteJSON(w, response.Err(-2, err.Error()))
		return
	}
	events, release := h.subRates.watch(m, func(id int64, token string) {
		_, _ = h.wsServer.SendCommand(id, "SubscriptionRateLease", token, 2*time.Second)
	})
	defer release()
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Accel-Buffering", "no")
	enc := json.NewEncoder(w)
	rc := http.NewResponseController(w)
	heartbeat := time.NewTicker(time.Second)
	defer heartbeat.Stop()
	end := time.NewTimer(90 * time.Second)
	defer end.Stop()
	for {
		snap := h.subRates.snapshot(time.Now(), m)
		var wire any = snap
		if r.URL.Query().Get("v") != "2" {
			legacy := map[int64]*ws.RatePair{}
			for id, row := range snap {
				if row.Instant != nil {
					legacy[id] = row.Instant
				}
			}
			wire = legacy
		}
		if enc.Encode(wire) != nil || rc.Flush() != nil {
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-end.C:
			return
		case <-heartbeat.C:
		case <-events:
			// Coalesce concurrent node arrivals without waiting another one-second tick.
			select {
			case <-r.Context().Done():
				return
			case <-time.After(50 * time.Millisecond):
			}
			select {
			case <-events:
			default:
			}
		}
	}
}
