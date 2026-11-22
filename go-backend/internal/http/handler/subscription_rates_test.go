package handler

import (
	"go-backend/internal/ws"
	"testing"
	"time"
)

func TestSubscriptionLiveRateSumAndExpiry(t *testing.T) {
	now := time.Now()
	p := &ws.RatePair{Up: 1, Down: 2}
	hub := subscriptionRateHub{nodes: map[int64]*liveRateNode{1: {updated: now, rates: map[int64]ws.NodeUserRate{7: {Instant: p, Average: p}, 8: {Instant: &ws.RatePair{}}}}, 2: {updated: now, rates: map[int64]ws.NodeUserRate{7: {Instant: p}, 8: {Instant: p}}}, 3: {updated: now.Add(-3 * time.Second), rates: map[int64]ws.NodeUserRate{7: {Instant: p}}}}}
	m := rateMembership{7: {1, 2, 3}, 8: {1}, 9: {3}, 10: {}}
	r := hub.snapshot(now, m)
	if x := r[7]; x.Expected != 3 || x.Valid != 2 || x.AverageValid != 1 || *x.Instant != (ws.RatePair{Up: 2, Down: 4}) || *x.Average != *p {
		t.Fatal(x)
	}
	if x := r[8]; x.Valid != 1 || *x.Instant != (ws.RatePair{}) {
		t.Fatal(x)
	}
	if r[9].Instant != nil || r[9].Expected != 1 || r[10].Expected != 0 {
		t.Fatal(r)
	}
}
func TestSubscriptionLiveWatch(t *testing.T) {
	var hub subscriptionRateHub
	lease := make(chan string, 5)
	send := func(_ int64, s string) { lease <- s }
	m := rateMembership{7: {3}}
	_, a := hub.watch(m, send)
	event, b := hub.watch(m, send)
	a()
	defer b()
	var token string
	select {
	case token = <-lease:
	case <-time.After(time.Second):
		t.Fatal("no lease")
	}
	p := &ws.RatePair{Down: 8}
	frame := ws.NodeRateFrame{Token: token, Users: map[int64]ws.NodeUserRate{7: {Instant: p}}}
	hub.receive(3, &frame)
	select {
	case <-event:
	default:
		t.Fatal("frame did not wake viewer")
	}
	hub.receive(3, &frame)
	frame.Token = "previous"
	hub.receive(3, &frame)
	select {
	case <-event:
		t.Fatal("duplicate/old lease accepted")
	default:
	}
	if r := hub.snapshot(time.Now(), m)[7]; r.Valid != 1 || r.Instant.Down != 8 {
		t.Fatal(r)
	}
	select {
	case <-lease:
		t.Fatal("viewers didn't share lease")
	default:
	}
	hub.receive(3, nil)
	if hub.snapshot(time.Now(), m)[7].Instant != nil {
		t.Fatal("offline node counted")
	}
}
func TestSubscriptionLiveDelayedFrame(t *testing.T) {
	n := &liveRateNode{token: "x", issued: time.Now().Add(-8 * time.Second)}
	hub := subscriptionRateHub{nodes: map[int64]*liveRateNode{1: n}}
	hub.receive(1, &ws.NodeRateFrame{Token: "x", Elapsed: 1500, Users: map[int64]ws.NodeUserRate{}})
	if !n.updated.IsZero() {
		t.Fatal("stale queue accepted")
	}
}
