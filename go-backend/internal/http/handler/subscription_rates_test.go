package handler

import (
	"sync/atomic"
	"testing"
	"time"
)

func testRateSample(ms, up, down int64) rateSample {
	return rateSample{"e", ms, []agentUserCounter{{7, "up", up}, {7, "down", down}}}
}
func TestSubscriptionLiveRates(t *testing.T) {
	base := testRateSample(1000, 400, 2000)
	downloading := testRateSample(2250, 525, 4560)
	if got := subscriptionRates(base, downloading)[7]; got != (subRate{100, 2048}) {
		t.Fatal(got)
	}
	idle := testRateSample(3250, 525, 4560)
	if r, ok := subscriptionRates(downloading, idle)[7]; !ok || r != (subRate{}) {
		t.Fatal("valid idle", r, ok)
	}
	changed := downloading
	changed.Epoch = "new"
	missing := downloading
	missing.Users = missing.Users[:1]
	for _, s := range []rateSample{base, changed, missing, testRateSample(5000, 1000, 9999), testRateSample(2000, 100, 3000)} {
		if _, ok := subscriptionRates(base, s)[7]; ok {
			t.Fatal("invalid sample yielded speed", s)
		}
	}
	if len(subscriptionRates(rateSample{}, base)) != 0 {
		t.Fatal("first sample is only a baseline")
	}
}
func TestSubscriptionLiveRateSumAndExpiry(t *testing.T) {
	now := time.Now()
	hub := subscriptionRateHub{nodes: map[int64]*liveRateNode{1: {updated: now, rates: map[int64]subRate{7: {10, 20}}}, 2: {updated: now, rates: map[int64]subRate{7: {1, 2}}}, 3: {updated: now.Add(-4 * time.Second), rates: map[int64]subRate{8: {1, 2}, 7: {100, 200}}}}}
	got := hub.snapshot(now)
	if len(got) != 1 || got[7] != (subRate{11, 22}) {
		t.Fatal(got)
	}
}
func TestSubscriptionLiveWatch(t *testing.T) {
	var hub subscriptionRateHub
	var calls atomic.Int32
	sample := func(id int64) (rateSample, error) {
		n := int64(calls.Add(1))
		return testRateSample(1000*n, 100*n, 1000*n), nil
	}
	a := hub.watch([]int64{3}, sample)
	b := hub.watch([]int64{3}, sample)
	a()
	defer b()
	until := time.Now().Add(4 * time.Second)
	for time.Now().Before(until) {
		if r, ok := hub.snapshot(time.Now())[7]; ok {
			if r != (subRate{100, 1000}) || calls.Load() != 2 {
				t.Fatal(r, calls.Load())
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no live rate")
}
