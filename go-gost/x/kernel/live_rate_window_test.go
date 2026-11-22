package kernel

import (
	"math"
	"testing"
	"time"
)

func userSample(epoch string, up, down int64) SubscriptionSnapshot {
	return SubscriptionSnapshot{Epoch: epoch, Users: []UserCounter{{7, "up", up}, {7, "down", down}}}
}
func TestLiveRateWindow(t *testing.T) {
	var w RateWindow
	t0 := time.Now()
	at := func(ms int, s SubscriptionSnapshot) map[int64]UserSpeed {
		return w.Observe(t0.Add(time.Duration(ms)*time.Millisecond), s)
	}
	if len(at(0, userSample("a", 1000, 1000))) != 0 {
		t.Fatal("baseline")
	}
	r := at(1250, userSample("a", 1125, 3560))[7]
	if r.Instant != (RatePair{100, 2048}) || r.Average != nil {
		t.Fatal(r)
	}
	r, ok := at(2200, userSample("a", 1125, 3560))[7]
	if !ok || r.Instant != (RatePair{}) || r.Average != nil {
		t.Fatal(r)
	}
	r = at(3000, userSample("a", 1300, 7000))[7]
	if r.Average == nil || *r.Average != (RatePair{100, 2000}) {
		t.Fatal(r)
	}
	if len(at(4000, userSample("a", 1301, 20))) != 0 {
		t.Fatal("counter rollback")
	}
	r = at(5000, userSample("a", 1301, 20))[7]
	if r.Average != nil {
		t.Fatal("average spanned rollback")
	}
	for _, tc := range []struct {
		ms int
		s  SubscriptionSnapshot
	}{{6000, userSample("b", 1500, 200)}, {9000, userSample("b", 1550, 220)}, {10000, SubscriptionSnapshot{}}} {
		if len(at(tc.ms, tc.s)) != 0 {
			t.Fatal(tc)
		}
	}
	missing := userSample("b", 1600, 300)
	missing.Users = missing.Users[:1]
	at(11000, missing)
	if len(at(12000, userSample("b", 1700, 400))) != 0 {
		t.Fatal("missing direction must rewarm")
	}
}
func TestLiveRateWindowMinuteConservation(t *testing.T) {
	var w RateWindow
	t0 := time.Now()
	at := t0
	var total int64 = 1 << 54
	base := total
	var integrated float64
	w.Observe(at, userSample("x", 0, total))
	for i := 1; i <= 60; i++ {
		dt := time.Duration(850+300*(i%2)) * time.Millisecond
		at = at.Add(dt)
		total += int64(i*i + 17)
		r, ok := w.Observe(at, userSample("x", 0, total))[7]
		if !ok {
			t.Fatalf("missing interval %d", i)
		}
		integrated += r.Instant.Down * dt.Seconds()
	}
	if math.Abs(integrated-float64(total-base)) > 1e-6 {
		t.Fatal(integrated, total-base)
	}
}
