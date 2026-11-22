package socket

import (
	"context"
	"github.com/go-gost/x/kernel"
	"testing"
	"testing/synctest"
	"time"
)

// A stalled network consumer skips over two frames; the sampler still
// measures each second, and renewal keeps the full three-second window.
func TestRateSamplerLeaseAndSlowWriter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		leases := make(chan rateLease, 1)
		frames := make(chan rateFrame, 1)
		calls := 0
		go runRateSampler(ctx, leases, frames, func() (kernel.SubscriptionSnapshot, error) {
			calls++
			n := int64(calls)
			return kernel.SubscriptionSnapshot{Epoch: "e", Users: []kernel.UserCounter{{ID: 7, Dir: "up", Bytes: 0}, {ID: 7, Dir: "down", Bytes: n * n * 100}}}, nil
		})
		advance := func(d time.Duration) { time.Sleep(d); synctest.Wait() }
		advance(time.Second)
		if calls != 0 {
			t.Fatal("read without viewer")
		}
		leases <- rateLease{"a", time.Now()}
		synctest.Wait()
		if len((<-frames).Users) != 0 {
			t.Fatal("first sample not a baseline")
		}
		advance(time.Second)
		if f := <-frames; f.Users[7].Instant.Down != 300 || f.Users[7].Average != nil {
			t.Fatal(f)
		}
		advance(time.Second)
		leases <- rateLease{"b", time.Now()}
		synctest.Wait()
		advance(2 * time.Second)
		f := <-frames
		r := f.Users[7]
		if f.Token != "b" || f.Elapsed != 2000 || calls != 5 || r.Instant.Down != 900 || r.Average == nil || r.Average.Down != 700 {
			t.Fatal(calls, f, r)
		}
		advance(16 * time.Second)
		stopped := calls
		advance(10 * time.Second)
		if calls != stopped {
			t.Fatal("expired lease read counters")
		}
		leases <- rateLease{"c", time.Now()}
		synctest.Wait()
		if f := <-frames; f.Token != "c" || len(f.Users) != 0 {
			t.Fatal("resubscribe did not rewarm", f)
		}
	})
}
