package socket

import (
	"context"
	"time"

	"github.com/go-gost/x/kernel"
)

// A lease belongs to one authenticated connection. Renewal changes the clock
// anchor, not the counter history. No viewer -> no renewals and no nft reads.
type rateLease struct {
	token    string
	received time.Time
}
type rateFrame struct {
	Token   string                     `json:"token"`
	Elapsed int64                      `json:"elapsedMs"`
	Users   map[int64]kernel.UserSpeed `json:"users"`
}

func runRateSampler(ctx context.Context, leases <-chan rateLease, frames chan rateFrame, read func() (kernel.SubscriptionSnapshot, error)) {
	var window kernel.RateWindow
	var lease rateLease
	var tick *time.Ticker
	var ticks <-chan time.Time
	defer func() {
		if tick != nil {
			tick.Stop()
		}
	}()
	sample := func() {
		start := time.Now()
		s, err := read()
		if err != nil {
			s = kernel.SubscriptionSnapshot{}
		}
		// nft returns no monotonic timestamp; place the observation midway
		// through its bounded read rather than attributing all read latency to it.
		at := start.Add(time.Since(start) / 2)
		f := rateFrame{lease.token, at.Sub(lease.received).Milliseconds(), window.Observe(at, s)}
		// Only the latest frame matters; never wait for the writer.
		select {
		case <-frames:
		default:
		}
		frames <- f
	}
	for {
		select {
		case <-ctx.Done():
			return
		case l := <-leases:
			restart := tick == nil || l.received.Sub(lease.received) >= 15*time.Second
			lease = l
			if restart {
				window = kernel.RateWindow{}
				if tick != nil {
					tick.Stop()
				}
				tick = time.NewTicker(time.Second)
				ticks = tick.C
				sample()
			}
		case <-ticks:
			if time.Since(lease.received) >= 15*time.Second {
				tick.Stop()
				tick = nil
				ticks = nil
				window = kernel.RateWindow{}
				continue
			}
			sample()
		}
	}
}
