package kernel

import "time"

// RateWindow lives on the node, not on the network sender. The timestamps
// supplied by the sampler retain Go's monotonic clock component.
type RateWindow struct {
	epoch   string
	history map[int64][]ratePoint
}
type ratePoint struct {
	at       time.Time
	up, down int64
}
type RatePair struct {
	Up   float64 `json:"up"`
	Down float64 `json:"down"`
}
type UserSpeed struct {
	Instant RatePair  `json:"instant"`
	Average *RatePair `json:"average,omitempty"`
}

func (p ratePoint) since(old ratePoint) RatePair {
	seconds := p.at.Sub(old.at).Seconds()
	return RatePair{float64(p.up-old.up) / seconds, float64(p.down-old.down) / seconds}
}
func (w *RateWindow) Observe(at time.Time, s SubscriptionSnapshot) map[int64]UserSpeed {
	if w.epoch != s.Epoch || s.Epoch == "" {
		w.history = nil
	}
	w.epoch = s.Epoch
	type pair struct {
		p    ratePoint
		mask uint8
	}
	pairs := make(map[int64]pair, len(s.Users)/2)
	for _, c := range s.Users {
		if c.ID <= 0 || s.Epoch == "" {
			continue
		}
		v := pairs[c.ID]
		var bit uint8
		switch c.Dir {
		case "up":
			bit = 1
			v.p.up = c.Bytes
		case "down":
			bit = 2
			v.p.down = c.Bytes
		default:
			continue
		}
		if v.mask&bit != 0 || c.Bytes < 0 {
			v.mask |= 4
		}
		v.mask |= bit
		pairs[c.ID] = v
	}
	next := make(map[int64][]ratePoint, len(pairs))
	out := make(map[int64]UserSpeed, len(pairs))
	for id, v := range pairs {
		if v.mask != 3 {
			continue
		}
		p := v.p
		p.at = at
		h := w.history[id]
		if len(h) > 0 {
			last := h[len(h)-1]
			dt := at.Sub(last.at)
			if dt < 500*time.Millisecond || dt > 1600*time.Millisecond || p.up < last.up || p.down < last.down {
				h = nil
			} else {
				r := UserSpeed{Instant: p.since(last)}
				// Nearest continuous sample to three seconds, within half a tick.
				best := 500 * time.Millisecond
				for _, old := range h {
					d := (at.Sub(old.at) - 3*time.Second).Abs()
					if d <= best {
						a := p.since(old)
						r.Average = &a
						best = d
					}
				}
				out[id] = r
			}
		}
		for len(h) > 0 && at.Sub(h[0].at) >= 3500*time.Millisecond {
			h = h[1:]
		}
		next[id] = append(h, p)
	}
	w.history = next
	return out
}
