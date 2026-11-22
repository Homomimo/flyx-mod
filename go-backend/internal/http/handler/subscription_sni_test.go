package handler

import (
	"testing"
	"time"
)

func TestSNIBounds(t *testing.T) {
	tz := time.FixedZone("", 345*60)
	now := time.Date(2024, 2, 29, 23, 5, 0, 0, tz)
	for _, r := range []string{"hour", "day", "month"} {
		q := subTrafficRequest{Range: r, TZOffset: 345}
		b := subTrafficBuckets(r, now)
		for i, start := range b {
			end, ok := sniBounds(q, start.UnixMilli(), now)
			if !ok || end <= start.UnixMilli() || (i+1 < len(b) && end != b[i+1].UnixMilli()) {
				t.Fatal(r, start, end)
			}
		}
		if _, ok := sniBounds(q, b[0].UnixMilli()-1, now); ok {
			t.Fatal("accepted invalid time")
		}
	}
}
