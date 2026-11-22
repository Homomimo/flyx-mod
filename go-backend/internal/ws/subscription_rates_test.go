package ws

import (
	"strings"
	"testing"
)

func TestPrivateSubscriptionFrames(t *testing.T) {
	s := NewServer(nil, "")
	ns := &nodeSession{nodeID: 7}
	s.nodes[7] = ns
	count := 0
	s.SetSubscriptionRateHook(func(node int64, f *NodeRateFrame) {
		if node != 7 || f.Users[3].Instant.Down != 0 {
			t.Fatal(node, f)
		}
		count++
	})
	valid := `{"data":{"elapsedMs":0,"token":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa","users":{"3":{"instant":{"up":1,"down":0}}}}}`
	s.privateRateFrame(ns, valid)
	s.privateRateFrame(&nodeSession{nodeID: 7}, valid)
	for _, invalid := range []string{strings.Replace(valid, `"elapsedMs":0,`, "", 1), strings.Replace(valid, `"down":0`, `"down":null`, 1), strings.Replace(valid, `"up":1`, `"up":-1`, 1), strings.Replace(valid, `"3"`, `"65536"`, 1)} {
		s.privateRateFrame(ns, invalid)
	}
	if count != 1 {
		t.Fatal("accepted invalid frame or stale connection", count)
	}
}
