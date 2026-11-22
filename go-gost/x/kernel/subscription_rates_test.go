package kernel

import (
	"context"
	"testing"
)

func TestSubscriptionCounterSnapshot(t *testing.T) {
	m := &Manager{nft: &nftCounters{state: nftState{Table: nftTable, Epoch: "same"}}}
	old := nftRunner
	defer func() { nftRunner = old }()
	calls := 0
	nftRunner = func(ctx context.Context, stdin string, args ...string) ([]byte, error) {
		calls++
		if stdin != "" || len(args) != 5 || args[0] != "list" || args[1] != "counters" || args[4] != nftTable {
			t.Fatal(stdin, args)
		}
		return []byte("table inet " + nftTable + " {\n counter uup_9 {\n packets 1 bytes 512\n }\n counter udown_9 {\n packets 1 bytes 1000\n }\n}"), nil
	}
	s, err := m.SubscriptionCounters()
	if err != nil || s.SampledAt <= 0 || s.Epoch != "same" || len(s.Users) != 2 || s.Users[1] != (UserCounter{9, "down", 1000}) {
		t.Fatal(s, err)
	}
	m.nft.mu.Lock()
	defer m.nft.mu.Unlock()
	if _, err = m.SubscriptionCounters(); err == nil || calls != 1 {
		t.Fatal("busy read was not skipped")
	}
}
