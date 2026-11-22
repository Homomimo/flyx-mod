package kernel

import (
	"context"
	"errors"
	"time"
)

// SubscriptionSnapshot is read-only: never reset counters used by billing.
type SubscriptionSnapshot struct {
	Epoch     string        `json:"epoch"`
	SampledAt int64         `json:"sampledAt"`
	Users     []UserCounter `json:"users"`
}

func (m *Manager) SubscriptionCounters() (SubscriptionSnapshot, error) {
	n := m.nft
	// Never queue live reads behind a table rebuild or another slow nft call.
	if !n.mu.TryLock() {
		return SubscriptionSnapshot{}, errors.New("nft counters busy")
	}
	defer n.mu.Unlock()
	if n.state.Epoch == "" {
		return SubscriptionSnapshot{}, errors.New("nft counters not initialized")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 850*time.Millisecond)
	defer cancel()
	_, users, epoch, err := n.readLocked(ctx)
	return SubscriptionSnapshot{Epoch: epoch, SampledAt: time.Now().UnixMilli(), Users: users}, err
}
