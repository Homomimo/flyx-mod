package ws

import (
	"encoding/json"
	"errors"
)

var errInvalidRate = errors.New("invalid subscription rate pair")

// Subscription telemetry is private; it never enters node metric broadcasts.
type RatePair struct {
	Up   float64 `json:"up"`
	Down float64 `json:"down"`
}
type NodeUserRate struct {
	Instant *RatePair `json:"instant"`
	Average *RatePair `json:"average,omitempty"`
}
type NodeRateFrame struct {
	Token   string                 `json:"token"`
	Elapsed int64                  `json:"elapsedMs"`
	Users   map[int64]NodeUserRate `json:"users"`
}

func (p *RatePair) UnmarshalJSON(raw []byte) error {
	var v struct{ Up, Down *float64 }
	if err := json.Unmarshal(raw, &v); err != nil {
		return err
	}
	if v.Up == nil || v.Down == nil || *v.Up < 0 || *v.Down < 0 || *v.Up > 2e19 || *v.Down > 2e19 {
		return errInvalidRate
	}
	p.Up = *v.Up
	p.Down = *v.Down
	return nil
}
func (s *Server) SetSubscriptionRateHook(fn func(int64, *NodeRateFrame)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onRate = fn
}
func (s *Server) privateRateFrame(ns *nodeSession, msg string) {
	if len(msg) > 12<<20 {
		return
	}
	var env struct {
		Data NodeRateFrame `json:"data"`
	}
	env.Data.Elapsed = -1
	if json.Unmarshal([]byte(msg), &env) != nil {
		return
	}
	f := &env.Data
	if len(f.Token) != 36 || f.Elapsed < 0 || f.Elapsed > 16000 || f.Users == nil || len(f.Users) > 65535 {
		return
	}
	for id, r := range f.Users {
		if id <= 0 || id > 65535 || r.Instant == nil {
			return
		}
	}
	// Hold the session check through delivery so a replaced socket cannot
	// repopulate a node after its disconnect. The hook never calls back into WS.
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.nodes[ns.nodeID] == ns && s.onRate != nil {
		s.onRate(ns.nodeID, f)
	}
}
