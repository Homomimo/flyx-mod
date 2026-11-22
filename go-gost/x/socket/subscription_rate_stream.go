package socket

import (
	"encoding/json"
	"github.com/gorilla/websocket"
	"time"
)

func (w *WebSocketReporter) sendRateFrame(conn *websocket.Conn, f rateFrame) {
	// A busy writer drops a frame, never the sampler's baseline. Never fall
	// back to plaintext for subscription-level telemetry or use a new session.
	if !w.connMutex.TryLock() {
		return
	}
	defer w.connMutex.Unlock()
	if w.conn != conn || !w.connected || w.aesCrypto == nil {
		return
	}
	raw, err := json.Marshal(CommandResponse{Type: "SubscriptionRateFrame", Success: true, Data: f})
	if err != nil {
		return
	}
	encrypted, err := w.aesCrypto.Encrypt(raw)
	if err != nil {
		return
	}
	conn.SetWriteDeadline(time.Now().Add(time.Second))
	if err := conn.WriteJSON(map[string]any{"encrypted": true, "data": encrypted, "timestamp": time.Now().Unix()}); err != nil {
		w.connected = false
		_ = conn.Close()
	}
}
