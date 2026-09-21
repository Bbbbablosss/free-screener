package main

import (
	"encoding/json"
	"testing"

	"github.com/gorilla/websocket"
)

func TestResumeReplaysAuthoritativeState(t *testing.T) {
	c := &gwClient{
		send:      make(chan *websocket.PreparedMessage, 16),
		visitorID: "v1_test_resume",
		isPro:     true,
	}
	c.paused.Store(true)
	g := &Gateway{
		clients:  map[*gwClient]struct{}{c: {}},
		densBlob: map[string]json.RawMessage{"density": json.RawMessage(`{"id":"density"}`)},
		arbBlob:  map[string]json.RawMessage{"arb": json.RawMessage(`{"id":"arb"}`)},
		mdBlob:   []byte(`{"type":"market_data","pairs":[]}`),
	}

	g.setVisitorPaused(c.visitorID, false, "resume")

	if c.paused.Load() {
		t.Fatal("client remained paused")
	}
	// delivery_status + initial_state + arb_sync + market_data
	if got := len(c.send); got != 4 {
		t.Fatalf("queued messages = %d, want 4", got)
	}
}
