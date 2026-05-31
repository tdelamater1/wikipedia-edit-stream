package main

import (
	"context"
	"log"
)

// Hub is the broadcaster: a single goroutine owns the set of connected clients, so the
// client map needs no mutex — all mutation happens in run()'s select loop. Everything else
// talks to the Hub by sending on its channels. This "share memory by communicating" shape is
// the idiomatic Go answer to what you'd do with a synchronized collection + listeners in Java.
type Hub struct {
	clients    map[*Client]bool
	broadcast  chan []byte  // new top-10 payloads to fan out
	register   chan *Client // a browser connected
	unregister chan *Client // a browser went away
	latest     []byte       // last payload, replayed to clients that connect mid-window
}

func newHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		broadcast:  make(chan []byte, 8),
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}
}

// run is the Hub's owning goroutine. It exits when ctx is cancelled (graceful shutdown).
func (h *Hub) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case c := <-h.register:
			h.clients[c] = true
			// Give the newcomer the current state immediately rather than making it wait
			// up to a full window for the next change-stream event.
			if h.latest != nil {
				h.trySend(c, h.latest)
			}

		case c := <-h.unregister:
			if _, ok := h.clients[c]; ok {
				delete(h.clients, c)
				close(c.send)
			}

		case msg := <-h.broadcast:
			h.latest = msg
			for c := range h.clients {
				h.trySend(c, msg)
			}
		}
	}
}

// trySend does a non-blocking send. If a client's buffer is full it's a slow/dead consumer,
// so we drop it rather than letting one stuck browser stall the whole broadcast.
func (h *Hub) trySend(c *Client, msg []byte) {
	select {
	case c.send <- msg:
	default:
		log.Printf("dropping slow client %s", c.conn.RemoteAddr())
		delete(h.clients, c)
		close(c.send)
	}
}
