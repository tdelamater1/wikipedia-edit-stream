package main

import (
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

// Timing for the WebSocket keepalive. A browser tab that's been backgrounded or a dropped
// connection won't always send a TCP FIN, so we ping periodically and expect a pong back; if
// none arrives within pongWait the read deadline fires and we tear the connection down.
const (
	writeWait  = 10 * time.Second
	pongWait   = 60 * time.Second
	pingPeriod = (pongWait * 9) / 10 // must be < pongWait
	sendBuffer = 8
)

// upgrader turns an HTTP request into a WebSocket connection.
var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 4096,
	// Homelab/dev: accept any Origin. In production you'd allowlist your dashboard's origin
	// here to prevent cross-site WebSocket hijacking.
	CheckOrigin: func(r *http.Request) bool { return true },
}

// Client is one connected browser. send is buffered so the Hub never blocks on a single
// slow socket; the two pumps below are the standard gorilla pattern — exactly one goroutine
// writes to the connection (writePump) and exactly one reads (readPump).
type Client struct {
	hub  *Hub
	conn *websocket.Conn
	send chan []byte
}

// serveWS upgrades the request and wires the client into the Hub.
func serveWS(hub *Hub, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("websocket upgrade failed: %v", err)
		return
	}
	client := &Client{hub: hub, conn: conn, send: make(chan []byte, sendBuffer)}
	hub.register <- client

	// One goroutine per direction. readPump owns the unregister-on-exit.
	go client.writePump()
	go client.readPump()
}

// writePump is the ONLY writer to the connection. It drains send and emits periodic pings.
func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case msg, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// Hub closed the channel (we were unregistered) — say goodbye cleanly.
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// readPump is the ONLY reader. The dashboard is push-only, so we don't expect real messages —
// reading exists to process pong/close control frames and to detect a dead connection.
func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()

	c.conn.SetReadLimit(512)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		if _, _, err := c.conn.ReadMessage(); err != nil {
			break // normal close, timeout, or error — all mean "client is gone"
		}
	}
}
