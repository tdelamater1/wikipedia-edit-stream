package main

import (
	"context"
	_ "embed"
	"log"
	"net/http"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// The frontend is compiled into the binary, so the whole dashboard ships as one static file.
//
//go:embed web/index.html
var indexHTML []byte

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("[dashboard] ")

	cfg := loadConfig()

	// ctx is cancelled on SIGINT/SIGTERM (Ctrl-C locally, pod termination in k8s) so the
	// Hub, watcher, and HTTP server all shut down cleanly.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	client := connectMongo(ctx, cfg.MongoURI)
	defer client.Disconnect(context.Background())
	coll := client.Database(cfg.MongoDB).Collection(cfg.HotTopicsColl)

	hub := newHub()
	go hub.run(ctx)

	// ready flips true once the change stream is open; readiness probe reflects it.
	var ready atomic.Bool
	go runWatcher(ctx, coll, hub, &ready)

	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: routes(hub, &ready)}

	// Run the server until ctx is cancelled, then shut it down gracefully.
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutCtx)
	}()

	log.Printf("listening on %s", cfg.HTTPAddr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("http server: %v", err)
	}
	log.Print("shut down")
}

func connectMongo(ctx context.Context, uri string) *mongo.Client {
	connectCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(connectCtx, options.Client().ApplyURI(uri))
	if err != nil {
		log.Fatalf("mongo connect: %v", err)
	}
	if err := client.Ping(connectCtx, nil); err != nil {
		log.Fatalf("mongo ping: %v", err)
	}
	log.Print("connected to MongoDB")
	return client
}

func routes(hub *Hub, ready *atomic.Bool) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWS(hub, w, r)
	})

	// Liveness: the process is up.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Readiness: only serve traffic once the change stream is established.
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if ready.Load() {
			w.WriteHeader(http.StatusOK)
			return
		}
		http.Error(w, "change stream not ready", http.StatusServiceUnavailable)
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	})

	return mux
}
