package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// HotTopics mirrors the doc the aggregation app upserts at hot_topics/_id="current".
// The two struct-tag sets do the translation: `bson` decodes the MongoDB document, `json`
// shapes what we send to the browser (camelCase, ISO timestamps).
type HotTopics struct {
	WindowStart time.Time `bson:"windowStart" json:"windowStart"`
	WindowEnd   time.Time `bson:"windowEnd"   json:"windowEnd"`
	GeneratedAt time.Time `bson:"generatedAt" json:"generatedAt"`
	TopPages    []TopPage `bson:"topPages"    json:"topPages"`
}

type TopPage struct {
	Rank            int     `bson:"rank"            json:"rank"`
	Title           string  `bson:"title"           json:"title"`
	PageURL         string  `bson:"pageUrl"         json:"pageUrl"`
	EditCount       int64   `bson:"editCount"       json:"editCount"`
	DistinctEditors int64   `bson:"distinctEditors" json:"distinctEditors"`
	BytesChanged    int64   `bson:"bytesChanged"    json:"bytesChanged"`
	HotnessScore    float64 `bson:"hotnessScore"    json:"hotnessScore"`
}

// runWatcher keeps a change stream open for the life of ctx, reconnecting on error. Change
// streams can be interrupted (failover, network blip); the loop makes the watcher resilient
// the way a real service needs to be.
func runWatcher(ctx context.Context, coll *mongo.Collection, hub *Hub, ready *atomic.Bool) {
	for ctx.Err() == nil {
		if err := watchOnce(ctx, coll, hub, ready); err != nil && ctx.Err() == nil {
			ready.Store(false)
			log.Printf("change stream error: %v; reconnecting in 5s", err)
			select {
			case <-time.After(5 * time.Second):
			case <-ctx.Done():
			}
		}
	}
}

// watchOnce seeds the current state, then blocks streaming changes until the stream ends.
func watchOnce(ctx context.Context, coll *mongo.Collection, hub *Hub, ready *atomic.Bool) error {
	seedCurrent(ctx, coll, hub)

	// FullDocument=updateLookup ensures we always receive the full doc, not just the delta.
	// (A replaceOne already carries the full doc, but this also covers plain updates.)
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	stream, err := coll.Watch(ctx, mongo.Pipeline{}, opts)
	if err != nil {
		return err
	}
	defer stream.Close(context.Background())

	ready.Store(true)
	log.Printf("watching change stream on %s.%s", coll.Database().Name(), coll.Name())

	for stream.Next(ctx) {
		var ev struct {
			FullDocument bson.Raw `bson:"fullDocument"`
		}
		if err := stream.Decode(&ev); err != nil {
			log.Printf("decode change event: %v", err)
			continue
		}
		if len(ev.FullDocument) == 0 {
			continue // e.g. a delete — nothing to render
		}
		broadcastDoc(hub, ev.FullDocument)
	}
	return stream.Err()
}

// seedCurrent reads the existing hot_topics/current once so freshly-connected clients (and a
// freshly-started server) have data without waiting for the next window to be written.
func seedCurrent(ctx context.Context, coll *mongo.Collection, hub *Hub) {
	res := coll.FindOne(ctx, bson.D{{Key: "_id", Value: "current"}})
	raw, err := res.Raw()
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			log.Print("no hot_topics/current yet — waiting for the aggregator to write one")
		} else {
			log.Printf("seed read failed: %v", err)
		}
		return
	}
	broadcastDoc(hub, raw)
}

// broadcastDoc converts a raw BSON hot_topics doc to browser JSON and fans it out.
func broadcastDoc(hub *Hub, raw bson.Raw) {
	var ht HotTopics
	if err := bson.Unmarshal(raw, &ht); err != nil {
		log.Printf("unmarshal hot_topics: %v", err)
		return
	}
	payload, err := json.Marshal(ht)
	if err != nil {
		log.Printf("marshal payload: %v", err)
		return
	}
	hub.broadcast <- payload
}
