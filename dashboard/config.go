package main

import (
	"fmt"
	"net/url"
	"os"
)

// Config holds everything the dashboard needs, all sourced from the environment so the same
// binary runs unchanged on a laptop (port-forwarded Mongo) and in-cluster (a ConfigMap +
// Secret). Defaults target the laptop case.
type Config struct {
	HTTPAddr      string // address the HTTP/WebSocket server listens on
	MongoURI      string // assembled connection string (password URL-escaped)
	MongoDB       string // database holding the hot_topics collection
	HotTopicsColl string // collection the change stream watches
}

// loadConfig reads env vars and builds a Mongo URI. We assemble the URI ourselves and
// url.QueryEscape the password for the same reason the Java apps used discrete properties:
// the base64-derived password contains '/' and '=' which would corrupt a raw URI's userinfo.
func loadConfig() Config {
	host := env("MONGO_HOST", "localhost")
	port := env("MONGO_PORT", "27017")
	user := env("MONGO_USERNAME", "wikipedia")
	pass := env("MONGO_PASSWORD", "")
	authDB := env("MONGO_AUTH_DB", "admin")

	// directConnection bypasses replica-set topology discovery — needed when reaching a
	// single RS member through a port-forward (laptop). Set false in-cluster so the driver
	// discovers the replica set (change streams require a replica set either way).
	uri := fmt.Sprintf("mongodb://%s:%s@%s:%s/?authSource=%s",
		url.QueryEscape(user), url.QueryEscape(pass), host, port, url.QueryEscape(authDB))
	if env("MONGO_DIRECT_CONNECTION", "true") == "true" {
		uri += "&directConnection=true"
	}

	return Config{
		HTTPAddr:      env("HTTP_ADDR", ":8080"),
		MongoURI:      uri,
		MongoDB:       env("MONGO_DB", "wikipedia"),
		HotTopicsColl: env("HOT_TOPICS_COLLECTION", "hot_topics"),
	}
}

func env(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}
