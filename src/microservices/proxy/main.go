package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
)

var (
	monolithProxy      *httputil.ReverseProxy
	moviesServiceProxy *httputil.ReverseProxy
	eventsServiceProxy *httputil.ReverseProxy
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}

	monolithURL, _ := url.Parse(getEnv("MONOLITH_URL", "http://localhost:8080"))
	moviesURL, _ := url.Parse(getEnv("MOVIES_SERVICE_URL", "http://localhost:8081"))
	eventsURL, _ := url.Parse(getEnv("EVENTS_SERVICE_URL", "http://localhost:8082"))

	monolithProxy = httputil.NewSingleHostReverseProxy(monolithURL)
	moviesServiceProxy = httputil.NewSingleHostReverseProxy(moviesURL)
	eventsServiceProxy = httputil.NewSingleHostReverseProxy(eventsURL)

	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/api/events/", eventsHandler)
	http.HandleFunc("/api/movies", moviesHandler)
	http.HandleFunc("/api/movies/", moviesHandler)
	http.HandleFunc("/", monolithHandler)

	log.Printf("Proxy service starting on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func moviesHandler(w http.ResponseWriter, r *http.Request) {
	gradual := os.Getenv("GRADUAL_MIGRATION") == "true"

	if gradual {
		percent, err := strconv.Atoi(os.Getenv("MOVIES_MIGRATION_PERCENT"))
		if err != nil {
			percent = 0
		}
		if rand.Intn(100) < percent {
			log.Println("[proxy] movies → movies-service")
			moviesServiceProxy.ServeHTTP(w, r)
			return
		}
		log.Println("[proxy] movies → monolith")
		monolithProxy.ServeHTTP(w, r)
		return
	}

	log.Println("[proxy] movies → movies-service (100%)")
	moviesServiceProxy.ServeHTTP(w, r)
}

func eventsHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("[proxy] events →", r.URL.Path)
	eventsServiceProxy.ServeHTTP(w, r)
}

func monolithHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("[proxy] → monolith:", r.URL.Path)
	monolithProxy.ServeHTTP(w, r)
}
