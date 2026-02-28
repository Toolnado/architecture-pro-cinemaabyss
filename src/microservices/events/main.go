package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

const (
	topicMovies   = "movie-events"
	topicUsers    = "user-events"
	topicPayments = "payment-events"
)

var kafkaBrokers []string

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}

	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}
	kafkaBrokers = strings.Split(brokers, ",")

	go consumeTopic(topicMovies)
	go consumeTopic(topicUsers)
	go consumeTopic(topicPayments)

	http.HandleFunc("/api/events/health", healthHandler)
	http.HandleFunc("/api/events/movie", movieEventHandler)
	http.HandleFunc("/api/events/user", userEventHandler)
	http.HandleFunc("/api/events/payment", paymentEventHandler)

	log.Printf("Events service starting on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func movieEventHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var payload map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	if err := produce(topicMovies, payload); err != nil {
		log.Printf("[events] failed to produce movie event: %v", err)
		http.Error(w, "failed to publish event", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"status": "success"})
}

func userEventHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var payload map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	if err := produce(topicUsers, payload); err != nil {
		log.Printf("[events] failed to produce user event: %v", err)
		http.Error(w, "failed to publish event", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"status": "success"})
}

func paymentEventHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var payload map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	if err := produce(topicPayments, payload); err != nil {
		log.Printf("[events] failed to produce payment event: %v", err)
		http.Error(w, "failed to publish event", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"status": "success"})
}

func produce(topic string, payload map[string]interface{}) error {
	w := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBrokers...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer w.Close()

	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := w.WriteMessages(ctx, kafka.Message{Value: data}); err != nil {
		return err
	}

	log.Printf("[events] produced to %s: %s", topic, string(data))
	return nil
}

func consumeTopic(topic string) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        kafkaBrokers,
		Topic:          topic,
		GroupID:        "events-service",
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
	})
	defer r.Close()

	log.Printf("[events] consumer started for topic: %s", topic)

	for {
		msg, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Printf("[events] consumer error on %s: %v", topic, err)
			time.Sleep(2 * time.Second)
			continue
		}
		log.Printf("[events] consumed from %s: key=%s value=%s",
			topic, string(msg.Key), string(msg.Value))
	}
}
