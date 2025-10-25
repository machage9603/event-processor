package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub"
)

type Event struct {
	UserID    string `json:"user_id"`
	EventType string `json:"event_type"`
	Timestamp string `json:"timestamp"`
	Page      string `json:"page"`
}

type ProcessedEvent struct {
	UserID      string    `bigquery:"user_id"`
	EventType   string    `bigquery:"event_type"`
	Timestamp   time.Time `bigquery:"timestamp"`
	Page        string    `bigquery:"page"`
	ProcessedAt time.Time `bigquery:"processed_at"`
	// Enrich: Add mock geo
	Country string `bigquery:"country"`
}

func ProcessEvent(ctx context.Context, m *pubsub.Message) error {
	var e Event
	if err := json.Unmarshal(m.Data, &e); err != nil {
		log.Printf("Parse error: %v", err)
		return err
	}

	pe := ProcessedEvent{
		UserID:      e.UserID,
		EventType:   e.EventType,
		Timestamp:   time.Time{}, // Parse e.Timestamp
		Page:        e.Page,
		ProcessedAt: time.Now(),
		Country:     "US", // Mock enrichment
	}

	// Insert to BigQuery (details in Step 4)
	client, err := bigquery.NewClient(ctx, "your-project-id")
	if err != nil {
		return err
	}
	defer client.Close()

	table := client.Dataset("analytics").Table("user_events")
	inserter := table.Inserter()
	if err := inserter.Put(ctx, pe); err != nil {
		return err
	}

	m.Ack()
	return nil
}

// Entry point for Cloud Functions
func HelloPubSub(ctx context.Context, name string) error {
	// For gen2 functions, adapt to event data
	return nil // Placeholder
}
