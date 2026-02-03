package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

// WebhookEvent represents the payload sent to webhook URLs.
type WebhookEvent struct {
	Event     string `json:"event"`
	Repo      string `json:"repo"`
	Branch    string `json:"branch"`
	CommitID  string `json:"commit_id"`
	Timestamp string `json:"timestamp"`
}

// WebhookConfig holds the list of configured webhook URLs.
type WebhookConfig struct {
	URLs []string
}

// WebhookNotifier sends HTTP POST notifications to configured webhook URLs.
type WebhookNotifier struct {
	config *WebhookConfig
	client *http.Client
	logger *slog.Logger
}

// NewWebhookNotifier creates a webhook notifier. Returns nil if no URLs are configured.
func NewWebhookNotifier(cfg *WebhookConfig, logger *slog.Logger) *WebhookNotifier {
	if cfg == nil || len(cfg.URLs) == 0 {
		return nil
	}
	return &WebhookNotifier{
		config: cfg,
		client: &http.Client{Timeout: 10 * time.Second},
		logger: logger,
	}
}

// NotifyPush sends a push event to all configured webhook URLs.
// Runs asynchronously — does not block the caller.
func (wn *WebhookNotifier) NotifyPush(repo, branch, commitID string) {
	if wn == nil {
		return
	}

	event := &WebhookEvent{
		Event:     "push",
		Repo:      repo,
		Branch:    branch,
		CommitID:  commitID,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}

	go wn.send(event)
}

// send delivers the webhook event to all configured URLs.
func (wn *WebhookNotifier) send(event *WebhookEvent) {
	data, err := json.Marshal(event)
	if err != nil {
		wn.logger.Error("webhook: marshal event", "error", err)
		return
	}

	for _, url := range wn.config.URLs {
		if err := wn.post(url, data); err != nil {
			wn.logger.Warn("webhook: delivery failed", "url", url, "error", err)
		} else {
			wn.logger.Debug("webhook: delivered", "url", url, "event", event.Event)
		}
	}
}

// post sends a single webhook POST with retry (up to 2 retries).
func (wn *WebhookNotifier) post(url string, data []byte) error {
	const maxRetries = 2

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		req, err := http.NewRequest("POST", url, bytes.NewReader(data))
		if err != nil {
			return fmt.Errorf("create request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", "wvc-server/1.0")

		resp, err := wn.client.Do(req)
		if err != nil {
			lastErr = err
			time.Sleep(time.Duration(attempt+1) * time.Second)
			continue
		}
		resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}

		lastErr = fmt.Errorf("HTTP %d", resp.StatusCode)
		if resp.StatusCode < 500 {
			return lastErr // don't retry 4xx
		}
		time.Sleep(time.Duration(attempt+1) * time.Second)
	}

	return lastErr
}
