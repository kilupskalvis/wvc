package server

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
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
	URLs         []string
	Secret       string
	AllowPrivate bool // skip SSRF validation (for tests only)
}

// WebhookNotifier sends HTTP POST notifications to configured webhook URLs.
type WebhookNotifier struct {
	config *WebhookConfig
	client *http.Client
	logger *slog.Logger
	sem    chan struct{}
}

// isPrivateIP returns true if the IP falls within loopback, link-local, or private ranges.
func isPrivateIP(ip net.IP) bool {
	privateRanges := []struct {
		network string
	}{
		{"127.0.0.0/8"},
		{"169.254.0.0/16"},
		{"10.0.0.0/8"},
		{"172.16.0.0/12"},
		{"192.168.0.0/16"},
		{"::1/128"},
		{"fe80::/10"},
		{"fc00::/7"},
	}
	for _, r := range privateRanges {
		_, cidr, err := net.ParseCIDR(r.network)
		if err != nil {
			continue
		}
		if cidr.Contains(ip) {
			return true
		}
	}
	return false
}

// NewWebhookNotifier creates a webhook notifier. Returns nil if no URLs are configured.
// URLs whose hosts resolve to loopback, link-local, or private IP ranges are rejected.
func NewWebhookNotifier(cfg *WebhookConfig, logger *slog.Logger) *WebhookNotifier {
	if cfg == nil || len(cfg.URLs) == 0 {
		return nil
	}

	if cfg.AllowPrivate {
		// Skip SSRF validation (test only).
	} else {
		var safeURLs []string
		for _, rawURL := range cfg.URLs {
			parsed, err := url.Parse(rawURL)
			if err != nil {
				logger.Warn("webhook: rejected invalid URL", "url", rawURL, "error", err)
				continue
			}

			host := parsed.Hostname()
			if host == "" {
				logger.Warn("webhook: rejected URL with empty host", "url", rawURL)
				continue
			}

			ips, err := net.LookupIP(host)
			if err != nil {
				logger.Warn("webhook: rejected URL — DNS lookup failed", "url", rawURL, "error", err)
				continue
			}

			blocked := false
			for _, ip := range ips {
				if isPrivateIP(ip) {
					logger.Warn("webhook: rejected URL — host resolves to private/loopback address", "url", rawURL, "ip", ip.String())
					blocked = true
					break
				}
			}
			if blocked {
				continue
			}

			safeURLs = append(safeURLs, rawURL)
		}

		if len(safeURLs) == 0 {
			return nil
		}

		cfg.URLs = safeURLs
	}
	var client *http.Client
	if cfg.AllowPrivate {
		client = &http.Client{Timeout: 10 * time.Second}
	} else {
		transport := &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				host, port, err := net.SplitHostPort(addr)
				if err != nil {
					return nil, err
				}
				ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
				if err != nil {
					return nil, err
				}
				for _, ip := range ips {
					if isPrivateIP(ip.IP) {
						return nil, fmt.Errorf("webhook blocked: %s resolves to private IP %s", host, ip.IP)
					}
				}
				dialer := &net.Dialer{Timeout: 10 * time.Second}
				return dialer.DialContext(ctx, network, net.JoinHostPort(ips[0].IP.String(), port))
			},
		}
		client = &http.Client{Timeout: 10 * time.Second, Transport: transport}
	}
	return &WebhookNotifier{
		config: cfg,
		client: client,
		logger: logger,
		sem:    make(chan struct{}, 10),
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

	select {
	case wn.sem <- struct{}{}:
		go func() {
			defer func() { <-wn.sem }()
			wn.send(event)
		}()
	default:
		wn.logger.Warn("webhook: goroutine limit reached, skipping notification", "repo", repo, "branch", branch)
	}
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

		if wn.config.Secret != "" {
			mac := hmac.New(sha256.New, []byte(wn.config.Secret))
			mac.Write(data)
			sig := hex.EncodeToString(mac.Sum(nil))
			req.Header.Set("X-WVC-Signature-256", "sha256="+sig)
		}

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
