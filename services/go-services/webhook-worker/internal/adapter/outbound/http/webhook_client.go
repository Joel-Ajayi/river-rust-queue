package http

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/core/domain"
)

type WebhookClient struct {
	client *http.Client
}

func NewWebhookClient() *WebhookClient {
	return &WebhookClient{
		client: &http.Client{
			Timeout: domain.HTTPClientTimeout,
		},
	}
}

// Post delivers the signed payload to the merchant.
func (c *WebhookClient) Post(ctx context.Context, url string, payload []byte, signature, eventID string, attempt int) (int, error) {
	req, err := http.NewRequestWithContext(ctx, domain.HTTPMethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return 0, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set(domain.HTTPHeaderContentType, domain.HTTPContentTypeJSON)
	req.Header.Set(domain.HTTPHeaderSignature, domain.HTTPSignaturePrefix+signature)
	req.Header.Set(domain.HTTPHeaderEventID, eventID)
	req.Header.Set(domain.HTTPHeaderDeliveryAttempt, strconv.Itoa(attempt))
	req.Header.Set(domain.HTTPHeaderUserAgent, domain.HTTPUserAgentValue)

	if tp := platform.ExtractTraceparent(ctx); tp != "" {
		req.Header.Set(platform.TraceparentHeader, tp)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return 0, err // timeout or network error
	}
	defer resp.Body.Close()

	// Optionally read the body to ensure the connection can be reused
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return resp.StatusCode, fmt.Errorf("%w: non-2xx response: %d", domain.ErrDeliveryFailed, resp.StatusCode)
	}

	return resp.StatusCode, nil
}
