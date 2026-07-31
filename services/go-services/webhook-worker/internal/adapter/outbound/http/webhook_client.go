package http

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/core/domain"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

type WebhookClient struct {
	client *http.Client
}

func NewWebhookClient() *WebhookClient {
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          domain.HTTPMaxIdleConns,
		MaxIdleConnsPerHost:   domain.HTTPMaxIdleConnsPerHost,
		IdleConnTimeout:       domain.HTTPIdleConnTimeout,
		ResponseHeaderTimeout: 5 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	return &WebhookClient{
		client: &http.Client{
			Timeout:   domain.HTTPClientTimeout,
			Transport: otelhttp.NewTransport(transport),
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) >= 3 {
					return fmt.Errorf("stopped after 3 redirects")
				}
				return nil
			},
		},
	}
}

// Post delivers the signed payload to the merchant.
func (c *WebhookClient) Post(ctx context.Context, merchantID string, url string, payload []byte, signature, timestamp, eventID string, attempt int) (int, error) {
	req, err := http.NewRequestWithContext(ctx, domain.HTTPMethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return 0, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set(domain.HTTPHeaderContentType, domain.HTTPContentTypeJSON)
	req.Header.Set(domain.HTTPHeaderSignature, domain.HTTPSignaturePrefix+signature)
	req.Header.Set(domain.HTTPHeaderTimestamp, timestamp)
	req.Header.Set(domain.HTTPHeaderEventID, eventID)
	req.Header.Set(domain.HTTPHeaderDeliveryAttempt, strconv.Itoa(attempt))
	req.Header.Set(domain.HTTPHeaderUserAgent, domain.HTTPUserAgentValue)

	resp, err := c.client.Do(req)
	if err != nil {
		return 0, err // timeout or network error
	}
	defer resp.Body.Close()

	// Optionally read the body to ensure the connection can be reused
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		baseErr := fmt.Errorf("%w: non-2xx response: %d", domain.ErrDeliveryFailed, resp.StatusCode)
		return resp.StatusCode, platform.NewHttpError(resp.StatusCode, baseErr)
	}

	return resp.StatusCode, nil
}
