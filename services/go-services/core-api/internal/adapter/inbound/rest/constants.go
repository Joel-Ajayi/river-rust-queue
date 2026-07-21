package rest

import "time"

type contextKey string

const (
	// ContextPrincipal is the context key for the authenticated principal.
	// Uses unexported `contextKey` type to prevent collision with other packages.
	ContextPrincipal contextKey = "principal"

	// Server Timeouts
	ServerReadTimeout  = 10 * time.Second // 10s
	ServerWriteTimeout = 10 * time.Second // 10s
	ServerIdleTimeout  = 15 * time.Second // 15s

	// HTTP Headers
	HeaderAuthorization  = "Authorization"
	HeaderValBearer      = "Bearer"
	ApplicationJSON      = "application/json"
	ContentType          = "Content-Type"
	HeaderCacheControl   = "Cache-Control"
	HeaderValNoCache     = "no-cache"
	HeaderValEventStream = "text/event-stream"
	HeaderConnection     = "Connection"
	HeaderValKeepAlive   = "keep-alive"
	HeaderIdempotencyKey = "Idempotency-Key"
	HeaderMerchantID     = "X-Merchant-Id"
	HeaderMerchantTier   = "X-Merchant-Tier"

	// Query Params & Path Vars
	ParamWalletID = "wallet_id"
	ParamJobID    = "id"

	// Transfer Field Params
	ParamFromWallet = "from_wallet"
	ParamToWallet   = "to_wallet"
	ParamAmount     = "amount"
	ParamCurrency   = "currency"
)
