package rest

type contextKey string

const (
	// ContextPrincipal is the context key for the authenticated principal.
	// Uses unexported `contextKey` type to prevent collision with other packages.
	ContextPrincipal contextKey = "principal"

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
	HeaderIdempotencyKey = "X-Idempotency-Key"
	HeaderMerchantID     = "X-Merchant-Id"
	HeaderMerchantTier   = "X-Merchant-Tier"

	// HeaderEdgeOrigin is stamped by the Envoy gateway's admin route so the
	// admin API can reject direct access that bypasses the gateway.
	HeaderEdgeOrigin      = "X-RRQ-Edge"
	HeaderEdgeOriginValue = "envoy"

	// Query Params & Path Vars
	ParamWalletID = "wallet_id"
	ParamJobID    = "id"

	// Transfer Field Params
	ParamFromWallet = "from_wallet"
	ParamToWallet   = "to_wallet"
	ParamAmount     = "amount"
	ParamCurrency   = "currency"

	// Admin DLQ Params & Path Vars
	ParamShardID = "shard_id"
	ParamSource  = "source"
	ParamStatus  = "status"
	ParamLimit   = "limit"
	ParamOffset  = "offset"
	ParamDLQID   = "dlq_id"
)
