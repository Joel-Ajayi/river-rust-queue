// BASE_URL points to the Envoy Gateway HTTPS endpoint (port 443).
// Dev:  https://api.127.0.0.1.nip.io  (nip.io → localhost)
// Prod: https://api.rrq.yotstack.tech
// Override with BASE_URL env var for production or custom clusters.
export const BASE_URL = __ENV.BASE_URL || "https://api.127.0.0.1.nip.io";

export const MERCHANT_COUNT = 100;

export const SCENARIO_DURATION = __ENV.DURATION || "30m";
