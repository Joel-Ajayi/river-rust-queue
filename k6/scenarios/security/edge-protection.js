import { check, sleep } from "k6";
import http from "k6/http";
import { Rate } from "k6/metrics";
import { BASE_URL } from "../../lib/config.js";
import { prepareTestData } from "../../lib/setup-helper.js";

// Scenario settings and response indicators.
const STATUS_UNAUTHORIZED = 401;
const STATUS_PAYLOAD_TOO_LARGE = 413;
const STATUS_BAD_REQUEST = 400;
const REPEAT_COUNT_BYTES = 2 * 1024 * 1024; // 2MB
const SLEEP_INTERVAL_SEC = 1;

// Custom telemetry metrics.
const blockedRequests = new Rate("blocked_malicious_requests");

// Security options to flood gateway with invalid requests.
export const options = {
	scenarios: {
		edge_security_load: {
			executor: "constant-vus",
			vus: 100,
			duration: "5m",
		},
	},
	thresholds: {
		http_req_failed: ["rate>0.90"],
	},
};

// Seeding phase executed once before security test starts.
export function setup() {
	return prepareTestData();
}

// Simulated malicious traffic generation logic.
export default function (data) {
	const jwt = data.jwts[0];

	// 1. Request with missing Authorization Header
	const resNoAuth = http.post(`${BASE_URL}/v1/transfers`, JSON.stringify({}), {
		headers: {
			"Content-Type": "application/json",
			"Idempotency-Key": "sec-test-1",
		},
	});
	const noAuthBlocked = resNoAuth.status === STATUS_UNAUTHORIZED;
	blockedRequests.add(noAuthBlocked);
	check(resNoAuth, {
		"missing auth returns 401": () => noAuthBlocked,
	});

	// 2. Request with invalid JWT signature/key
	const resBadAuth = http.post(`${BASE_URL}/v1/transfers`, JSON.stringify({}), {
		headers: {
			"Content-Type": "application/json",
			"Authorization": "Bearer bad-jwt-token-signature-field",
			"Idempotency-Key": "sec-test-2",
		},
	});
	const badAuthBlocked = resBadAuth.status === STATUS_UNAUTHORIZED;
	blockedRequests.add(badAuthBlocked);
	check(resBadAuth, {
		"invalid JWT returns 401": () => badAuthBlocked,
	});

	// 3. Request with excessively large body (exceeding MaxRequestBytes payload limit)
	const giantBody = "A".repeat(REPEAT_COUNT_BYTES);
	const resGiant = http.post(`${BASE_URL}/v1/transfers`, giantBody, {
		headers: {
			"Content-Type": "application/json",
			"Authorization": `Bearer ${jwt}`,
			"Idempotency-Key": "sec-test-3",
		},
	});
	const giantBlocked = resGiant.status === STATUS_PAYLOAD_TOO_LARGE || resGiant.status === STATUS_BAD_REQUEST;
	blockedRequests.add(giantBlocked);
	check(resGiant, {
		"oversized body blocked": () => giantBlocked,
	});

	sleep(SLEEP_INTERVAL_SEC);
}
