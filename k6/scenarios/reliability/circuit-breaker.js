import { check } from "k6";
import http from "k6/http";
import { Trend, Rate } from "k6/metrics";
import { transferRequest, depositRequest } from "../../lib/helpers.js";
import { prepareTestData } from "../../lib/setup-helper.js";

// Scenario settings and probability boundaries.
const DEPOSIT_PROBABILITY = 0.30;
const MIN_AMOUNT = 100;
const MAX_AMOUNT = 1000000;
const HEALTH_CHECK_INTERVAL = 25;
const STATUS_OK = 200;

// Custom telemetry metrics.
const transferDuration = new Trend("transfer_duration_ms");
const depositDuration = new Trend("deposit_duration_ms");
const requestSuccessRate = new Rate("request_success_rate");

// Circuit breaker testing options for fallback verification.
export const options = {
	scenarios: {
		circuit_breaker_test: {
			executor: "constant-vus",
			vus: 200,
			duration: "10m",
		},
	},
	thresholds: {
		http_req_duration: ["p(95)<5000", "p(99)<10000"],
		http_req_failed: ["rate<0.10"],
	},
};

// Seeding phase executed once before load test starts.
export function setup() {
	return prepareTestData();
}

// Simulated workload traffic routing logic.
export default function (data) {
	const merchantIdx = Math.floor((__VU % 100000) / 1000);
	const jwt = data.jwts[merchantIdx];
	const from = data.wallets[merchantIdx][__VU % 1000];
	const to = data.wallets[(merchantIdx + 1) % 100][(__VU + 1) % 1000];
	const amount = Math.floor(Math.random() * (MAX_AMOUNT - MIN_AMOUNT)) + MIN_AMOUNT;

	const isDeposit = Math.random() < DEPOSIT_PROBABILITY;

	if (isDeposit) {
		const res = http.request(depositRequest(from, amount, jwt));

		depositDuration.add(res.timings.duration);
		requestSuccessRate.add(res.status >= 200 && res.status < 600);

		check(res, {
			"deposit responded": (r) => r.status >= 200 && r.status < 600,
		});
	} else {
		const res = http.request(transferRequest(from, to, amount, jwt));

		transferDuration.add(res.timings.duration);
		requestSuccessRate.add(res.status >= 200 && res.status < 600);

		check(res, {
			"transfer responded": (r) => r.status >= 200 && r.status < 600,
		});
	}

	// Dynamic API health probe evaluation.
	if (__ITER % HEALTH_CHECK_INTERVAL === 0) {
		const healthRes = http.get(`${__ENV.BASE_URL || "http://localhost:8080"}/health`);
		check(healthRes, { "health ok": (r) => r.status === STATUS_OK });
	}
}
