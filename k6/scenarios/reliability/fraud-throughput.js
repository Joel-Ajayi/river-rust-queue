import { check } from "k6";
import http from "k6/http";
import { Trend, Rate } from "k6/metrics";
import { transferRequest } from "../../lib/helpers.js";
import { prepareTestData } from "../../lib/setup-helper.js";

// Scenario settings and status codes.
const MIN_AMOUNT = 10000;
const MAX_AMOUNT = 10000000;
const STATUS_ACCEPTED = 202;
const STATUS_OK = 200;
const STATUS_RATE_LIMITED = 429;
const STATUS_FORBIDDEN = 403;

// Custom telemetry metrics.
const transferDuration = new Trend("transfer_duration_ms");
const rateLimitedCount = new Rate("rate_limited_requests");

// Fraud limit testing options to check limit triggers.
export const options = {
	scenarios: {
		fraud_velocity: {
			executor: "constant-vus",
			vus: 200,
			duration: "10m",
		},
	},
	thresholds: {
		http_req_duration: ["p(95)<500", "avg<200"],
		http_req_failed: ["rate<0.05"],
		http_reqs: ["rate>500"],
	},
};

// Seeding phase executed once before load test starts.
export function setup() {
	return prepareTestData();
}

// Simulated workload traffic generating fraud velocity limits.
export default function (data) {
	const from = data.wallets[0][__ITER % 1000];
	const to = data.wallets[1][(__VU * 1000 + __ITER) % 1000];
	const amount = Math.floor(Math.random() * (MAX_AMOUNT - MIN_AMOUNT)) + MIN_AMOUNT;

	const res = http.request(transferRequest(from, to, amount, data.jwts[0]));

	transferDuration.add(res.timings.duration);
	rateLimitedCount.add(res.status === STATUS_RATE_LIMITED);

	const validResponse =
		res.status === STATUS_ACCEPTED ||
		res.status === STATUS_OK ||
		res.status === STATUS_RATE_LIMITED ||
		res.status === STATUS_FORBIDDEN;

	check(res, {
		"accepted or rate-limited": () => validResponse,
	});
}
