import { check } from "k6";
import http from "k6/http";
import { Trend, Rate } from "k6/metrics";
import { transferRequest, depositRequest } from "../../lib/helpers.js";
import { prepareTestData } from "../../lib/setup-helper.js";

// Scenario settings and probability boundaries.
const DEPOSIT_PROBABILITY = 0.30;
const MIN_AMOUNT = 100;
const MAX_AMOUNT = 500000;
const STATUS_ACCEPTED = 202;
const STATUS_OK = 200;

// Custom telemetry metrics.
const transferDuration = new Trend("transfer_duration_ms");
const depositDuration = new Trend("deposit_duration_ms");
const requestSuccessRate = new Rate("request_success_rate");

// Scalability options to ramp up to peak throughput levels.
export const options = {
	scenarios: {
		ramp_to_peak: {
			executor: "ramping-vus",
			startVUs: 0,
			stages: [
				{ duration: "5m", target: 200 },
				{ duration: "5m", target: 500 },
				{ duration: "5m", target: 1000 },
				{ duration: "5m", target: 2000 },
				{ duration: "5m", target: 2000 },
				{ duration: "5m", target: 0 },
			],
			gracefulRampDown: "30s",
		},
	},
	thresholds: {
		http_req_duration: ["p(95)<300", "p(99)<600", "avg<150"],
		http_req_failed: ["rate<0.001"],
	},
};

// Seeding phase executed once before load test starts.
export function setup() {
	return prepareTestData();
}

// Simulated workload traffic and dynamic routing logic.
export default function (data) {
	const i = __VU * 100000 + __ITER;
	const merchantIdx = Math.floor((i % 100000) / 1000);
	const jwt = data.jwts[merchantIdx];
	const from = data.wallets[merchantIdx][i % 1000];
	const to = data.wallets[(merchantIdx + 1) % 100][(i + 1) % 1000];
	const amount = Math.floor(Math.random() * (MAX_AMOUNT - MIN_AMOUNT)) + MIN_AMOUNT;

	const isDeposit = Math.random() < DEPOSIT_PROBABILITY;

	if (isDeposit) {
		const res = http.request(depositRequest(from, amount, jwt));

		depositDuration.add(res.timings.duration);
		const success = res.status === STATUS_ACCEPTED || res.status === STATUS_OK;
		requestSuccessRate.add(success);

		let hasJobId = false;
		try {
			const body = res.body ? JSON.parse(res.body) : {};
			hasJobId = typeof body.job_id === "string" && body.job_id.length > 0;
		} catch (e) {
			// Ignore JSON parsing errors for failure cases
		}

		check(res, {
			"deposit accepted": () => success,
			"deposit has job_id": () => hasJobId,
		});
	} else {
		const res = http.request(transferRequest(from, to, amount, jwt));

		transferDuration.add(res.timings.duration);
		const success = res.status === STATUS_ACCEPTED || res.status === STATUS_OK;
		requestSuccessRate.add(success);

		let hasJobId = false;
		try {
			const body = res.body ? JSON.parse(res.body) : {};
			hasJobId = typeof body.job_id === "string" && body.job_id.length > 0;
		} catch (e) {
			// Ignore JSON parsing errors for failure cases
		}

		check(res, {
			"transfer accepted": () => success,
			"transfer has job_id": () => hasJobId,
		});
	}
}
