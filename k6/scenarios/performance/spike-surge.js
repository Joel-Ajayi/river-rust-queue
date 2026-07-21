import { check } from "k6";
import http from "k6/http";
import { Trend, Rate } from "k6/metrics";
import { transferRequest, depositRequest } from "../../lib/helpers.js";
import { prepareTestData } from "../../lib/setup-helper.js";

// DEPOSIT_PROBABILITY controls the ratio of deposits vs transfers.
// 30% of requests will perform wallet balance funding.
const DEPOSIT_PROBABILITY = 0.30;

// MIN_AMOUNT and MAX_AMOUNT define transfer limits.
// Smallest unit of currency (e.g. kobo/cents).
const MIN_AMOUNT = 50;
const MAX_AMOUNT = 500000;

// Expected response status codes.
// 202 is returned when transfer is successfully queued.
const STATUS_ACCEPTED = 202;
const STATUS_OK = 200;

// transferDuration tracks latency for all transfers.
const transferDuration = new Trend("transfer_duration_ms");

// depositDuration tracks latency for all deposits.
const depositDuration = new Trend("deposit_duration_ms");

// requestSuccessRate monitors overall API availability.
const requestSuccessRate = new Rate("request_success_rate");

// options defines the k6 load profile settings.
// Spike testing ramps VUs up and down rapidly.
export const options = {
	scenarios: {
		spike_surge: {
			executor: "ramping-vus",
			startVUs: 0,
			stages: [
				{ duration: "5s", target: 2000 },
				{ duration: "60s", target: 2000 },
				{ duration: "30s", target: 0 },
			],
			gracefulRampDown: "10s",
		},
	},
	thresholds: {
		http_req_duration: ["p(95)<500", "p(99)<1000"],
		http_req_failed: ["rate<0.01"],
		http_reqs: ["rate>2000"],
	},
};

// setup runs once before load test starts.
// Registers 100 merchants and 100,000 wallets.
export function setup() {
	return prepareTestData();
}

// default function is run by VUs in a loop.
// Simulates live user transactions.
export default function (data) {
	// Calculate unique index based on VU and Iteration.
	const i = __VU * 100000 + __ITER;

	// Select a merchant index and retrieve JWT.
	const merchantIdx = Math.floor((i % 100000) / 1000);
	const jwt = data.jwts[merchantIdx];

	// Retrieve source and destination wallet IDs.
	const from = data.wallets[merchantIdx][i % 1000];
	const to = data.wallets[(merchantIdx + 1) % 100][(i + 1) % 1000];

	// Generate a random transaction amount.
	const amount = Math.floor(Math.random() * (MAX_AMOUNT - MIN_AMOUNT)) + MIN_AMOUNT;

	// Decide whether to run a deposit or transfer.
	const isDeposit = Math.random() < DEPOSIT_PROBABILITY;

	if (isDeposit) {
		// Fire deposit request to top up wallet balance.
		const res = http.request(depositRequest(from, amount, jwt));

		// Log the request timings.
		depositDuration.add(res.timings.duration);
		const success = res.status === STATUS_ACCEPTED || res.status === STATUS_OK;
		requestSuccessRate.add(success);

		// Safely parse JSON to verify payload structure.
		let hasJobId = false;
		try {
			const body = res.body ? JSON.parse(res.body) : {};
			hasJobId = typeof body.job_id === "string" && body.job_id.length > 0;
		} catch (e) {
			// Ignored for failed HTTP statuses
		}

		// Perform checks on the response.
		check(res, {
			"deposit accepted": () => success,
			"deposit has job_id": () => hasJobId,
		});
	} else {
		// Fire transfer request to move money between accounts.
		const res = http.request(transferRequest(from, to, amount, jwt));

		// Log the request timings.
		transferDuration.add(res.timings.duration);
		const success = res.status === STATUS_ACCEPTED || res.status === STATUS_OK;
		requestSuccessRate.add(success);

		// Safely parse JSON to verify payload structure.
		let hasJobId = false;
		try {
			const body = res.body ? JSON.parse(res.body) : {};
			hasJobId = typeof body.job_id === "string" && body.job_id.length > 0;
		} catch (e) {
			// Ignored for failed HTTP statuses
		}

		// Perform checks on the response.
		check(res, {
			"transfer accepted": () => success,
			"transfer has job_id": () => hasJobId,
		});
	}
}
