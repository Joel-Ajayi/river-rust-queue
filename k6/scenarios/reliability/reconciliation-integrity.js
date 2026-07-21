import { check, sleep } from "k6";
import http from "k6/http";
import { Trend, Rate } from "k6/metrics";
import { transferRequest, depositRequest, jobStatusRequest, walletBalanceRequest } from "../../lib/helpers.js";
import { prepareTestData } from "../../lib/setup-helper.js";

// DEPOSIT_PROBABILITY controls the ratio of deposits vs transfers.
// 30% of requests will perform wallet balance funding.
const DEPOSIT_PROBABILITY = 0.30;

// MIN_AMOUNT and MAX_AMOUNT define transfer limits.
// Smallest unit of currency (e.g. kobo/cents).
const MIN_AMOUNT = 10;
const MAX_AMOUNT = 100000;

// Expected response status codes.
// 202 is returned when transfer is successfully queued.
const STATUS_ACCEPTED = 202;
const STATUS_OK = 200;

// Status polling boundaries.
const POLLING_ATTEMPTS = 10;
const POLLING_SLEEP_SEC = 0.5;

// Balance check periodic interval.
const BALANCE_CHECK_INTERVAL = 10;

// transferDuration tracks latency for all transfers.
const transferDuration = new Trend("transfer_duration_ms");

// depositDuration tracks latency for all deposits.
const depositDuration = new Trend("deposit_duration_ms");

// requestSuccessRate monitors overall API availability.
const requestSuccessRate = new Rate("request_success_rate");

// options defines the k6 load profile settings.
// Reconciliation stress runs 500 VUs to check integrity under load.
export const options = {
	scenarios: {
		reconciliation_stress: {
			executor: "constant-vus",
			vus: 500,
			duration: "30m",
		},
	},
	thresholds: {
		http_req_duration: ["p(95)<1000", "p(99)<2000", "avg<300"],
		http_req_failed: ["rate<0.002"],
	},
};

// setup runs once before load test starts.
// Registers 100 merchants and 100,000 wallets.
export function setup() {
	return prepareTestData();
}

// default function is run by VUs in a loop.
// Simulates live user transactions and polls jobs.
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
		const depositRes = http.request(depositRequest(from, amount, jwt));
		depositDuration.add(depositRes.timings.duration);
		requestSuccessRate.add(depositRes.status === STATUS_ACCEPTED || depositRes.status === STATUS_OK);

		if (depositRes.status === STATUS_ACCEPTED) {
			let jobId = "";
			try {
				const body = depositRes.body ? JSON.parse(depositRes.body) : {};
				jobId = body.job_id || "";
			} catch (e) {
				// Ignored for failed HTTP statuses
			}

			if (jobId) {
				// Poll the job status endpoint until completion or timeout.
				for (let attempt = 0; attempt < POLLING_ATTEMPTS; attempt++) {
					const statusRes = http.request(jobStatusRequest(jobId, jwt));
					if (statusRes.status === STATUS_OK) {
						let jobStatus = "";
						try {
							const jobBody = statusRes.body ? JSON.parse(statusRes.body) : {};
							jobStatus = jobBody.status || "";
						} catch (e) {
							// Ignored for failed HTTP statuses
						}
						if (jobStatus === "completed" || jobStatus === "failed") {
							break;
						}
					}
					sleep(POLLING_SLEEP_SEC);
				}
			}
		}
	} else {
		// Fire transfer request to move money between accounts.
		const transferRes = http.request(transferRequest(from, to, amount, jwt));
		transferDuration.add(transferRes.timings.duration);
		requestSuccessRate.add(transferRes.status === STATUS_ACCEPTED || transferRes.status === STATUS_OK);

		if (transferRes.status === STATUS_ACCEPTED) {
			let jobId = "";
			try {
				const body = transferRes.body ? JSON.parse(transferRes.body) : {};
				jobId = body.job_id || "";
			} catch (e) {
				// Ignored for failed HTTP statuses
			}

			if (jobId) {
				// Poll the job status endpoint until completion or timeout.
				for (let attempt = 0; attempt < POLLING_ATTEMPTS; attempt++) {
					const statusRes = http.request(jobStatusRequest(jobId, jwt));
					if (statusRes.status === STATUS_OK) {
						let jobStatus = "";
						try {
							const jobBody = statusRes.body ? JSON.parse(statusRes.body) : {};
							jobStatus = jobBody.status || "";
						} catch (e) {
							// Ignored for failed HTTP statuses
						}
						if (jobStatus === "completed" || jobStatus === "failed") {
							break;
						}
					}
					sleep(POLLING_SLEEP_SEC);
				}
			}
		}
	}

	// Verify balance values periodically.
	if (__ITER % BALANCE_CHECK_INTERVAL === 0) {
		const balanceRes = http.request(walletBalanceRequest(from, jwt));
		let balance = -1;
		try {
			const balanceBody = balanceRes.body ? JSON.parse(balanceRes.body) : {};
			balance = typeof balanceBody.balance === "number" ? balanceBody.balance : -1;
		} catch (e) {
			// Ignored for failed HTTP statuses
		}

		check(balanceRes, {
			"balance verifiable": (r) => r.status === STATUS_OK,
			"balance non-negative": () => balance >= 0,
		});
	}
}
