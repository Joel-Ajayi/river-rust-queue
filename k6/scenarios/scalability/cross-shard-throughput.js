import { check, sleep } from "k6";
import http from "k6/http";
import { Trend, Rate } from "k6/metrics";
import { transferRequest, depositRequest, jobStatusRequest } from "../../lib/helpers.js";
import { prepareTestData } from "../../lib/setup-helper.js";

// Scenario settings and probability boundaries.
const DEPOSIT_PROBABILITY = 0.30;
const MIN_AMOUNT = 100;
const MAX_AMOUNT = 500000;
const STATUS_ACCEPTED = 202;
const STATUS_OK = 200;
const POLLING_ATTEMPTS = 20;
const POLLING_SLEEP_SEC = 1;
const MERCHANT_SHARDS_BOUND = 50;

// Custom telemetry metrics.
const transferDuration = new Trend("transfer_duration_ms");
const depositDuration = new Trend("deposit_duration_ms");
const requestSuccessRate = new Rate("request_success_rate");

// Cross shard performance options to evaluate Saga coordinator.
export const options = {
	scenarios: {
		cross_shard_stress: {
			executor: "constant-vus",
			vus: 200,
			duration: "30m",
		},
	},
	thresholds: {
		http_req_duration: ["p(95)<2000", "p(99)<5000", "avg<1000"],
		http_req_failed: ["rate<0.01"],
	},
};

// Seeding phase executed once before load test starts.
export function setup() {
	return prepareTestData();
}

// Simulated workload traffic generating cross-shard transfers.
export default function (data) {
	const i = __VU * 100000 + __ITER;

	const isCrossShard = __ITER % 2 === 0;
	const fromMerchant = __VU % MERCHANT_SHARDS_BOUND;
	const toMerchant = isCrossShard ? MERCHANT_SHARDS_BOUND + (__VU % MERCHANT_SHARDS_BOUND) : fromMerchant;

	const from = data.wallets[fromMerchant][i % 1000];
	const to = data.wallets[toMerchant][(i + 1) % 1000];
	const amount = Math.floor(Math.random() * (MAX_AMOUNT - MIN_AMOUNT)) + MIN_AMOUNT;

	const isDeposit = Math.random() < DEPOSIT_PROBABILITY;

	if (isDeposit) {
		const res = http.request(depositRequest(from, amount, data.jwts[fromMerchant]));
		depositDuration.add(res.timings.duration);
		requestSuccessRate.add(res.status === STATUS_ACCEPTED || res.status === STATUS_OK);

		check(res, {
			"deposit accepted": (r) => r.status === STATUS_ACCEPTED || r.status === STATUS_OK,
			"has job_id": (r) => {
				try {
					const body = r.body ? JSON.parse(r.body) : {};
					return typeof body.job_id === "string" && body.job_id.length > 0;
				} catch (e) {
					return false;
				}
			},
		});

		if (res.status === STATUS_ACCEPTED) {
			let jobId = "";
			try {
				const body = res.body ? JSON.parse(res.body) : {};
				jobId = body.job_id || "";
			} catch (e) {
				// Ignore JSON parsing errors for failure cases
			}

			if (jobId) {
				for (let attempt = 0; attempt < POLLING_ATTEMPTS; attempt++) {
					const statusRes = http.request(jobStatusRequest(jobId, data.jwts[fromMerchant]));
					if (statusRes.status === STATUS_OK) {
						let jobStatus = "";
						try {
							const jobBody = statusRes.body ? JSON.parse(statusRes.body) : {};
							jobStatus = jobBody.status || "";
						} catch (e) {
							// Ignore JSON parsing errors for failure cases
						}
						if (jobStatus === "completed" || jobStatus === "failed") {
							check(jobStatus, {
								"cross-shard deposit job completed": (s) => s === "completed",
							});
							break;
						}
					}
					sleep(POLLING_SLEEP_SEC);
				}
			}
		}
	} else {
		const res = http.request(transferRequest(from, to, amount, data.jwts[fromMerchant]));
		transferDuration.add(res.timings.duration);
		requestSuccessRate.add(res.status === STATUS_ACCEPTED || res.status === STATUS_OK);

		check(res, {
			"transfer accepted": (r) => r.status === STATUS_ACCEPTED || r.status === STATUS_OK,
			"has job_id": (r) => {
				try {
					const body = r.body ? JSON.parse(r.body) : {};
					return typeof body.job_id === "string" && body.job_id.length > 0;
				} catch (e) {
					return false;
				}
			},
		});

		if (res.status === STATUS_ACCEPTED) {
			let jobId = "";
			try {
				const body = res.body ? JSON.parse(res.body) : {};
				jobId = body.job_id || "";
			} catch (e) {
				// Ignore JSON parsing errors for failure cases
			}

			if (jobId) {
				for (let attempt = 0; attempt < POLLING_ATTEMPTS; attempt++) {
					const statusRes = http.request(jobStatusRequest(jobId, data.jwts[fromMerchant]));
					if (statusRes.status === STATUS_OK) {
						let jobStatus = "";
						try {
							const jobBody = statusRes.body ? JSON.parse(statusRes.body) : {};
							jobStatus = jobBody.status || "";
						} catch (e) {
							// Ignore JSON parsing errors for failure cases
						}
						if (jobStatus === "completed" || jobStatus === "failed") {
							check(jobStatus, {
								"cross-shard transfer job completed": (s) => s === "completed",
							});
							break;
						}
					}
					sleep(POLLING_SLEEP_SEC);
				}
			}
		}
	}
}
