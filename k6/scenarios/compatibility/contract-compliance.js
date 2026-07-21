import { check, sleep } from "k6";
import http from "k6/http";
import { Rate } from "k6/metrics";
import { transferRequest, jobStatusRequest } from "../../lib/helpers.js";
import { prepareTestData } from "../../lib/setup-helper.js";

const AMOUNT = 100;
const STATUS_ACCEPTED = 202;
const STATUS_OK = 200;
const POLLING_SLEEP_SEC = 0.5;
const LOOP_SLEEP_SEC = 1;

const contractSuccessRate = new Rate("contract_compliance_rate");

export const options = {
	scenarios: {
		contract_compliance_concurrency: {
			executor: "constant-vus",
			vus: 50,
			duration: "5m",
		},
	},
	thresholds: {
		http_req_failed: ["rate<0.001"],
	},
};

export function setup() {
	return prepareTestData();
}

export default function (data) {
	const jwt = data.jwts[0];
	const from = data.wallets[0][0];
	const to = data.wallets[1][0];

	// 1. Validate /v1/transfers response contract
	const resTransfer = http.request(transferRequest(from, to, AMOUNT, jwt));
	const transferSuccess = resTransfer.status === STATUS_ACCEPTED || resTransfer.status === STATUS_OK;
	check(resTransfer, {
		"transfer status is 202 or 200": () => transferSuccess,
	});

	if (transferSuccess) {
		let body = {};
		let parseSuccess = false;
		try {
			body = resTransfer.body ? JSON.parse(resTransfer.body) : {};
			parseSuccess = true;
		} catch (e) {
			// Ignored
		}

		const hasValidJobFields =
			parseSuccess &&
			typeof body.job_id === "string" &&
			body.job_id.length > 0 &&
			typeof body.status === "string" &&
			body.status.length > 0 &&
			body._links &&
			typeof body._links.self === "string" &&
			body._links.self.length > 0;

		contractSuccessRate.add(hasValidJobFields);

		check(body, {
			"transfer response has job_id": () => typeof body.job_id === "string" && body.job_id.length > 0,
			"transfer response has status": () => typeof body.status === "string" && body.status.length > 0,
			"transfer response has self link": () => body._links && typeof body._links.self === "string" && body._links.self.length > 0,
		});

		const jobId = body.job_id || "";

		if (jobId) {
			// 2. Validate /v1/jobs/{id} response contract
			sleep(POLLING_SLEEP_SEC);
			const resJob = http.request(jobStatusRequest(jobId, jwt));
			const jobGetSuccess = resJob.status === STATUS_OK;
			check(resJob, {
				"job status is 200": () => jobGetSuccess,
			});

			if (jobGetSuccess) {
				let jobBody = {};
				let jobParseSuccess = false;
				try {
					jobBody = resJob.body ? JSON.parse(resJob.body) : {};
					jobParseSuccess = true;
				} catch (e) {
					// Ignored
				}

				const hasValidGetFields =
					jobParseSuccess &&
					typeof jobBody.job_id === "string" &&
					jobBody.job_id === jobId &&
					typeof jobBody.type === "string" &&
					jobBody.type.length > 0 &&
					typeof jobBody.status === "string" &&
					jobBody.status.length > 0 &&
					(typeof jobBody.occurred_at === "string" || jobBody.occurred_at === undefined);

				contractSuccessRate.add(hasValidGetFields);

				check(jobBody, {
					"job response has job_id": () => typeof jobBody.job_id === "string" && jobBody.job_id === jobId,
					"job response has type": () => typeof jobBody.type === "string" && jobBody.type.length > 0,
					"job response has status": () => typeof jobBody.status === "string" && jobBody.status.length > 0,
					"job response has occurred_at or equivalent": () => typeof jobBody.occurred_at === "string" || jobBody.occurred_at === undefined,
				});
			}
		}
	}

	sleep(LOOP_SLEEP_SEC);
}
