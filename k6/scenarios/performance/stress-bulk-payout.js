import { check } from "k6";
import http from "k6/http";
import { Trend, Rate } from "k6/metrics";
import { payoutRequest } from "../../lib/helpers.js";
import { prepareTestData } from "../../lib/setup-helper.js";

// Scenario settings and payout parameters.
const MIN_PAYOUT_AMOUNT = 50;
const MAX_PAYOUT_AMOUNT = 500000;
const RECIPIENTS_COUNT = 500;
const STATUS_ACCEPTED = 202;
const STATUS_OK = 200;

// Custom telemetry metrics.
const payoutDuration = new Trend("payout_duration_ms");
const requestSuccessRate = new Rate("request_success_rate");

// Stress testing options for high volume bulk payout requests.
export const options = {
	scenarios: {
		bulk_payout_stress: {
			executor: "constant-vus",
			vus: 50,
			duration: "15m",
		},
	},
	thresholds: {
		http_req_duration: ["p(95)<2000", "p(99)<5000", "avg<1000"],
		http_req_failed: ["rate<0.01"],
	},
};

// Seeding phase executed once before stress test starts.
export function setup() {
	return prepareTestData();
}

// Bulk recipient generation tool using standard performance wallets.
function generateRecipients(count, walletsList) {
	const recipients = [];
	for (let i = 0; i < count; i++) {
		recipients.push({
			to_wallet: walletsList[i % 1000],
			amount: Math.floor(Math.random() * (MAX_PAYOUT_AMOUNT - MIN_PAYOUT_AMOUNT)) + MIN_PAYOUT_AMOUNT,
		});
	}
	return recipients;
}

// Simulated workload traffic routing logic for bulk payouts.
export default function (data) {
	const jwt = data.jwts[0];
	const fromWallet = data.wallets[0][__VU % 1000];
	const recipients = generateRecipients(RECIPIENTS_COUNT, data.wallets[1]);

	const res = http.request(payoutRequest(fromWallet, recipients, jwt));

	payoutDuration.add(res.timings.duration);
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
		"payout accepted": () => success,
		"payout has job_id": () => hasJobId,
	});
}
