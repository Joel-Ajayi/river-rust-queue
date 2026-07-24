import { check } from "k6";
import http from "k6/http";
import { Trend, Rate } from "k6/metrics";
import { transferRequest } from "../../lib/helpers.js";
import { prepareTestData } from "../../lib/setup-helper.js";

const MIN_AMOUNT = 50;
const MAX_AMOUNT = 500000;
const BATCH_SIZE = 500;
const STATUS_ACCEPTED = 202;
const STATUS_OK = 200;

const transferDuration = new Trend("transfer_duration_ms");
const requestSuccessRate = new Rate("request_success_rate");

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

export function setup() {
  return prepareTestData();
}

export default function (data) {
  const merchantIdx = __VU % 100;
  const jwt = data.jwts[merchantIdx];
  const fromWallet = data.wallets[merchantIdx][__VU % 1000];
  const toMerchantIdx = (merchantIdx + 1) % 100;

  const requests = [];
  for (let i = 0; i < BATCH_SIZE; i++) {
    const to = data.wallets[toMerchantIdx][(i + __VU) % 1000];
    const amount =
      Math.floor(Math.random() * (MAX_AMOUNT - MIN_AMOUNT)) + MIN_AMOUNT;
    const req = transferRequest(fromWallet, to, amount, jwt);
    requests.push({
      method: req.method,
      url: req.url,
      body: req.body,
      params: { headers: req.headers },
    });
  }

  const responses = http.batch(requests);

  let successCount = 0;
  let totalDuration = 0;
  for (const res of responses) {
    const success = res.status === STATUS_ACCEPTED || res.status === STATUS_OK;
    requestSuccessRate.add(success);
    if (success) successCount++;
    totalDuration += res.timings.duration;
  }

  transferDuration.add(Math.round(totalDuration / responses.length));

  check(responses, {
    "at least 90% transfers accepted": () => successCount >= BATCH_SIZE * 0.9,
  });
}
