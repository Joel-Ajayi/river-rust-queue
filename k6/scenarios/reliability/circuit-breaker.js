import { check } from "k6";
import http from "k6/http";
import { Trend, Rate } from "k6/metrics";
import {
  transferRequest,
  depositRequest,
  selectWalletPair,
} from "../../lib/helpers.js";
import { prepareTestData } from "../../lib/setup-helper.js";

const DEPOSIT_PROBABILITY = 0.3;
const MIN_AMOUNT = 100;
const MAX_AMOUNT = 1000000;
const HEALTH_CHECK_INTERVAL = 25;
const STATUS_ACCEPTED = 202;
const STATUS_OK = 200;

const transferDuration = new Trend("transfer_duration_ms");
const depositDuration = new Trend("deposit_duration_ms");
const requestSuccessRate = new Rate("request_success_rate");

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

export function setup() {
  return prepareTestData();
}

export default function (data) {
  const { from, to, jwt } = selectWalletPair(data, __VU, __ITER);
  const amount =
    Math.floor(Math.random() * (MAX_AMOUNT - MIN_AMOUNT)) + MIN_AMOUNT;

  const isDeposit = Math.random() < DEPOSIT_PROBABILITY;

  if (isDeposit) {
    const res = http.request(depositRequest(from, amount, jwt));
    depositDuration.add(res.timings.duration);
    const success = res.status === STATUS_ACCEPTED || res.status === STATUS_OK;
    requestSuccessRate.add(success);
    check(res, {
      "deposit responded": (r) =>
        r.status === STATUS_ACCEPTED || r.status === STATUS_OK,
    });
  } else {
    const res = http.request(transferRequest(from, to, amount, jwt));
    transferDuration.add(res.timings.duration);
    const success = res.status === STATUS_ACCEPTED || res.status === STATUS_OK;
    requestSuccessRate.add(success);
    check(res, {
      "transfer responded": (r) =>
        r.status === STATUS_ACCEPTED || r.status === STATUS_OK,
    });
  }

  if (__ITER % HEALTH_CHECK_INTERVAL === 0) {
    const healthRes = http.get(
      `${__ENV.BASE_URL || "https://api.127.0.0.1.nip.io"}/health`,
    );
    check(healthRes, { "health ok": (r) => r.status === STATUS_OK });
  }
}
