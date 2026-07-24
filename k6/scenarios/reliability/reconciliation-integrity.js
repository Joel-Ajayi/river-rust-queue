import { check, sleep } from "k6";
import http from "k6/http";
import { Trend, Rate } from "k6/metrics";
import {
  transferRequest,
  depositRequest,
  jobStatusRequest,
  walletBalanceRequest,
  selectWalletPair,
} from "../../lib/helpers.js";
import { prepareTestData } from "../../lib/setup-helper.js";

const DEPOSIT_PROBABILITY = 0.3;
const MIN_AMOUNT = 10;
const MAX_AMOUNT = 100000;
const STATUS_ACCEPTED = 202;
const STATUS_OK = 200;
const POLLING_ATTEMPTS = 10;
const POLLING_SLEEP_SEC = 0.5;
const BALANCE_CHECK_INTERVAL = 10;

const transferDuration = new Trend("transfer_duration_ms");
const depositDuration = new Trend("deposit_duration_ms");
const requestSuccessRate = new Rate("request_success_rate");

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

export function setup() {
  return prepareTestData();
}

export default function (data) {
  const { from, to, jwt, merchantIdx } = selectWalletPair(data, __VU, __ITER);
  const amount =
    Math.floor(Math.random() * (MAX_AMOUNT - MIN_AMOUNT)) + MIN_AMOUNT;

  const isDeposit = Math.random() < DEPOSIT_PROBABILITY;

  if (isDeposit) {
    const depositRes = http.request(depositRequest(from, amount, jwt));
    depositDuration.add(depositRes.timings.duration);
    requestSuccessRate.add(
      depositRes.status === STATUS_ACCEPTED || depositRes.status === STATUS_OK,
    );

    if (depositRes.status === STATUS_ACCEPTED) {
      let jobId = "";
      try {
        const body = depositRes.body ? JSON.parse(depositRes.body) : {};
        jobId = body.job_id || "";
      } catch (e) {}

      if (jobId) {
        for (let attempt = 0; attempt < POLLING_ATTEMPTS; attempt++) {
          const statusRes = http.request(jobStatusRequest(jobId, jwt));
          if (statusRes.status === STATUS_OK) {
            let jobStatus = "";
            try {
              const jobBody = statusRes.body ? JSON.parse(statusRes.body) : {};
              jobStatus = jobBody.status || "";
            } catch (e) {}
            if (jobStatus === "completed" || jobStatus === "failed") {
              break;
            }
          }
          sleep(POLLING_SLEEP_SEC);
        }
      }
    }
  } else {
    const transferRes = http.request(transferRequest(from, to, amount, jwt));
    transferDuration.add(transferRes.timings.duration);
    requestSuccessRate.add(
      transferRes.status === STATUS_ACCEPTED ||
        transferRes.status === STATUS_OK,
    );

    if (transferRes.status === STATUS_ACCEPTED) {
      let jobId = "";
      try {
        const body = transferRes.body ? JSON.parse(transferRes.body) : {};
        jobId = body.job_id || "";
      } catch (e) {}

      if (jobId) {
        for (let attempt = 0; attempt < POLLING_ATTEMPTS; attempt++) {
          const statusRes = http.request(jobStatusRequest(jobId, jwt));
          if (statusRes.status === STATUS_OK) {
            let jobStatus = "";
            try {
              const jobBody = statusRes.body ? JSON.parse(statusRes.body) : {};
              jobStatus = jobBody.status || "";
            } catch (e) {}
            if (jobStatus === "completed" || jobStatus === "failed") {
              break;
            }
          }
          sleep(POLLING_SLEEP_SEC);
        }
      }
    }
  }

  if (__ITER % BALANCE_CHECK_INTERVAL === 0) {
    const balanceRes = http.request(walletBalanceRequest(from, jwt));
    let balance = -1;
    try {
      const balanceBody = balanceRes.body ? JSON.parse(balanceRes.body) : {};
      balance =
        typeof balanceBody.balance === "number" ? balanceBody.balance : -1;
    } catch (e) {}

    check(balanceRes, {
      "balance endpoint ok": (r) => r.status === STATUS_OK,
      "balance non-negative": () => balance >= 0,
    });
  }
}
