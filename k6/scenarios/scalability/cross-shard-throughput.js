import { check, sleep } from "k6";
import http from "k6/http";
import { Trend, Rate } from "k6/metrics";
import {
  transferRequest,
  depositRequest,
  jobStatusRequest,
  selectWalletPair,
} from "../../lib/helpers.js";
import { prepareTestData } from "../../lib/setup-helper.js";

const DEPOSIT_PROBABILITY = 0.3;
const MIN_AMOUNT = 100;
const MAX_AMOUNT = 500000;
const STATUS_ACCEPTED = 202;
const STATUS_OK = 200;
const POLLING_ATTEMPTS = 20;
const POLLING_SLEEP_SEC = 1;
const MERCHANT_SHARDS_BOUND = 50;

const transferDuration = new Trend("transfer_duration_ms");
const depositDuration = new Trend("deposit_duration_ms");
const requestSuccessRate = new Rate("request_success_rate");

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

export function setup() {
  return prepareTestData();
}

export default function (data) {
  const isCrossShard = __ITER % 2 === 0;
  const fromMerchant = __VU % MERCHANT_SHARDS_BOUND;
  const toMerchant = isCrossShard
    ? MERCHANT_SHARDS_BOUND + (__VU % MERCHANT_SHARDS_BOUND)
    : fromMerchant;

  const walletIdx = (__ITER + __VU * 37) % 1000;
  const from = data.wallets[fromMerchant][walletIdx];
  const to = data.wallets[toMerchant][(walletIdx + 1) % 1000];
  const amount =
    Math.floor(Math.random() * (MAX_AMOUNT - MIN_AMOUNT)) + MIN_AMOUNT;

  const isDeposit = Math.random() < DEPOSIT_PROBABILITY;

  if (isDeposit) {
    const res = http.request(
      depositRequest(from, amount, data.jwts[fromMerchant]),
    );
    depositDuration.add(res.timings.duration);
    requestSuccessRate.add(
      res.status === STATUS_ACCEPTED || res.status === STATUS_OK,
    );

    check(res, {
      "deposit accepted": (r) =>
        r.status === STATUS_ACCEPTED || r.status === STATUS_OK,
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
      } catch (e) {}

      if (jobId) {
        let jobCompleted = false;
        for (let attempt = 0; attempt < POLLING_ATTEMPTS; attempt++) {
          const statusRes = http.request(
            jobStatusRequest(jobId, data.jwts[fromMerchant]),
          );
          if (statusRes.status === STATUS_OK) {
            let jobStatus = "";
            try {
              const jobBody = statusRes.body ? JSON.parse(statusRes.body) : {};
              jobStatus = jobBody.status || "";
            } catch (e) {}
            if (jobStatus === "completed" || jobStatus === "failed") {
              check(jobStatus, {
                "cross-shard deposit job completed": (s) => s === "completed",
              });
              jobCompleted = true;
              break;
            }
          }
          sleep(POLLING_SLEEP_SEC);
        }
        if (!jobCompleted) {
          check(jobId, {
            "cross-shard deposit job completed": () => false,
          });
        }
      }
    }
  } else {
    const res = http.request(
      transferRequest(from, to, amount, data.jwts[fromMerchant]),
    );
    transferDuration.add(res.timings.duration);
    requestSuccessRate.add(
      res.status === STATUS_ACCEPTED || res.status === STATUS_OK,
    );

    check(res, {
      "transfer accepted": (r) =>
        r.status === STATUS_ACCEPTED || r.status === STATUS_OK,
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
      } catch (e) {}

      if (jobId) {
        let jobCompleted = false;
        for (let attempt = 0; attempt < POLLING_ATTEMPTS; attempt++) {
          const statusRes = http.request(
            jobStatusRequest(jobId, data.jwts[fromMerchant]),
          );
          if (statusRes.status === STATUS_OK) {
            let jobStatus = "";
            try {
              const jobBody = statusRes.body ? JSON.parse(statusRes.body) : {};
              jobStatus = jobBody.status || "";
            } catch (e) {}
            if (jobStatus === "completed" || jobStatus === "failed") {
              check(jobStatus, {
                "cross-shard transfer job completed": (s) => s === "completed",
              });
              jobCompleted = true;
              break;
            }
          }
          sleep(POLLING_SLEEP_SEC);
        }
        if (!jobCompleted) {
          check(jobId, {
            "cross-shard transfer job completed": () => false,
          });
        }
      }
    }
  }
}
