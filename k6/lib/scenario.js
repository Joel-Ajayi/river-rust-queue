import { check } from "k6";
import http from "k6/http";
import { Trend, Rate } from "k6/metrics";
import {
  transferRequest,
  depositRequest,
  selectWalletPair,
} from "./helpers.js";

const STATUS_ACCEPTED = 202;
const STATUS_OK = 200;

export function createScenario(defaultFn) {
  return {
    transferDuration: new Trend("transfer_duration_ms"),
    depositDuration: new Trend("deposit_duration_ms"),
    requestSuccessRate: new Rate("request_success_rate"),
    defaultFn,
  };
}

export function runTransferOrDeposit(data, opts) {
  const {
    depositProbability = 0.3,
    minAmount = 100,
    maxAmount = 1000000,
    transferDuration,
    depositDuration,
    requestSuccessRate,
  } = opts;

  const { from, to, jwt } = selectWalletPair(data, __VU, __ITER);
  const amount =
    Math.floor(Math.random() * (maxAmount - minAmount)) + minAmount;
  const isDeposit = Math.random() < depositProbability;

  if (isDeposit) {
    const res = http.request(depositRequest(from, amount, jwt));
    depositDuration.add(res.timings.duration);
    const success = res.status === STATUS_ACCEPTED || res.status === STATUS_OK;
    requestSuccessRate.add(success);
    let hasJobId = false;
    try {
      const body = res.body ? JSON.parse(res.body) : {};
      hasJobId = typeof body.job_id === "string" && body.job_id.length > 0;
    } catch (e) {}
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
    } catch (e) {}
    check(res, {
      "transfer accepted": () => success,
      "transfer has job_id": () => hasJobId,
    });
  }
}
