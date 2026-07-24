import { sleep } from "k6";
import { prepareTestData } from "../../lib/setup-helper.js";
import { createScenario, runTransferOrDeposit } from "../../lib/scenario.js";

const scenario = createScenario();

const SOAK_LIMIT_INTERVAL_SEC = 3600;
const SOAK_LIMIT_WINDOW_SEC = 5;

let startTime = Date.now();

export const options = {
  scenarios: {
    soak_endurance: {
      executor: "constant-vus",
      vus: 500,
      duration: "4h",
    },
  },
  thresholds: {
    http_req_duration: ["p(95)<500", "p(99)<1000", "avg<200"],
    http_req_failed: ["rate<0.001"],
    http_reqs: ["rate>500"],
    iteration_duration: ["p(95)<2000"],
  },
};

export function setup() {
  return prepareTestData();
}

export default function (data) {
  runTransferOrDeposit(data, {
    depositProbability: 0.3,
    minAmount: 100,
    maxAmount: 100000,
    transferDuration: scenario.transferDuration,
    depositDuration: scenario.depositDuration,
    requestSuccessRate: scenario.requestSuccessRate,
  });

  const elapsed = (Date.now() - startTime) / 1000;
  if (
    elapsed > SOAK_LIMIT_INTERVAL_SEC &&
    elapsed < SOAK_LIMIT_INTERVAL_SEC + SOAK_LIMIT_WINDOW_SEC
  ) {
    console.log(`Hourly indicator reached: elapsed=${elapsed}s`);
  }
}
