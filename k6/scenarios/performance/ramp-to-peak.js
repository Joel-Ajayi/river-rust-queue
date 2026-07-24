import { prepareTestData } from "../../lib/setup-helper.js";
import { createScenario, runTransferOrDeposit } from "../../lib/scenario.js";

const scenario = createScenario();

export const options = {
  scenarios: {
    ramp_to_peak: {
      executor: "ramping-vus",
      startVUs: 0,
      stages: [
        { duration: "5m", target: 200 },
        { duration: "5m", target: 500 },
        { duration: "5m", target: 1000 },
        { duration: "5m", target: 2000 },
        { duration: "5m", target: 2000 },
        { duration: "5m", target: 0 },
      ],
      gracefulRampDown: "30s",
    },
  },
  thresholds: {
    http_req_duration: ["p(95)<500", "p(99)<1000", "avg<250"],
    http_req_failed: ["rate<0.001"],
  },
};

export function setup() {
  return prepareTestData();
}

export default function (data) {
  runTransferOrDeposit(data, {
    depositProbability: 0.3,
    minAmount: 100,
    maxAmount: 500000,
    transferDuration: scenario.transferDuration,
    depositDuration: scenario.depositDuration,
    requestSuccessRate: scenario.requestSuccessRate,
  });
}
