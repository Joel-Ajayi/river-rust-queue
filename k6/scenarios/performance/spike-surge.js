import { prepareTestData } from "../../lib/setup-helper.js";
import { createScenario, runTransferOrDeposit } from "../../lib/scenario.js";

const scenario = createScenario();

export const options = {
  scenarios: {
    spike_surge: {
      executor: "ramping-vus",
      startVUs: 0,
      stages: [
        { duration: "5s", target: 2000 },
        { duration: "60s", target: 2000 },
        { duration: "30s", target: 0 },
      ],
      gracefulRampDown: "10s",
    },
  },
  thresholds: {
    http_req_duration: ["p(95)<500", "p(99)<1000"],
    http_req_failed: ["rate<0.01"],
    http_reqs: ["rate>2000"],
  },
};

export function setup() {
  return prepareTestData();
}

export default function (data) {
  runTransferOrDeposit(data, {
    depositProbability: 0.3,
    minAmount: 50,
    maxAmount: 500000,
    transferDuration: scenario.transferDuration,
    depositDuration: scenario.depositDuration,
    requestSuccessRate: scenario.requestSuccessRate,
  });
}
