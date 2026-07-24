import { SharedArray } from "k6/data";

// Read the pre-seeded test data from test-data.json
const testData = new SharedArray("testData", function () {
  return [JSON.parse(open("k6/test-data.json"))];
});

export function prepareTestData() {
  if (!testData || testData.length === 0) {
    throw new Error(
      "Test data not found. Please run seed-test-data.mjs first.",
    );
  }
  return testData[0];
}
