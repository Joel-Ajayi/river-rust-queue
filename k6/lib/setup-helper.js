import http from "k6/http";
import { sleep } from "k6";
import { BASE_URL, MERCHANT_COUNT } from "./config.js";

// prepareTestData registers standard merchants and customer wallets via core-api.
export function prepareTestData() {
	const platformKey = __ENV.RRQ_PLATFORM_KEY;
	if (!platformKey) {
		throw new Error("RRQ_PLATFORM_KEY environment variable is required to register merchants");
	}

	console.log(`Preparing test data: creating ${MERCHANT_COUNT} merchants...`);

	// 1. Batch create merchants
	const merchantRequests = [];
	for (let i = 0; i < MERCHANT_COUNT; i++) {
		merchantRequests.push({
			method: "POST",
			url: `${BASE_URL}/v1/merchants`,
			body: JSON.stringify({
				name: `Perf Merchant ${i + 1}`,
				webhook_url: "http://webhook-echo:8080/",
				webhook_secret: `secret-key-perf-${i + 1}`,
				tier: "standard",
			}),
			params: {
				headers: {
					"Content-Type": "application/json",
					"Authorization": `Bearer ${platformKey}`,
				},
			},
		});
	}

	const merchantResponses = http.batch(merchantRequests);
	const merchants = [];

	for (let i = 0; i < MERCHANT_COUNT; i++) {
		const res = merchantResponses[i];
		if (res.status !== 201) {
			throw new Error(`Failed to create merchant ${i}: status=${res.status} body=${res.body}`);
		}
		const body = JSON.parse(res.body);
		merchants.push({
			id: body.merchant_id,
			apiKey: body.api_key,
		});
	}

	console.log(`Successfully created ${MERCHANT_COUNT} merchants. Logging in to acquire JWTs...`);

	// 2. Batch login to get JWTs
	const authRequests = [];
	for (let i = 0; i < MERCHANT_COUNT; i++) {
		const m = merchants[i];
		authRequests.push({
			method: "POST",
			url: `${BASE_URL}/v1/auth/token`,
			params: {
				headers: {
					"Authorization": `Bearer ${m.apiKey}`,
				},
			},
		});
	}

	const authResponses = http.batch(authRequests);
	const jwts = [];
	for (let i = 0; i < MERCHANT_COUNT; i++) {
		const res = authResponses[i];
		if (res.status !== 200) {
			throw new Error(`Failed to log in for merchant ${merchants[i].id}: status=${res.status} body=${res.body}`);
		}
		jwts.push(JSON.parse(res.body).token);
	}

	console.log("Successfully acquired JWTs. Creating 100,000 wallets (1,000 per merchant)...");

	const wallets = [];
	for (let i = 0; i < MERCHANT_COUNT; i++) {
		wallets.push([]);
	}

	// Create wallets in 10 rounds to avoid exhaustion
	const ROUNDS = 10;
	const WALLETS_PER_ROUND = 100;

	for (let r = 0; r < ROUNDS; r++) {
		console.log(`  Wallet creation round ${r + 1}/${ROUNDS}...`);
		const walletRequests = [];
		const mapping = [];

		for (let m = 0; m < MERCHANT_COUNT; m++) {
			const jwt = jwts[m];
			for (let w = 0; w < WALLETS_PER_ROUND; w++) {
				walletRequests.push({
					method: "POST",
					url: `${BASE_URL}/v1/wallets`,
					body: JSON.stringify({
						currency: "NGN",
					}),
					params: {
						headers: {
							"Content-Type": "application/json",
							"Authorization": `Bearer ${jwt}`,
						},
					},
				});
				mapping.push(m);
			}
		}

		const walletResponses = http.batch(walletRequests);
		for (let i = 0; i < walletResponses.length; i++) {
			const res = walletResponses[i];
			const mIdx = mapping[i];
			if (res.status !== 201) {
				throw new Error(`Failed to create wallet for merchant ${merchants[mIdx].id}: status=${res.status} body=${res.body}`);
			}
			wallets[mIdx].push(JSON.parse(res.body).wallet_id);
		}
	}

	console.log("Database successfully seeded with 100 merchants and 100,000 wallets.");
	console.log("Waiting 35 seconds for Kong Sync Worker to update gateway consumer configurations...");
	sleep(35);

	return { jwts, wallets };
}
