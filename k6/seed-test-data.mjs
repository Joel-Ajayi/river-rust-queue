import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const BASE_URL = process.env.BASE_URL || "https://api.127.0.0.1.nip.io";
const MERCHANT_COUNT = 100;
const WALLETS_PER_MERCHANT = 1000;
const CONCURRENCY = 50; // Max concurrent requests per batch
const INITIAL_DEPOSIT_AMOUNT = 10000000; // 100,000 NGN in smallest unit

// Simple batch concurrency limiter — processes items in batches of CONCURRENCY
async function batchProcess(items, fn) {
  for (let i = 0; i < items.length; i += CONCURRENCY) {
    const batch = items.slice(i, i + CONCURRENCY);
    await Promise.all(batch.map(fn));
  }
}

async function fetchWithRetry(url, options, retries = 3) {
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      const res = await fetch(url, options);
      if (!res.ok && res.status >= 500 && attempt < retries - 1) {
        await new Promise((r) => setTimeout(r, 1000 * (attempt + 1)));
        continue;
      }
      return res;
    } catch (err) {
      if (attempt < retries - 1) {
        await new Promise((r) => setTimeout(r, 1000 * (attempt + 1)));
        continue;
      }
      throw err;
    }
  }
}

async function main() {
  // ── Phase 1: Create merchants (POST /v1/merchants is unauthenticated) ──
  console.log(`\nPreparing test data: creating ${MERCHANT_COUNT} merchants...`);
  const merchants = [];
  for (let i = 0; i < MERCHANT_COUNT; i++) {
    const res = await fetchWithRetry(`${BASE_URL}/v1/merchants`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        name: `Perf Merchant ${i + 1}`,
        webhookUrl: "http://webhook-echo:8080/",
        webhookSecret: `secret-key-perf-${i + 1}`,
        tier: "standard",
      }),
    });
    if (!res.ok) {
      console.error(`Failed to create merchant ${i}: status=${res.status}`);
      process.exit(1);
    }
    const body = await res.json();
    merchants.push({ id: body.merchantId, apiKey: body.apiKey });
  }

  // ── Phase 2: Acquire JWTs for each merchant ──
  console.log(
    `Successfully created ${MERCHANT_COUNT} merchants. Acquiring JWTs...`,
  );
  const jwts = [];
  for (let i = 0; i < MERCHANT_COUNT; i++) {
    const res = await fetchWithRetry(`${BASE_URL}/v1/auth/token`, {
      method: "POST",
      headers: { Authorization: `Bearer ${merchants[i].apiKey}` },
    });
    if (!res.ok) {
      console.error(
        `Failed to log in for merchant ${merchants[i].id}: status=${res.status}`,
      );
      process.exit(1);
    }
    jwts.push((await res.json()).token);
  }

  // ── Phase 3: Create wallets (1,000 per merchant) ──
  console.log(`Creating ${MERCHANT_COUNT * WALLETS_PER_MERCHANT} wallets...`);
  const wallets = Array.from({ length: MERCHANT_COUNT }, () => []);
  const ROUNDS = 10;
  const WALLETS_PER_ROUND = WALLETS_PER_MERCHANT / ROUNDS;

  for (let r = 0; r < ROUNDS; r++) {
    console.log(`  Wallet creation round ${r + 1}/${ROUNDS}...`);
    const requests = [];
    for (let m = 0; m < MERCHANT_COUNT; m++) {
      const jwt = jwts[m];
      for (let w = 0; w < WALLETS_PER_ROUND; w++) {
        requests.push(
          fetchWithRetry(`${BASE_URL}/v1/wallets`, {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              Authorization: `Bearer ${jwt}`,
            },
            body: JSON.stringify({ currency: "NGN" }),
          }).then(async (res) => {
            if (!res.ok) throw new Error(`Wallet create failed: ${res.status}`);
            const body = await res.json();
            wallets[m].push(body.walletId);
          }),
        );
      }
    }
    // Process in batches of CONCURRENCY
    for (let i = 0; i < requests.length; i += CONCURRENCY) {
      const batch = requests.slice(i, i + CONCURRENCY);
      await Promise.all(batch);
    }
  }

  // ── Phase 4: Pre-fund wallets with deposits (separate phase) ──
  // This ensures wallets have balance before k6 transfer tests run.
  console.log(
    `\nPre-funding ${MERCHANT_COUNT * WALLETS_PER_MERCHANT} wallets with initial deposits...`,
  );
  let fundedCount = 0;
  for (let m = 0; m < MERCHANT_COUNT; m++) {
    const jwt = jwts[m];
    const depositRequests = [];
    for (let w = 0; w < WALLETS_PER_MERCHANT; w++) {
      depositRequests.push(
        fetchWithRetry(`${BASE_URL}/v1/wallets/${wallets[m][w]}/deposit`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${jwt}`,
            "Idempotency-Key": `seed-dep-${m}-${w}`,
          },
          body: JSON.stringify({
            amount: INITIAL_DEPOSIT_AMOUNT,
            currency: "NGN",
          }),
        }).then(async (res) => {
          if (!res.ok) {
            console.error(
              `Deposit failed for wallet ${wallets[m][w]}: status=${res.status}`,
            );
          }
        }),
      );
    }
    // Process deposits in batches of CONCURRENCY
    for (let i = 0; i < depositRequests.length; i += CONCURRENCY) {
      const batch = depositRequests.slice(i, i + CONCURRENCY);
      await Promise.all(batch);
    }
    fundedCount += WALLETS_PER_MERCHANT;
    console.log(
      `  Funded ${fundedCount}/${MERCHANT_COUNT * WALLETS_PER_MERCHANT} wallets`,
    );
  }

  // Wait for deposits to be processed by the ledger worker
  console.log(`\nWaiting 30s for deposits to be processed by ledger worker...`);
  await new Promise((r) => setTimeout(r, 30000));

  // ── Phase 5: Write test data ──
  console.log("Writing test-data.json...");
  const outData = { jwts, wallets };
  fs.writeFileSync(
    path.join(__dirname, "test-data.json"),
    JSON.stringify(outData, null, 2),
  );
  console.log("Done! Written to k6/test-data.json");
  console.log(`  Merchants: ${MERCHANT_COUNT}`);
  console.log(`  Wallets: ${MERCHANT_COUNT * WALLETS_PER_MERCHANT}`);
  console.log(`  Initial deposit per wallet: ${INITIAL_DEPOSIT_AMOUNT}`);
}

main().catch(console.error);
