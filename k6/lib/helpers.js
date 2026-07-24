import { BASE_URL, MERCHANT_COUNT } from "./config.js";

// Generate a UUIDv4 for Idempotency-Key headers (required by the API).
function uuidv4() {
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, function (c) {
    const r = (Math.random() * 16) | 0;
    const v = c === "x" ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

export function transferRequest(fromWallet, toWallet, amount, jwt) {
  return {
    method: "POST",
    url: `${BASE_URL}/v1/transfers`,
    headers: {
      Authorization: `Bearer ${jwt}`,
      "Content-Type": "application/json",
      "Idempotency-Key": uuidv4(),
    },
    body: JSON.stringify({
      fromWallet: fromWallet,
      toWallet: toWallet,
      amount: amount,
      currency: "NGN",
    }),
  };
}

export function jobStatusRequest(jobId, jwt) {
  return {
    method: "GET",
    url: `${BASE_URL}/v1/jobs/${jobId}`,
    headers: { Authorization: `Bearer ${jwt}` },
  };
}

export function walletBalanceRequest(walletId, jwt) {
  return {
    method: "GET",
    url: `${BASE_URL}/v1/balances?wallet_id=${walletId}`,
    headers: { Authorization: `Bearer ${jwt}` },
  };
}

export function depositRequest(walletId, amount, jwt) {
  return {
    method: "POST",
    url: `${BASE_URL}/v1/wallets/${walletId}/deposit`,
    headers: {
      Authorization: `Bearer ${jwt}`,
      "Content-Type": "application/json",
      "Idempotency-Key": uuidv4(),
    },
    body: JSON.stringify({
      amount: amount,
      currency: "NGN",
    }),
  };
}

// selectWalletPair returns a from-wallet, to-wallet, and JWT for a given VU/iteration.
// Uses __VU % MERCHANT_COUNT for merchant selection to avoid hotspotting with VU > 1000.
// Wallet index uses (iter + vu * 37) % 1000 for better distribution across VUs.
export function selectWalletPair(data, vu, iter) {
  const merchantIdx = vu % MERCHANT_COUNT;
  const walletIdx = (iter + vu * 37) % 1000;
  const from = data.wallets[merchantIdx][walletIdx];
  const toMerchant = (merchantIdx + 1) % MERCHANT_COUNT;
  const toWalletIdx = (walletIdx + 1) % 1000;
  const to = data.wallets[toMerchant][toWalletIdx];
  return { from, to, jwt: data.jwts[merchantIdx], merchantIdx };
}
