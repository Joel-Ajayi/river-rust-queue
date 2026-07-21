import { BASE_URL, MERCHANT_COUNT } from "./config.js";

function toHex(n, pad) {
  return n.toString(16).padStart(pad, "0");
}

export function merchantID(index) {
  const n = (index % MERCHANT_COUNT) + 1;
  return `merchant_00000000-0000-0000-0000-${toHex(n, 12)}`;
}

export function walletID(index) {
  const i = index % 100000;
  const merchantNum = Math.floor(i / 1000) + 1;
  const walletNum = (i % 1000) + 1;
  return `merchant_00000000-0000-0000-0000-${toHex(merchantNum, 12)}.00000000-0000-0000-0000-${toHex(walletNum, 12)}`;
}

export function ownedWallet(merchantIdx, index) {
  const walletNum = (index % 1000) + 1;
  const id = merchantID(merchantIdx);
  return `${id}.00000000-0000-0000-0000-${toHex(walletNum, 12)}`;
}

export function getAuthToken(merchantIdx) {
  const id = merchantID(merchantIdx);
  return {
    method: "POST",
    url: `${BASE_URL}/v1/auth/token`,
    headers: {
      Authorization: `Bearer ${id}:test-api-key-perf`,
    },
  };
}

export function transferRequest(fromWallet, toWallet, amount, jwt) {
  return {
    method: "POST",
    url: `${BASE_URL}/v1/transfers`,
    headers: {
      Authorization: `Bearer ${jwt}`,
      "Content-Type": "application/json",
      "Idempotency-Key": `${__VU}-${__ITER}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`,
    },
    body: JSON.stringify({
      from_wallet: fromWallet,
      to_wallet: toWallet,
      amount: amount,
      currency: "NGN",
    }),
  };
}

export function payoutRequest(fromWallet, recipients, jwt) {
  return {
    method: "POST",
    url: `${BASE_URL}/v1/payouts`,
    headers: {
      Authorization: `Bearer ${jwt}`,
      "Content-Type": "application/json",
      "Idempotency-Key": `${__VU}-${__ITER}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`,
    },
    body: JSON.stringify({
      from_wallet: fromWallet,
      recipients: recipients,
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

// depositRequest initiates a wallet deposit transaction.
export function depositRequest(walletId, amount, jwt) {
  return {
    method: "POST",
    url: `${BASE_URL}/v1/wallets/${walletId}/deposit`,
    headers: {
      Authorization: `Bearer ${jwt}`,
      "Content-Type": "application/json",
      "Idempotency-Key": `dep-${walletId}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`,
    },
    body: JSON.stringify({
      amount: amount,
      currency: "NGN",
    }),
  };
}
