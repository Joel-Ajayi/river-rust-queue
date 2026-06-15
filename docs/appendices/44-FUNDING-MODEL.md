# 44 — Funding Model

> **What this is.** The model for how money enters and exits RRQ. Explicit treatment of an aspect the original docs left implicit. Short and reference-format.

---

## The fundamental question

RRQ moves money between wallets. Every wallet starts with zero balance. Two transfers can move money around the system, but they can't *create* it. So: where does the initial balance come from?

In a real production payment system, the answer is "from external sources" — bank transfers, card top-ups, money received from other PSPs, etc. RRQ has no integration with any external financial system in v1. Money has to get in *somehow* for the system to do anything useful.

This document is explicit about how.

---

## v1: operator-seeded wallets

In v1, an operator can seed a wallet with starting funds using the dashboard. The action is called `seed_wallet` and is **only enabled in dev/staging environments**.

The mechanism:

1. Operator opens the dashboard, navigates to a wallet's detail page.
2. If `ALLOW_WALLET_SEEDING` is true (dev/staging), the "Seed Wallet" action is visible.
3. Operator submits an amount and a reason (free-text justification).
4. The system writes:
    - An `operator.seeded_wallet` event with the operator's identity, amount, and reason.
    - A ledger entry attributed to the seed: `(wallet_id, +amount, balance_after, saga_id='SEED_<run_id>', step_name='seed', event_id=...)`.
5. The wallet's balance is now non-zero. Transfers can proceed.

The `saga_id = 'SEED_*'` prefix is the audit signal. Reconciliation, audit queries, and operator inspections all recognize this as "not a normal saga" — it's an operator-introduced credit, intended only for testing purposes.

The `ALLOW_WALLET_SEEDING` flag is a deployment-time configuration. Production environments have it set to `false`; the dashboard hides the action; the API returns 403 if called directly. There is no way to invoke wallet seeding in production v1.

---

## v2: real funding sources

v2 replaces operator seeding with real integrations:

- **Bank deposit** (incoming wire to the platform's bank account → credit to a merchant's operational wallet).
- **Card top-up** (merchant or their customer charges a card → credit to a wallet).
- **Inter-platform transfer** (another payment system sends RRQ funds → credit).

Each integration becomes a saga, with the external system as a participant. The Validate step verifies the source has actually arrived (e.g., the bank confirms receipt of the wire); the Credit step writes the ledger entry; the Notify step tells the merchant.

The withdrawal direction works the same way in reverse:

- **Bank withdrawal** (merchant requests payout → debit from operational wallet, initiate bank transfer, mark complete when bank confirms).
- **Card refund** (rare; same idea).
- **Inter-platform transfer out** (sending RRQ funds to another payment system).

Each is a saga with the relevant external party as a step. Reconciliation extends to verify that "what we have in our wallets" agrees with "what we have at the bank" — the cross-system reconciliation problem.

This is real production payment infrastructure. v2 (or v3, depending on scope) territory. The architecture supports it; the implementation is the work.

---

## Why the funding model is explicit

Three reasons it deserves its own appendix:

**1. It's the most common reviewer question.** "How does money get into this system?" If the answer is hand-wavy, the system feels incomplete. The explicit answer ("operator seeding for v1; real integrations for v2, designed but not built") gives a defensible position.

**2. It's the boundary between RRQ and external systems.** Everything else in RRQ is internal — transfers within the system. The funding model is the only place RRQ touches (or eventually will touch) the outside world. Boundaries deserve explicit design.

**3. It catches a class of bugs early.** A common mistake in payment-system design: treating wallets as if they're isolated from the external world, then realizing late that "balance from nowhere" can't exist and the design needs reworking. By being explicit about the model from the start, the system avoids the late-cycle realization.

---

## Edge cases the model addresses

**A test environment is migrated to staging accidentally.** The seeded balances flow with the data. The reconciliation would NOT flag this as a discrepancy (the seed events are real events; the ledger entries are real). The audit log shows the seeds. This is a *correct* result; a human reviewing would see the seeds and recognize the environmental confusion.

**An attacker discovers the seed endpoint in production.** Cannot. The feature flag is read at startup; the dashboard doesn't render the action; the API endpoint returns 403. No bypass.

**A bug allows seeding in production.** This would be a code review failure. The defense in depth: the audit event for every seed includes the environment name from a runtime check; a seed event in production would be immediately visible in dashboards. Still bad, but visible.

**A merchant complains about an unexplained credit on their wallet.** Operator looks at the wallet's events; sees `operator.seeded_wallet` with the seeding operator's identity and reason. Either explains it or escalates.

---

## The v1 testing implication

For developing and demoing RRQ, you'll regularly use the seed action. A typical demo setup:

1. Create three test merchants via the dashboard.
2. Create one operational wallet per merchant.
3. Seed each operational wallet with ₦10,000,000 (or equivalent). Reason: "demo setup".
4. The system is now ready to demonstrate transfers between merchants.

This is a 5-minute setup that everyone runs once. The state persists in the dev database; subsequent demos don't need to repeat the setup.

For load tests, the seed scripts in `scripts/seed/` create a larger volume of test data automatically. The seeding still goes through the same `operator.seeded_wallet` events, just at scale.

---

## What's deliberately not in v1

- **Customer-funded wallets via card.** Real card integration is hard (PCI-DSS, tokenization, chargebacks). v2.
- **Bank deposits.** Requires bank API integration. v2.
- **Cryptocurrency on-ramps.** Not even in the v2 roadmap; would be its own product.
- **Stablecoin reserves.** Same.
- **Cross-currency funding.** Tied to FX (`31-FX-SETTLEMENT.md`). v2.

The current scope is intentional: RRQ v1 is about *the ledger and movement engine*, not the funding rails. Funding is a separable problem with its own engineering scope.

---

## Where to read next

- The merchant and wallet lifecycle that uses this model → [`../services/16-MERCHANT-WALLET-LIFECYCLE.md`](../services/16-MERCHANT-WALLET-LIFECYCLE.md)
- The reconciliation that knows about seed entries → [`../services/14-RECONCILIATION.md`](../services/14-RECONCILIATION.md)
- v2 FX work that's adjacent → [`../deferred/31-FX-SETTLEMENT.md`](../deferred/31-FX-SETTLEMENT.md)

---

*Pass 5 addition. Makes explicit a model the original docs left implicit.*
