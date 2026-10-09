# Publish Per Key API Spend Analytics Events Within Customer Support Ceilings

A customer-support system with a hard spend ceiling cannot treat analytics delivery as part of the request path: a delayed dashboard must not delay a reply to a customer, and an unavailable analytics sink must not erase billable usage. The practical choice is to record immutable usage locally, price it from a versioned rate table, and publish exactly one idempotent event for each key and billing period from an asynchronous rollup worker.

**Short answer:** make `(key_ref, period_start)` the aggregation identity and the analytics event's idempotency key. Atomically close a period before publishing it, retain the closed rollup in an outbox until the sink acknowledges it, and distinguish accepted spend from refused traffic. That last distinction is the control surface: a hard ceiling favors predictable spend but can refuse support traffic, while a soft ceiling preserves traffic at the cost of overshoot during delayed reconciliation.

## How should you publish per-key API spend to analytics?

The event should mean that one defined accounting period for one pseudonymous credential has reached a particular revision, not that a timer happened to run. Timers are triggers; they are poor identities. A retry, a worker restart, or two schedulers racing must converge on the same event identity.

Retries happen.

For a customer-support workload, the useful grain is usually the credential that maps to a customer account or an internal support automation, provided that this mapping does not place the raw secret in analytics. Use a stable internal key reference, such as an HMAC-derived label with a separately protected secret, and keep the credential itself in the secrets system. OWASP's Secrets Management Cheat Sheet recommends centralized management, access control, rotation, expiration, and auditability across a secret's lifecycle. An analytics warehouse is not a secrets manager.

The payload needs enough accounting context to be replayed and challenged later:

| Field | Purpose |
| --- | --- |
| `event_id` | Deterministic identity for one key, period, and revision |
| `key_ref` | Pseudonymous attribution key; never the API credential |
| `period_start`, `period_end` | Half-open UTC interval `[start, end)` |
| `accepted_units`, `accepted_cost_micros` | Usage admitted and priced in this period |
| `refused_requests` | Traffic rejected by the ceiling decision |
| `currency` | Explicit unit for the integer cost |
| `rate_version` | Audit link to the pricing inputs |
| `revision` | Monotonic correction number, initially `1` |

Integer micros avoid floating-point arithmetic in the accounting path. The exact scale is an internal contract, not a claim about any provider's price. Likewise, a fifteen-minute period in the example below is an operating choice: shorter periods improve the freshness of enforcement and increase event volume; longer periods reduce churn and permit a larger gap between actual and observed spend.

## Derive the boundary before writing the publisher

Start with the invariant that each accepted request creates one durable usage record with its own idempotency key. The API handler writes that record in the same local transactional boundary as the decision to accept the work, or it refuses the request. It does not call the analytics service synchronously.

No acknowledgment, no deletion.

This is where an exactly-once mindset matters, although the transport will still be at least once. Exactly-once publication is an end-to-end property built from a unique producer identity and idempotent consumption; no retry loop can manufacture it by itself. A unique constraint on the usage record prevents a client retry from charging twice. A unique constraint on `(key_ref, period_start, revision)` prevents two rollup workers from closing the same revision. Finally, the analytics consumer upserts on `event_id` rather than appending blindly.

Keep the evidence. Deleting raw usage immediately after a rollup makes a compact dashboard, but it destroys the ability to reconcile a disputed invoice or reprice a period after a rate-table correction. Retention must follow the organization's contractual, privacy, and compliance limits; no universal duration can be inferred from this design. The audit record should capture who changed a rate version, when it became effective, and which rollups used it.

The following Go example focuses on deterministic construction. Repository methods stand for transactional storage; the sender stands for any analytics destination that supports a stable event identifier.

```go
package metering

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

type Rollup struct {
	KeyRef            string
	PeriodStart       time.Time
	PeriodEnd         time.Time
	AcceptedUnits     int64
	AcceptedCostMicros int64
	RefusedRequests   int64
	Currency          string
	RateVersion       string
	Revision          int64
}

type Event struct {
	ID                 string    `json:"event_id"`
	KeyRef             string    `json:"key_ref"`
	PeriodStart        time.Time `json:"period_start"`
	PeriodEnd          time.Time `json:"period_end"`
	AcceptedUnits      int64     `json:"accepted_units"`
	AcceptedCostMicros int64     `json:"accepted_cost_micros"`
	RefusedRequests    int64     `json:"refused_requests"`
	Currency           string    `json:"currency"`
	RateVersion        string    `json:"rate_version"`
	Revision           int64     `json:"revision"`
}

type Repository interface {
	ClosePeriod(context.Context, string, time.Time, time.Time) (Rollup, error)
	MarkPublished(context.Context, string) error
}

type Sender interface {
	Publish(context.Context, Event) error
}

func eventID(r Rollup) string {
	identity := fmt.Sprintf("%s|%s|%d", r.KeyRef,
		r.PeriodStart.UTC().Format(time.RFC3339), r.Revision)
	sum := sha256.Sum256([]byte(identity))
	return hex.EncodeToString(sum[:])
}

func PublishPeriod(ctx context.Context, repo Repository, out Sender, keyRef string, start time.Time) error {
	start = start.UTC().Truncate(15 * time.Minute)
	rollup, err := repo.ClosePeriod(ctx, keyRef, start, start.Add(15*time.Minute))
	if err != nil {
		return fmt.Errorf("close period: %w", err)
	}

	event := Event{
		ID: eventID(rollup), KeyRef: rollup.KeyRef,
		PeriodStart: rollup.PeriodStart, PeriodEnd: rollup.PeriodEnd,
		AcceptedUnits: rollup.AcceptedUnits,
		AcceptedCostMicros: rollup.AcceptedCostMicros,
		RefusedRequests: rollup.RefusedRequests,
		Currency: rollup.Currency, RateVersion: rollup.RateVersion,
		Revision: rollup.Revision,
	}
	if err := out.Publish(ctx, event); err != nil {
		return fmt.Errorf("publish %s: %w", event.ID, err)
	}
	return repo.MarkPublished(ctx, event.ID)
}
```

There is a deliberate crash window between `Publish` and `MarkPublished`. After a restart, the worker sends the same `event_id` again. That is acceptable only if the destination treats that identifier idempotently. If it cannot, place a consumer under your control in front of it and deduplicate there; pretending the window does not exist turns a retry policy into an invoice defect.

## The ceiling is an accounting policy, not an alert

A spend alert observes a result after some delay. A hard ceiling makes an admission decision before work begins, so it needs a reservation ledger: reserve the maximum accountable amount, perform the request, then settle the reservation against actual metered units. Expired reservations require an explicit release rule and an audit entry. Without reservations, concurrent support requests can each see remaining budget and collectively cross the ceiling.

The decision cannot be hidden in the publisher. Publication is asynchronous by design, whereas admission needs the freshest authoritative balance available to the request path. Analytics receives the consequence of the decision: accepted usage, refused request count, and the policy or rate version needed for later explanation.

This produces an uncomfortable but honest choice. **A hard ceiling protects the spending bound by refusing customer-support traffic when no balance can be reserved.** A soft ceiling can queue or accept that traffic, but then the bound is advisory. Teams should set this policy per workload: an automated enrichment step may be safely skipped, while the channel that receives a customer's urgent support message may require a separately funded reserve or a degraded path that does not incur metered work.

This architecture has a clear limitation: it is a poor fit when request handling and usage recording cannot share a durable transaction, and it is needless machinery when approximate daily reporting is sufficient. In the first case, use a reservation service that owns admission and emits durable usage before dispatch; in the second, a scheduled database query with a uniqueness constraint may be easier to operate. The trade-off is explicit. More components buy replayable evidence and tighter enforcement, while also adding schema governance, outbox monitoring, correction handling, and an on-call burden.

Do not price from whatever rate table happens to be current when the rollup job runs. Select the rate version effective when usage was accepted, store that reference with the usage record, and make corrections additive through a higher revision. Overwriting revision `1` erases the reason an invoice changed.

## Failure modes worth testing

The useful tests are transition tests, not snapshots of a happy-path payload. Run two workers against the same closed period and verify one logical event. Crash after the destination accepts an event but before the outbox is marked, then verify the retry changes no totals. Move a request across a UTC boundary and verify that it belongs to exactly one half-open interval. Rotate a credential and verify that the old and new key references remain auditable without exposing either secret.

Also test the control plane under pressure:

- Submit concurrent reservations just below the remaining ceiling; accepted reservations must never exceed the authoritative balance.
- Delay the analytics destination for several periods; admission and local usage capture must continue according to policy, while outbox age becomes visible.
- Change a rate version at a period boundary; every usage record must resolve to one version.
- Send a late usage record after closure; either issue revision `2` or place it in a documented exception queue, never mutate history silently.
- Replay every pending outbox row; warehouse totals must remain unchanged.

Observe three different lags: time from request to durable usage, time from period close to outbox creation, and time from outbox creation to acknowledged publication. A single generic “pipeline latency” metric cannot tell an ingestion failure from an analytics outage. Pair those lags with outbox depth, oldest unpublished age, duplicate attempts, refused requests, and unresolved reservations. Avoid attaching raw key references as unbounded metric labels; logs and the analytics event can carry controlled identifiers, while operational metrics use bounded dimensions.

## Roll out without changing the invoice first

Begin in shadow mode. Produce rollups and reconcile their accepted units and integer costs against the existing invoice inputs, but do not let the new balance refuse traffic. Investigate every unexplained difference; an aggregate that “mostly matches” is not an accounting control.

Next, enable idempotent publication for a small set of internal keys, exercise replay and correction revisions, and record evidence that secret rotation does not break attribution. Only then enable reservations and refusal for a bounded customer cohort with an explicit degraded-service rule. The rollback is to disable enforcement, not to discard usage or audit rows.

The final acceptance criterion is compact: for every invoiced key-period, one current revision in analytics reconciles to immutable usage, every retry converges on the same identity, and every refused request has a ceiling decision that an operator can explain. **The dashboard is a projection; the ledger remains the authority.**

## Sources

- OWASP, “Secrets Management Cheat Sheet”: https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
