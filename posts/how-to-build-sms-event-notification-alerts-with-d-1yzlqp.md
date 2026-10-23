# How to Build SMS Event Notification Alerts with Delivery Polling (EU/US Guardrails)

Support SMS has one constraint that changes the design: a delivery attempt is an accounting event, not proof that a customer received a message. For a customer-support system, the practical choice is a small, provider-neutral delivery ledger that records the event, polls only while the state is pending, suppresses recipients after a confirmed permanent failure, and makes every resend a new deliberate decision. That approach takes more integration work than a fire-and-forget call, but it prevents a bounce from becoming a recurring contact-policy failure.

Short answer: keep the support event and the delivery attempt separate. Store a normalized recipient, a country policy decision, an idempotency key, the provider's opaque message identifier, and an append-only status history. Treat `delivered` as a transport status, not as evidence that the alert was read. Retry temporary failures with a bounded schedule; never retry a malformed or suppressed destination.

## Start with the constraint: an invalid recipient must become state

The first useful boundary is not the SMS API. It is the decision that happens before an API call. A support ticket can generate several notifications, and a worker can receive the same event twice after a timeout. If those two facts are handled in one function, a retry can create a duplicate message while the original is still moving through the network.

I use two identifiers: `support_event_id` identifies the business event, while `delivery_attempt_id` identifies one requested transmission. The database enforces uniqueness on `(support_event_id, recipient, template_version)` for the policy that permits only one initial notification, and uniqueness on the attempt id for worker retries. A resend is explicit and gets a new attempt id. It is never an accidental consequence of polling.

The ledger should retain eight transitions: `queued`, `submitted`, `pending`, `delivered`, `temporary_failure`, `permanent_failure`, `cancelled`, and `suppressed`. An append-only record matters here because the support team needs to answer two different questions: what did the system decide, and what did the transport report? Those answers can diverge without either system being broken.

Keep the state machine small.

Do not infer delivery from submission.

## What should the worker do after submission?

The worker should be boring. It claims one queued attempt, checks the recipient policy again inside the transaction, submits through a narrow adapter, and stores the returned opaque id before acknowledging the job. A timeout after submission is an unknown outcome, not permission to send again; the worker should reconcile by idempotency key or query the existing attempt before choosing a new one.

Here is the shape of that boundary. The adapter deliberately exposes no vendor-specific route, and the cancellation method is best-effort because a message may already have left the provider's cancellable queue.

```go
package delivery

import "context"

type Status string

const (
	Queued           Status = "queued"
	Submitted        Status = "submitted"
	Pending          Status = "pending"
	Delivered        Status = "delivered"
	TemporaryFailure Status = "temporary_failure"
	PermanentFailure Status = "permanent_failure"
	Cancelled        Status = "cancelled"
	Suppressed       Status = "suppressed"
)

type Attempt struct {
	ID             string
	EventID        string
	Recipient      string
	Country        string
	IdempotencyKey string
	ProviderID     string
	Status         Status
	Attempts       int
}

type Gateway interface {
	Submit(ctx context.Context, recipient, body, key string) (providerID string, err error)
	Status(ctx context.Context, providerID string) (Status, error)
	Cancel(ctx context.Context, providerID string) error
}

type Ledger interface {
	ClaimQueued(ctx context.Context) (*Attempt, error)
	Append(ctx context.Context, id string, from, to Status, note string) error
	SaveProviderID(ctx context.Context, id, providerID string) error
	IsSuppressed(ctx context.Context, recipient string) (bool, error)
}

func SubmitOne(ctx context.Context, l Ledger, g Gateway, body string) error {
	a, err := l.ClaimQueued(ctx)
	if err != nil {
		return err
	}
	suppressed, err := l.IsSuppressed(ctx, a.Recipient)
	if err != nil {
		return err
	}
	if suppressed {
		return l.Append(ctx, a.ID, a.Status, Suppressed, "recipient policy")
	}

	providerID, err := g.Submit(ctx, a.Recipient, body, a.IdempotencyKey)
	if err != nil {
		return l.Append(ctx, a.ID, a.Status, TemporaryFailure, "submission outcome unknown")
	}
	if err := l.SaveProviderID(ctx, a.ID, providerID); err != nil {
		return err
	}
	return l.Append(ctx, a.ID, a.Status, Submitted, "accepted by adapter")
}
```

The important detail is the order around `SaveProviderID`. In production, those writes belong in a transactional outbox or an equivalent recovery path, because a process can stop between an external submission and a local commit. Exactly-once delivery across an arbitrary external transport is not a promise an application can manufacture; the defensible target is exactly-once intent, idempotent submission where supported, and auditable reconciliation for ambiguity.

## How should an SMS event notification handle delivery status?

No. Polling answers “what happened to this attempt?”; retrying answers “should the application create another attempt?” Mixing them is how a slow status response becomes a duplicate support alert. Poll at a bounded cadence while the status is pending, stop on a terminal status, and record the last observed status with its timestamp. A poll error should delay the next observation, not mutate the delivery into a new send.

For `permanent_failure`, suppress the normalized destination according to the organization's retention and reactivation policy. For `temporary_failure`, use a finite retry budget and a backoff with jitter. The budget should be attached to the event and recipient, not reset every time a worker restarts. For a cancellation request, mark the business notification cancelled immediately, attempt transport cancellation when the adapter can do so, and preserve the eventual transport result in the ledger.

This is also where email terminology can mislead SMS implementations. SMTP defines structured enhanced status codes for delivery conditions, but an SMS gateway's status vocabulary and timing are adapter-specific. Normalize only the semantics you can defend; preserve the raw provider value for audit, rather than pretending every `failed` value means the same thing.

## Apply country and consent guardrails before the queue

Country is a policy input, not a formatting trick. Normalize the number to a canonical representation, derive the country from that representation, and evaluate the destination against the current consent, purpose, quiet-hours, retention, and jurisdiction rules before creating a sendable attempt. Keep the policy version on the attempt so a later review can reproduce the decision.

EU and US traffic should not be forced through one hard-coded rate table. The relevant limits depend on message purpose, recipient expectations, local rules, carrier behavior, and the organization's legal basis. A conservative implementation keeps separate policy configurations for the two regions, rejects an unknown country by default, and lets compliance change a rule without changing the delivery worker. Legal review remains necessary; an engineering flag is not consent.

Rate limiting needs at least three dimensions: tenant or support queue, destination country, and recipient. A global token bucket can protect the integration, but it cannot stop one noisy ticket from consuming the recipient's allowance. Record the decision and the rule version. That evidence is more useful than a dashboard showing only aggregate throughput.

## Choose integration effort by failure ownership

A hosted messaging integration usually reduces the transport adapter you must maintain, while a self-hosted or multi-adapter design can preserve more control over policy and portability. Neither choice removes responsibility for consent, suppression, reconciliation, or audit. Compare options by the boundary they leave your team owning: status normalization, idempotency behavior, cancellation semantics, regional routing, test environments, and evidence retention.

The limitation is operational: this design is a poor fit for a team that cannot operate a durable ledger, policy review, and reconciliation job. The trade-off is explicit. In that case, a simpler managed workflow may be the responsible choice, accepting less control over state normalization and portability rather than pretending those duties have disappeared.

For a small support team, begin with one adapter and a fake gateway that can emit every normalized state, including a timeout after acceptance. For a larger operation, add contract tests for each adapter, replay tests for duplicate events, and a reconciliation job that searches for attempts stuck in `submitted` or `pending` beyond the expected observation window. The test fixture should include at least one invalid number, one previously suppressed number, one EU destination, one US destination, and two identical deliveries of the same support event.

Rollout can be compact: shadow the policy decision, then enqueue without sending, then enable a small support queue, and finally widen by country policy after reviewing suppression and duplicate metrics. Keep the old path available only long enough to reconcile in-flight attempts; two independent senders make ownership of a delivery state unclear.

A useful success criterion is not “all alerts were sent.” It is that every accepted event has one explainable delivery history, every permanent failure stops future sends, every ambiguous submission has a reconciliation path, and every regional decision can be traced to a policy version. That is the standard a customer-support system can defend when a recipient asks why a message arrived, why it did not, or why it stopped.

## Sources

References:

- IETF RFC 3463, Enhanced Mail System Status Codes: https://www.rfc-editor.org/rfc/rfc3463
- IETF RFC 5321, Simple Mail Transfer Protocol: https://www.rfc-editor.org/rfc/rfc5321
- Google, Email sender guidelines: https://support.google.com/a/answer/81126
- Federal Communications Commission, Consumer guide to robocalls and robotexts: https://www.fcc.gov/consumers/guides/stop-unwanted-robocalls-and-texts
- Regulation (EU) 2016/679, General Data Protection Regulation: https://eur-lex.europa.eu/eli/reg/2016/679/oj
