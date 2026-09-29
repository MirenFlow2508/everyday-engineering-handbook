# Implementing 2FA Login SMS OTP API in Node.js — 3 Ownership Controls

TL;DR: The best API boundary for 2FA login SMS and OTP delivery is not the same policy used for a media application that emails generated reports as attachments. Choose by template ownership: keep the reviewed message definition and its version in your system, pass only typed data and an immutable attachment reference to delivery adapters, and record every accepted, canceled, or superseded attempt in Postgres. The dominant cost is usually retention: report bytes multiplied by copies and retention time. Remove duplicate attachment copies first; do not weaken the audit trail.

This also prevents a common category error in app-builder evaluations: a login OTP, a subscription-renewal notice, and a generated media report are three different obligations. An OTP needs short validity and strict attempt semantics; a renewal notice needs a durable business record; a report attachment adds large binary retention and content-governance concerns. One transport abstraction may carry all three, but one retry, cancellation, or retention policy should not.

## What is the bill actually made of?

Retention dominates.

Start with bytes retained, because attachment storage can dwarf the message envelope. For `R` generated reports, average attachment size `A`, retained copies `C`, and retention window `D`, attachment exposure is `R × A × C × D` byte-days. At 100,000 reports per month, 8 MB each, and three retained copies, one monthly cohort represents 2.4 TB before backups or replicas. This is arithmetic, not a benchmark or a storage-price claim.

The change that moves the dominant term is content addressing: retain one immutable report object, store its digest and object key on each delivery attempt, and let retries reference the same bytes. Do not regenerate or upload another attachment merely because transport failed. Envelope rows, status events, and template versions are small enough to keep much longer, and they carry the evidence needed for reconciliation.

| Record | Retention decision | Reason |
|---|---|---|
| Generated report object | Policy-defined, one immutable copy | Largest term; required for attachment retrieval or replay |
| Rendered message body | Keep only if compliance policy requires exact rendered evidence | Can contain personal data and duplicates template output |
| Template version and input digest | Keep with the audit record | Reconstructs what rules and data produced the message |
| Delivery events | Append-only retention | Supports reconciliation, disputes, and duplicate detection |
| OTP secret or clear code | Do not retain after its validity window | It is authentication material, not an audit artifact |

The deliberate loss is byte-for-byte replay after the report object's retention period expires. When an old delivery is disputed, the system can still prove the template version, input digest, attachment digest, recipient reference, and transport outcome, but it cannot reproduce the deleted document. That trade-off must be an explicit records-policy decision, because indefinite attachment retention merely converts operational convenience into continuing data exposure.

## How should an API handle a second 2FA login SMS OTP?

The application should own a versioned subject, body, attachment naming rule, and locale contract whenever editorial review, legal review, and delivery must refer to the same artifact. Provider-hosted templates can be operationally useful, but they move the authoritative version outside the report transaction and make a later adapter change depend on reconstructing remote state. A provider identifier is not a substitute for a locally reviewable template revision.

This is where the three message classes split. The renewal notice should bind a subscription snapshot and effective date to a durable template revision. The media report should additionally bind an attachment digest. The OTP should bind a purpose, recipient, expiry, and attempt lineage, while the code itself remains transient. Cancellation means “do not begin a still-pending attempt”; it cannot mean “recall a message that a downstream carrier or mailbox has already accepted.”

Words matter here.

DKIM adds another ownership boundary. RFC 6376 defines a domain-level signing mechanism in which selected headers and the body are signed, and it explains that verification does not itself prescribe message handling. Signing therefore belongs after final rendering, when the bytes are stable, while the application retains the template revision that explains how those bytes were produced.

## Build one auditable intent boundary

The write path should atomically create the business event and a delivery intent. A worker claims that intent, resolves the immutable report object, renders the locally owned template, and calls a narrow transport interface. It then appends the result. Exactly-once external delivery cannot be inferred from a timeout: the receiver may have accepted the message before the connection failed. The defensible target is one durable intent, idempotent claiming, stable transport keys where supported, and reconciliation for ambiguous outcomes.

There is no shortcut.

The following focused Go example models those controls without embedding a commercial SDK. It also rejects stale cancellation: only a pending intent can move to canceled, while an accepted attempt remains historical evidence.

```go
package delivery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

type Intent struct {
	ID              string
	Kind            string
	RecipientRef    string
	TemplateVersion string
	AttachmentKey   string
	AttachmentSHA256 string
	State           string
	CreatedAt       time.Time
}

type RenderedMessage struct {
	Subject        string
	Body           []byte
	AttachmentKey string
}

type Transport interface {
	Send(ctx context.Context, idempotencyKey string, message RenderedMessage) (receipt string, err error)
}

type Store interface {
	ClaimPending(ctx context.Context, intentID string) (Intent, error)
	AppendAccepted(ctx context.Context, intentID, receipt string, at time.Time) error
	AppendAmbiguous(ctx context.Context, intentID, reason string, at time.Time) error
	CancelPending(ctx context.Context, intentID, reason string, at time.Time) (bool, error)
}

func Digest(report []byte) string {
	sum := sha256.Sum256(report)
	return hex.EncodeToString(sum[:])
}

func Deliver(ctx context.Context, store Store, tx Transport, intentID string, render func(Intent) (RenderedMessage, error)) error {
	intent, err := store.ClaimPending(ctx, intentID)
	if err != nil {
		return fmt.Errorf("claim intent: %w", err)
	}
	message, err := render(intent)
	if err != nil {
		return fmt.Errorf("render template %s: %w", intent.TemplateVersion, err)
	}
	receipt, err := tx.Send(ctx, intent.ID, message)
	if err != nil {
		// A timeout is ambiguous; preserve it for reconciliation instead of blind resend.
		if auditErr := store.AppendAmbiguous(ctx, intent.ID, err.Error(), time.Now().UTC()); auditErr != nil {
			return errors.Join(err, auditErr)
		}
		return err
	}
	return store.AppendAccepted(ctx, intent.ID, receipt, time.Now().UTC())
}
```

In Postgres, `ClaimPending` can use a transaction that locks the intent row and inserts an attempt with a uniqueness constraint on the intended logical delivery key. The exact SQL depends on the queue topology, so the important invariant is more useful than a copied query: two workers must not both acquire permission to originate the same logical attempt. Each state transition needs an actor, timestamp, prior state, reason, and correlation identifier.

## Failure modes at the state boundary

“Resend” is too vague for an API requirement. A retry continues the same intent after a proven pre-acceptance failure. A replacement creates a new intent that supersedes an earlier one, perhaps because the report changed. A user-requested resend may be a new authorized delivery of identical content. Those operations need distinct identifiers even when they reference the same attachment object.

Consider a renewal job that emits a notice and a media report together. The worker records both intents, dispatches the notice, and times out while uploading the report; meanwhile, a user requests cancellation because the subscription state changed. Treating the timeout as failure creates a duplicate if acceptance occurred, while treating cancellation as a recall makes a promise the system cannot enforce. The state machine instead preserves the first attempt as ambiguous, prevents a new claim until reconciliation, records the cancellation request with its actor and reason, and allows policy to decide whether a later replacement notice supersedes the original business event. The same discipline applies to an OTP with a much shorter lifetime, but its expired code must not be revived by a delayed retry. This example does not require a transport-specific feature. It requires application-owned meaning.

Test the ugly boundary. Pause the worker after the transport accepts a message but before the accepted event commits, then restart it. The expected result is an ambiguous attempt awaiting receipt reconciliation, not an automatic second send. Also race cancellation against worker claiming; exactly one transition should win. For OTP traffic, test expiration before dispatch and before verification, and cap attempts according to the application's threat model rather than reusing the report-delivery retry schedule.

Deployment should preserve compatibility across template revisions. Workers must either render every version still present in the queue or reject deployment until old intents drain. Metrics should count pending age, ambiguous outcomes, cancellation races, attachment-digest mismatches, and reconciliation lag by message class; recipient addresses and OTP values do not belong in metric labels or ordinary logs.

Short retention makes incident investigation harder. Keep structured events and hashes after deleting bulky report objects, test deletion as seriously as sending, and document which questions the remaining record can and cannot answer.

Delete deliberately.

## Choose the boundary, then evaluate transports

Evaluate candidates with a contract test, not a feature matrix. The adapter must accept a caller-generated idempotency key, expose an acceptance receipt or a clearly defined failure, preserve the required attachment metadata, and permit status reconciliation. Determine where templates live, who can edit them, how revisions are exported, and whether a cancellation request has a precise state boundary. No transport can guarantee recall after downstream acceptance.

For an AI-assisted app builder, expose this boundary as a small typed tool rather than allowing generated code to compose arbitrary messages or provider parameters. The tool-use guidance in the Further reading section recommends detailed tool descriptions and JSON Schema input definitions; that supports constrained fields such as `intent_id`, `template_version`, and `attachment_key`. Authorization, state validation, and idempotency remain server-side responsibilities.

Three controls decide the architecture: locally governed template revisions, immutable attachment identity, and append-only delivery history. They allow transports to change without changing the meaning of a renewal notice, OTP challenge, or media report. They also make cost reduction honest: duplicate bytes can disappear while the evidence needed to explain a delivery remains.

## References

- RFC 6376, “DomainKeys Identified Mail (DKIM) Signatures”: https://datatracker.ietf.org/doc/html/rfc6376
- Anthropic, “Tool use with Claude”: https://platform.claude.com/docs/en/agents-and-tools/tool-use/overview

## Further reading

- https://datatracker.ietf.org/doc/html/rfc6376
- https://platform.claude.com/docs/en/agents-and-tools/tool-use/overview
