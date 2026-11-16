# Password Reset Email 400 Bad Request: A Guide to 3 Suppression Boundaries

Keep sender configuration failures separate from recipient suppression when a customer support workflow resends a password reset email. The deciding constraint is integration effort: support needs a small, auditable decision model that can distinguish an invalid request from an invalid destination without requiring an agent to interpret raw delivery events.

Short answer: an HTTP 400 response means the server considers the request invalid; it does not, by itself, establish that the recipient address is invalid. If the API reports an unverified From domain, inspect sender authorization and the requested address before retrying. Treat DKIM verification as sender configuration evidence, and process subsequent permanent recipient failures through a separate suppression path. A failed sender check must never suppress a customer.

This engineering decision record uses three boundaries: request admission, message acceptance, and recipient feedback. The proposed implementation keeps a local audit trail across them, so support can answer both “Why was this reset rejected?” and “Why are we withholding another attempt?” without confusing those questions.

## Why does a password reset email API return 400 Bad Request?

HTTP defines 400 Bad Request broadly as a refusal to process a request because of a perceived client error. It does not define a universal email API error taxonomy. An invalid From address, an unverified sending domain, or a malformed field therefore requires interpretation through the responding API's documented error details; status alone cannot distinguish them. Preserve a sanitized error code, correlation identifier, and configuration revision in the audit record. Do not turn the entire response body into an unrestricted support log.

Start with the exact From mailbox in the serialized request. Determine which account or environment authorizes it, then compare its domain with the domain whose verification was actually completed. A verification result for a different domain or environment is insufficient evidence. Check the published DKIM selector and public key against the configuration requested by the sending service; RFC 6376 describes signatures and key discovery, while the service defines its own onboarding and admission requirements.

DKIM is narrower than many troubleshooting screens suggest. It authenticates a signing domain and the covered message content; it does not certify that a recipient exists. DMARC adds alignment rules involving the visible From domain and authenticated identifiers. These are separate checks, and passing one is not a general delivery guarantee.

Keep that distinction.

For support, expose an actionable internal reason such as “sender configuration requires review,” while retaining a consistent public reset response. The OWASP forgot-password guidance recommends consistent responses for existing and nonexistent accounts. A detailed public rejection can undo that protection even when the email pipeline itself is correct.

## Invariants and failure boundaries

The principal invariant is that only evidence about a destination may change that destination's suppression state. Request validation errors belong to the sending configuration or request attempt. They must not become a customer-level delivery verdict, even if the rejected request happened to contain that customer's address.

Acceptance is another boundary. An API acknowledging a message is evidence of submission under its contract, not proof of arrival in an inbox. Store its message identifier and associate it with the reset attempt, but leave delivery state pending until the integration supplies a relevant outcome. If an acceptance response is lost, the sender faces an ambiguous result: blindly creating a new attempt can produce another message. Preserve the original logical attempt identifier and use documented idempotency support where it exists; otherwise record the uncertainty for reconciliation instead of claiming exactly-once delivery.

Recipient feedback needs finer classification than “bounce.” SMTP distinguishes transient failures from permanent failures, and enhanced status codes provide additional detail. RFC 3463's 5.1.1 denotes a bad destination mailbox address; a permanent policy rejection is a different kind of evidence and should not automatically establish that the mailbox is nonexistent. For this design, an authenticated, correlated permanent bad-address outcome suppresses further automatic sends to that destination within the relevant tenant. Temporary failures follow a bounded retry policy. Unclassified outcomes remain available for review.

The audit trail is append-only at the application level: each decision records the event identifier, tenant, internal recipient reference, attempt identifier, classified reason, and applicable policy revision. A restricted mapping resolves the recipient reference to its address. Password reset tokens and links do not belong in this trail.

Suppression is not deletion.

Keep a controlled correction path for an address that has changed or a classification that was wrong. Clearing suppression should create another audit event and should require evidence under the account recovery policy; a support agent repeatedly pressing “resend” is not that evidence. This keeps a delivery control from silently becoming an account ownership decision.

## Decision: compare the integration work

The decision is to maintain local suppression state with a small adapter for sender errors and authenticated recipient events. The table compares engineering choices for this support workflow, rather than products or delivery promises.

| Option | Initial integration work | Ongoing obligation | Fit for the decision |
| --- | --- | --- | --- |
| Local state with an event adapter | Correlate attempts, classify outcomes, persist deduplication keys | Reconcile uncertain submissions and review classification rules | Gives support an explicit, auditable reason for withholding a reset |
| Remote suppression state alone | Query or inspect the sending service's documented state | Reconcile that state with account recovery and tenant boundaries | Useful when one integration owns all sends and exposes sufficient evidence |
| Agent review for every failure | Route errors into the support queue | Staff repeated review and record decisions consistently | Suitable for low-volume exceptions, but puts classification on the critical support path |

Local state costs implementation time.

The trade-off is additional storage and reconciliation work in exchange for an explanation that remains available within the recovery workflow. This approach is not suitable for a team that cannot maintain the event adapter or investigate uncertain submissions; remote state with manual review may be a better fit there. Its justification here is the ability to express the invariant once, close to the customer support action, rather than rediscovering the difference between a sender rejection and a bad mailbox in every support ticket. Avoid building a comprehensive delivery platform: the adapter needs only the classifications the recovery workflow can act upon, with an explicit unknown case. Consider a hypothetical sequence in which an attempt receives a sender-domain rejection, a configuration change permits the next attempt, and a later authenticated event reports a nonexistent destination: only the last observation justifies bad-address suppression, while all three observations matter to the audit record. Collapsing that sequence into a single mutable failure field would erase the reason the earlier rejection required configuration repair.

## Critical path: commit suppression with its audit event

The following Go example models the application boundary after an adapter has authenticated the event source, checked its documented payload, and correlated the message to a stored attempt. It deliberately accepts only the adapter's permanent bad-address classification. The identifiers below are internal application fields, not an external API schema.

```go
package recovery

import (
	"context"
	"errors"
)

type Outcome string

const PermanentBadAddress Outcome = "permanent_bad_address"

type RecipientEvent struct {
	TenantID    string
	EventID     string
	AttemptID   string
	RecipientID string
	Outcome     Outcome
}

// ApplyRecipientEvent must atomically deduplicate the event, update
// suppression, and append an audit entry, or commit none of them.
// Its durable deduplication key is (TenantID, EventID).
type Store interface {
	ApplyRecipientEvent(context.Context, RecipientEvent) error
}

func RecordBadAddress(
	ctx context.Context, store Store, event RecipientEvent,
) error {
	if event.TenantID == "" || event.EventID == "" ||
		event.AttemptID == "" || event.RecipientID == "" {
		return errors.New("missing correlation identifiers")
	}
	if event.Outcome != PermanentBadAddress {
		return errors.New("outcome requires a different handler")
	}
	return store.ApplyRecipientEvent(ctx, event)
}
```

The transaction contract does the important work. A durable uniqueness constraint prevents a repeated event from creating another suppression transition; the same transaction appends the audit entry, so a crash cannot commit the customer-visible restriction without its explanation. An in-memory deduplication map cannot provide that contract across process restarts. This is an exactly-once mindset applied to a local state transition, with no claim that the surrounding networks deliver events exactly once.

The send path must also consult suppression before creating an outbound attempt. Define its concurrency boundary explicitly: if a send was already submitted before suppression committed, the local transaction cannot recall it. Test that race and document the permitted outcome. Do not promise that a suppression event can retroactively prevent an accepted submission.

Before deployment, exercise duplicate events, mismatched tenant correlation, temporary failures, sender-domain rejections, and a storage failure during the transaction. The expected results differ: duplicates preserve one transition, tenant mismatches are rejected, temporary failures leave bad-address suppression unchanged, and a failed transaction leaves neither a new suppression state nor its audit entry. Roll out classification with reviewable audit records, then enable automatic suppression once the mappings have been checked.

Monitor rejected submissions separately from recipient failures. A rise in sender configuration errors calls for configuration investigation; a backlog of unprocessed recipient events calls for event-processing investigation. Combining both into a single “email failed” count removes the distinction support needs. Retention and access controls apply to these records too: an audit trail is not permission to retain personal data indefinitely.

## Rejected option and compliance limits

For this workflow, relying exclusively on remote suppression state is rejected because the support action needs a local explanation tied to the account recovery attempt. That option remains valid for a simpler installation where one sending integration owns the complete workflow and its documented state provides adequate correlation and review access. It reduces local persistence work, while leaving the application responsible for understanding the boundary between sender rejection and recipient suppression.

Compliance does not collapse those boundaries either. The FTC distinguishes commercial messages from transactional or relationship messages and explains how primary purpose affects classification; do not treat a password reset's operational purpose as permission to add promotional content without reviewing the resulting message. Technical sender authentication also does not establish legal compliance. Applicable obligations require their own assessment.

The resulting decision is deliberately limited: repair invalid sender configuration at request admission, preserve uncertain submissions for reconciliation, and suppress invalid recipients only from classified destination evidence. Support gains a defensible reason for each action, and account recovery keeps an auditable boundary between delivery failure and customer identity.

## References

- RFC 9110, HTTP Semantics, including 400 Bad Request: https://www.rfc-editor.org/rfc/rfc9110.html#section-15.5.1
- RFC 6376, DomainKeys Identified Mail signatures: https://www.rfc-editor.org/rfc/rfc6376.html
- RFC 7489, Domain-based Message Authentication, Reporting, and Conformance: https://www.rfc-editor.org/rfc/rfc7489.html
- RFC 5321, Simple Mail Transfer Protocol: https://www.rfc-editor.org/rfc/rfc5321.html
- RFC 3463, Enhanced Mail System Status Codes: https://www.rfc-editor.org/rfc/rfc3463.html
- OWASP Forgot Password Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html
- FTC, CAN-SPAM Act compliance guide for business: https://www.ftc.gov/business-guidance/resources/can-spam-act-compliance-guide-business
