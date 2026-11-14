# 2026 Template Ownership and Rate Limits Against SMS OTP Abuse and Toll Fraud

Prevent SMS OTP abuse and toll fraud during SaaS signup by keeping verification templates under application control, enforcing rate limits before dispatch, and blocking destinations outside the approved country allowlist.

Short answer: prevent SMS OTP abuse and toll fraud by making message admission an application decision: enforce destination country allowlists and layered rate limits before dispatch, bind retries to an immutable verification intent, and reconcile each attempted send against that intent. For a B2B SaaS serving US and EU customers, template ownership is the practical control point: the approved template, destination policy, and retry behavior must describe the same operation.

This decision record takes a transaction-accounting angle. Its unit of analysis is a verification intent and the outbound attempts attributable to it, rather than a comparison of messaging products. The concrete job is delivering a verification link during account signup; an SMS OTP used for a later login or 2FA challenge is a separate purpose, with separate admission decisions. Do not quietly turn an email-link resend into an SMS challenge.

## How does template ownership help prevent SMS OTP abuse and toll fraud?

The proposed decision is application ownership of template identity, approved versions, and purpose bindings, while permitting a delivery service to store or render an approved artifact. Ownership here means authority to change behavior. It does not require the application to operate its own mail or messaging infrastructure.

For example, a signup intent for the fictional tenant `tenant_orbit` might identify purpose `signup_link`, template version `signup-v3`, and channel `email`. A login challenge for that tenant would receive a different intent and purpose. These are illustrative application identifiers, not public API fields. An operator editing a template must not thereby acquire authority to change channels, choose destinations, extend challenge validity, or initiate a new send; those actions belong to the application policy and audit trail.

Content matters here. A template release that introduces an SMS fallback changes the set of billable actions even if the signup screen looks identical. Treat that release as a behavioral change requiring review of admission controls, rather than approving only the wording.

Ownership has consequences.

Pin the version.

For this design, the application records the approved template version when it creates an intent, and resends retain that binding unless an explicit migration creates a replacement intent. That choice costs deployment coordination, but it allows an investigator to reconstruct what the application intended to deliver without relying on the template currently visible in an editor.

## Invariants and failure boundaries

There are four invariants in this proposal: a message has one declared purpose; a destination is authorized before dispatch; a repeated request does not silently create another logical send; and an uncertain delivery attempt remains visible until reconciliation resolves it. These are design requirements, not claims that a transport provides exactly-once delivery.

The exactly-once mindset belongs at the business boundary. A successful account verification should produce one state transition even if a recipient receives two messages or opens the same link repeatedly. Challenge redemption therefore needs an atomic transition against the intended account and purpose. Outbound message deduplication is a separate problem.

For SMS, evaluate the normalized destination number against an explicit country allowlist maintained for the supported customer population. “US and EU” is a business scope, not a sufficient machine-readable destination policy: enumerate supported destination countries, review exceptions, and retain the policy version used for each decision. Request IP geography may contribute a risk signal, but this design never lets it substitute for destination eligibility.

Use several admission budgets together: destination, account or signup intent, tenant, and total outbound volume, with an additional origin-based limit where appropriate. Illustrative starting policies might allow 3 attempts per intent within 15 minutes and impose a separately chosen tenant ceiling; these numbers are design examples, not validated production thresholds. Tune them using legitimate signup completion and rejected-request evidence. Shared business networks make an IP-only policy a particularly blunt instrument.

The expensive mistake is to check a counter, dispatch, and increment afterward. Concurrent requests can all pass that check. Instead, atomically reserve the applicable budgets when admitting a send, and ensure every dispatch path, including operator-triggered resends, crosses that boundary. The application should fail closed for paid dispatch when it cannot establish admission, while returning an appropriate retry experience to legitimate users.

A timeout proves uncertainty.

Keep its evidence.

If a dispatcher loses the response after handing off a message, do not automatically create another attempt with a fresh identity. Persist the ambiguous state, inspect available delivery evidence, and apply a documented retry policy. An audit trail should distinguish intent creation, admission, dispatch, transport acknowledgment, and challenge redemption; collapsing them into “sent” makes both abuse review and customer support less reliable.

## Options and their ownership trade-offs

The comparison below concerns implementation choices within this proposed system. It makes no assertion about a particular provider's feature set.

| Template arrangement | Change authority | Abuse-control consequence | Accepted trade-off |
| --- | --- | --- | --- |
| Application-owned, versioned artifact | Application release process approves content and purpose | Admission can bind the template version to the verification intent | Content changes require release coordination |
| Remotely stored artifact pinned to an approved version | Content editor proposes changes; application approves each usable version | Safe only if dispatch resolves the approved artifact and keeps admission in the application | Requires version synchronization and review tooling |
| Mutable remote template selected at dispatch | Content editor can alter the active artifact immediately | Reconstruction requires an independent content snapshot; purpose changes need separate safeguards | Faster editorial changes, more reconstruction work |

The first arrangement is the selected baseline for this signup workflow. The second is acceptable when the team can enforce version approval and preserve the exact rendered artifact or an appropriate reproducible record. Storage location alone does not settle the decision.

Operational effort matters more here than a unit-price comparison. Every extra delivery attempt consumes a budget and creates evidence that somebody must reconcile; every additional template editing surface creates another place where a release can change recipient instructions. A small team may reasonably accept slower content updates to keep that review path understandable.

## The critical path reserves one durable send intent

The Go example expresses the admission boundary as an application contract. It deliberately leaves database and transport implementation behind interfaces: the important guarantee is that admission and durable dispatch work are committed together, rather than a network call being placed inside a transaction. It is a design sketch, not a complete verification service.

```go
package verification

import (
    "context"
    "errors"
)

type Intent struct {
    TenantID        string
    IntentID        string
    Purpose         string
    Channel         string
    DestinationRef  string
    TemplateVersion string
    PolicyVersion   string
}

// The intake service constructs Intent from trusted persisted state.
// DestinationRef identifies a protected, normalized destination.
type AdmissionResult struct {
    AttemptID string
    Existing  bool
}

type AdmissionStore interface {
    // AdmitAndEnqueue commits one transaction that:
    // 1. Resolves IntentID within TenantID and verifies all bindings.
    // 2. Returns the prior result for the same logical send key.
    // 3. Checks expiry, purpose, destination eligibility, and budgets.
    // 4. Reserves budgets and inserts an audit event plus outbox work.
    // A reused key with different bindings must be rejected.
    AdmitAndEnqueue(
        context.Context, Intent, string,
    ) (AdmissionResult, error)
}

func QueueSignupLink(
    ctx context.Context,
    store AdmissionStore,
    intent Intent,
) (AdmissionResult, error) {
    if intent.TenantID == "" || intent.IntentID == "" ||
        intent.DestinationRef == "" ||
        intent.TemplateVersion == "" || intent.PolicyVersion == "" {
        return AdmissionResult{}, errors.New("incomplete intent")
    }
    if intent.Purpose != "signup_link" || intent.Channel != "email" {
        return AdmissionResult{}, errors.New("unexpected purpose or channel")
    }

    // A deliberate resend needs a separately admitted logical send key.
    return store.AdmitAndEnqueue(ctx, intent, "initial")
}
```

The durable outbox worker consumes admitted work; it must not manufacture new admission identities when delivery is slow. Its credentials and interfaces should permit only the intended dispatch path, and changes to budgets or destination rules should generate their own audit events. For an SMS OTP workflow, use a distinct purpose-specific entry point whose transaction enforces the SMS destination allowlist and corresponding budgets.

The interface cannot enforce those properties by itself. A database implementation must serialize competing admissions across every applicable budget and enforce uniqueness for the tenant, intent, and logical send key. A duplicate request should recover the original result even when the original request consumed the last available budget; checking exhausted counters before recognizing a duplicate would incorrectly turn a successful retry into a denial. Consider an illustrative tenant with one remaining admission: two simultaneous requests for the same initial send should resolve to the same durable attempt, while a request for a different intent must compete for that remaining admission. The transaction must make both decisions consistent. Otherwise, a uniqueness constraint on the send record can prevent duplicate rows while still allowing the budget accounting to drift, leaving an apparently tidy audit table that does not explain the remaining allowance.

Exercise that boundary before deployment. Submit concurrent requests with the same key, reuse a key with altered bindings, exhaust one tenant's allowance while another remains eligible, and simulate a worker losing its acknowledgment. Also test a template release while an older intent is still pending. Expected outcomes should include one durable initial-send record, explicit rejection of conflicting bindings, and retained evidence for uncertain dispatch.

Observe admitted attempts, denied attempts by reason, pending work age, and unresolved delivery states alongside signup completion. A falling send count alone does not establish that the control is effective; it could indicate that legitimate customers are blocked. Preserve correlation identifiers without copying live verification links, OTP values, or full destination data into ordinary logs, and define retention and access rules for the more sensitive evidence store.

## Rejected option and the limits of this decision

The rejected baseline is a mutable template editor that can independently trigger signup verification messages. It divides authority over one security-sensitive operation between content changes and application state, making the original recipient instructions harder to reconstruct. Its valid use case is a reviewed, non-authentication notification workflow where independent editing is a deliberate requirement and dispatch remains subject to appropriate controls.

Country eligibility is also not a compliance finding. The CTIA messaging material is relevant to messaging practices, but a destination allowlist does not establish consent, permission to process personal data, or satisfaction of every applicable US or EU obligation. Review the actual workflow, recipient population, retention policy, and messaging requirements with the responsible compliance function. Neither template versioning nor an audit table grants legal authorization.

For signup verification, the decision stands: keep purpose and template approval under application authority, reserve each send before dispatch, and reconcile uncertain outcomes. Apply the same accounting discipline to login 2FA while preserving its distinct security purpose. The acceptance criterion is explainable message admission and correct account state, with delivery evidence that supports investigation.

## References

- https://docs.aws.amazon.com/ses/latest/dg/Welcome.html
- https://www.ctia.org/the-wireless-industry/industry-commitments/messaging-interoperability-sms-mms
