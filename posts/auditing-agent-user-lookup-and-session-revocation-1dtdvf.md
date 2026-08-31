# Auditing Agent User Lookup and Session Revocation (During GDPR Account Deletion)

A GDPR account-deletion request turns a support console into an impersonation risk: an agent must complete user lookup and terminate every session, yet every extra search field or reusable credential gives an abusive operator and an automated bot more leverage.

Short answer: choose an architecture that separates agent identity from customer identity, requires a case-scoped authorization for lookup, creates no reusable customer credential, and executes deletion plus session revocation as one durable, idempotent workflow with an attributable audit record. A console that merely hides an impersonation button is not a security boundary.

The governing invariant is stronger than "the UI says deleted." After the workflow reaches its terminal state, no session issued before the deletion boundary may authorize a customer action; repeated deletion requests must converge on that same state; and an auditor must be able to identify the agent, approved support case, subject account, policy decision, and outcome without retaining the personal data that was supposed to be erased.

This is the hard part.

## How should support agents control user lookup and session impersonation risk?

Start with two principals, not one. The agent is the actor; the customer account is the subject. Any downstream request made through the console should preserve both identifiers, because replacing the actor with the subject destroys attribution and makes ordinary customer traffic indistinguishable from privileged support activity. OWASP recommends reauthentication for sensitive features and contextual authentication decisions; account deletion, session revocation, and any support-assisted access belong in that sensitive category.

Lookup should be a narrow capability granted for a particular support case. Accept exact identifiers such as a verified email address, account ID, or case-bound external reference; return the smallest result needed to disambiguate the account; rate-limit by agent, team, and tenant; and treat repeated misses as a security signal. Prefix search, bulk export, and unrestricted cross-tenant queries don't belong in the same permission. Bot resistance here is mostly about economics and containment: bounded query shapes, low result cardinality, short authorization windows, and alerts tied to a human actor make enumeration slower and conspicuous.

The authorization decision needs more than a role named `support`. It should evaluate the agent's current authentication strength, the case identifier, the requested operation, the customer tenant, and whether a second approver is required by policy. The resulting grant should be short-lived and capability-scoped. If the console offers a customer-view mode, issue a support session carrying `actor_id`, `subject_id`, `case_id`, `purpose`, and explicit allowed actions. Don't mint a normal customer refresh token, copy a cookie, or reveal a password-reset link to the agent. Those shortcuts turn a supervised operation into transferable authority.

I would also make destructive actions impossible from a read-only customer view. The boundary is deliberate: viewing an order to answer a ticket and deleting an identity are different duties, even when the same employee is allowed to initiate both. Your mileage may vary on dual approval for low-risk accounts, but the evidence needed to decide is measurable: privileged-action volume, false-positive rate, account value, and the consequences of a mistaken erasure.

| Control | Abuse it constrains | Evidence to retain |
|---|---|---|
| Exact, case-bound lookup | Enumeration and cross-tenant browsing | Actor, case, query type, result count, decision |
| Step-up before deletion | Stolen or unattended agent sessions | Authentication event reference and policy version |
| Scoped support session | Credential copying and privilege drift | Actor, subject, capability set, expiry |
| Per-principal quotas | Bots using one console account | Allow/deny counts and alert correlation ID |
| Independent approval | Insider misuse and destructive mistakes | Initiator, approver, reason, timestamps |

The audit trail is not permission to keep everything forever. GDPR Article 17 contains exceptions to erasure, including processing needed for a legal obligation or legal claims, so retention requires a documented lawful basis and schedule; it should never become a shadow customer profile. Store stable pseudonymous references where they satisfy the audit purpose, restrict access to the ledger, and make audit records append-only at the application boundary. Compliance counsel must set the actual retention period. Engineering cannot infer it from an authentication design.

Consider a design review using a synthetic account, `acct_7419`, and support case `CASE-48321`. Agent A receives an exact-match lookup grant for that case, sees two non-sensitive disambiguators, and requests erasure; Agent B approves the request after independent step-up authentication. The workflow binds a new operation ID to the actor, approver, subject, case, policy version, and request hash, then commits `access_blocked`. At precisely that point, a bot controlling Agent A's browser tries three moves: reusing the lookup grant against another tenant, creating a new customer-view session for `acct_7419`, and replaying the deletion request with a different subject under the same operation ID. All three must fail for different, auditable reasons, while a legitimate retry carrying the original immutable fields may continue. A customer access token minted one second before the boundary must also lose authorization when checked against account state. The worker can then revoke opaque sessions, advance the revocation epoch used by token validation, erase owned personal data, and mark the operation complete. If the worker restarts between any two transitions, it resumes from the committed record. This example exposes a useful review question that a checkbox cannot answer: exactly which durable write defines the instant after which neither customer credentials nor delegated support authority can act for the subject?

Access stops first.

## Make revocation part of the deletion state machine

Deletion is a workflow, not a row-level `DELETE`. A practical state machine might move through `requested`, `access_blocked`, `sessions_revoked`, `data_erased`, and `completed`, with each transition committed durably and retried safely. The first committed transition should prevent new customer authentication and new support sessions. Revocation then advances a per-account security boundary before asynchronous erasure fans out to owned data stores.

For opaque server-side sessions, delete or invalidate every session indexed by the subject. For self-contained access tokens, keep their lifetime short and check a server-side account status or a per-subject revocation epoch on sensitive requests; otherwise a token remains usable until its expiry even though the console claims the account is gone. OAuth 2.0 Security Best Current Practice recommends sender-constrained access tokens where practical and refresh-token protections, but token theft defenses do not replace account-level revocation.

Exactly-once delivery is the wrong promise. Queues redeliver, clients retry after ambiguous timeouts, and workers can stop after committing a side effect but before acknowledging the message. The achievable property is exactly-once business effect: a durable idempotency record identifies one deletion intent, transitions are conditional, and each participant records that it has applied the operation.

The following Go sketch keeps that contract visible. The store implementation must make `BeginDeletion` and each state transition transactional; the identity and data components must accept the same operation ID idempotently.

```go
package deletion

import (
    "context"
    "errors"
)

type Request struct {
    OperationID string
    ActorID     string
    SubjectID   string
    CaseID      string
    Reason      string
}

type Record struct {
    OperationID string
    SubjectID   string
    State       string
}

type Store interface {
    BeginDeletion(context.Context, Request) (Record, error)
    Advance(context.Context, string, string, string) error
}

type Identity interface {
    BlockAuthentication(context.Context, string, string) error
    RevokeSessions(context.Context, string, string) error
}

type PersonalData interface {
    Erase(context.Context, string, string) error
}

type Service struct {
    Store    Store
    Identity Identity
    Data     PersonalData
}

func (s Service) Delete(ctx context.Context, req Request) error {
    if req.OperationID == "" || req.ActorID == "" ||
        req.SubjectID == "" || req.CaseID == "" {
        return errors.New("missing deletion authorization context")
    }

    rec, err := s.Store.BeginDeletion(ctx, req)
    if err != nil {
        return err
    }
    if rec.SubjectID != req.SubjectID {
        return errors.New("idempotency key belongs to another subject")
    }
    if rec.State == "completed" {
        return nil
    }

    if err := s.Identity.BlockAuthentication(ctx, req.SubjectID, req.OperationID); err != nil {
        return err
    }
    if err := s.Store.Advance(ctx, req.OperationID, rec.State, "access_blocked"); err != nil {
        return err
    }
    if err := s.Identity.RevokeSessions(ctx, req.SubjectID, req.OperationID); err != nil {
        return err
    }
    if err := s.Store.Advance(ctx, req.OperationID, "access_blocked", "sessions_revoked"); err != nil {
        return err
    }
    if err := s.Data.Erase(ctx, req.SubjectID, req.OperationID); err != nil {
        return err
    }
    if err := s.Store.Advance(ctx, req.OperationID, "sessions_revoked", "completed"); err != nil {
        return err
    }
    return nil
}
```

The sketch omits orchestration mechanics, but not the invariants. In production, a worker should resume from the stored state instead of replaying every step blindly, and conditional transitions should reject stale writers. An idempotency key must be bound to a hash of the immutable request fields; accepting the same key with a different subject is a conflict, not a retry. Audit events belong in the same transaction as state changes, commonly through an outbox, so a successful transition cannot disappear from the evidence stream.

There is a subtle ordering trade-off. Blocking access before erasure minimizes the window in which a customer or support session can act, but it can lock out a legitimate customer if approval was attached to the wrong account. That is why exact lookup, a confirmation view containing non-sensitive disambiguators, and approval precede the first transition. After that boundary, retries should move forward. An "undo" button backed by restored credentials would undermine both erasure semantics and the audit story.

## Test the invariant, not the happy path

A passing browser test proves very little. Exercise the workflow at every transaction boundary: send the same operation concurrently, stop a worker after session revocation, deliver messages out of order, present a token minted immediately before `access_blocked`, and attempt lookup from an agent whose case grant has expired. The expected outcomes are convergence, denial after the revocation boundary, and one attributable audit sequence.

Three checks deserve automation at the authorization layer. First, property tests should generate arbitrary retries and verify that a completed subject never returns to an earlier state. Second, integration tests should hold old access and refresh credentials, run deletion, and prove that neither can establish an authorized session afterward. Third, tenancy tests should vary actor and subject organizations independently; a support role in tenant A must not make a subject in tenant B discoverable.

Observe decisions rather than sensitive payloads. Useful signals include lookup denials by reason code, distinct subjects viewed per agent per time window, support-session creation, approval latency, revocation lag, retries by workflow state, and audit-outbox backlog. Avoid putting emails, access tokens, free-form case notes, or raw lookup terms into metrics labels and logs. High-cardinality personal data is both an operational liability and a privacy problem.

I'm not sure a universal numeric threshold for abusive lookup exists; staffing patterns and case mix differ too much. Establish a baseline from approved activity, review the highest-volume agents with the support security team, and tune alerts using labeled investigations. The invariant remains fixed even while thresholds change: automation must not turn one compromised agent account into an unbounded directory search.

The catch is that immediate revocation requires an online decision point or short token lifetime. A fully stateless service that accepts long-lived bearer tokens without checking account state cannot provide immediate account-wide termination; keep that design only when delayed revocation is explicitly acceptable, which GDPR account deletion and privileged support access rarely make comfortable. Likewise, strict dual approval is not suitable for every low-risk support action. Use it for destructive or high-impact transitions, while keeping ordinary lookup narrow, logged, and rate-limited.

## Roll out the boundary without breaking support

Begin in report-only mode: calculate the new policy decision, record why it would allow or deny, and compare it with existing console behavior without exposing customer data to any new path. Then require case IDs and exact lookup, introduce scoped support sessions, and finally move deletion onto the durable workflow. Each stage needs a rollback of policy enforcement, not a rollback of completed erasure.

Before enforcement, inventory every token issuer and session store. One forgotten mobile refresh-token table defeats an otherwise polished console. Ship a synthetic account through lookup, supervised access, deletion, and post-deletion authorization checks in each environment; reconcile the workflow record against identity stores and the audit outbox; and require zero unexplained differences before expanding the cohort.

Keep the decision rule compact: if a design cannot preserve actor and subject separately, bind lookup to a legitimate case, revoke all credential classes, make retries idempotent, and produce a privacy-minimized audit trail, it is not ready for agent-driven account deletion.

## References

- https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html
- https://eur-lex.europa.eu/eli/reg/2016/679/oj

## Sources

- https://pages.nist.gov/800-63-4/sp800-63b.html
- https://www.rfc-editor.org/rfc/rfc9700.html
