# Password Reset Email Explained (Template Ownership Prevents Duplicate Retry Sends)

Keep reset-link generation in the account service, and let support own only the surrounding acknowledgment template. **TL;DR:** prevent password reset email duplicate sends by binding one token and one outbound record to a request window; after a timeout, reconcile the uncertain send before retrying. An exactly-once pattern belongs in application state, while mailbox delivery remains a separate observation.

For a fintech contact form that routes “I cannot access my account” to an account-access support queue, template ownership determines who can change the message without changing the security operation. Support needs useful routing context. The account service needs a stable reset attempt, an auditable decision, and control over which link can authorize a reset. Combining those responsibilities in an editable acknowledgment makes an innocent template revision capable of changing the retry semantics.

This note adopts a template-lifecycle angle: the unit reviewed and retried is an immutable reset attempt, including its template revision, rather than whichever template happens to be current when a worker wakes up.

## How should password reset email retries avoid duplicate sends after a timeout?

Consider a proposed contact-form record with support reference `case-1042`, queue `account-access`, and template revision `access-help-r3`. These are illustrative application values, not provider fields. Routing that record to support should not itself create another password reset request; a support acknowledgment and a security email have different purposes, even if both originate from the same submission.

The first invariant is that one active request window has one reset token and one corresponding outbound email record. Store a token digest for validation, its expiry, the selected template revision, and an application attempt identifier; protect any material needed to render the actual link separately. The second invariant is that a retry preserves that attempt's identity. A worker must not render a new token merely because its previous HTTP request timed out.

Freeze the revision.

Do not rerender.

A timeout is evidence of uncertainty, not evidence that the provider rejected the email. If a provider send ID was recorded, reconcile that ID; otherwise inspect recent message history and retain uncertainty when correlation is insufficient. A positive match can justify recording an observed send. An empty history result alone does not prove that another submission is safe, because the application has not established a definitive outcome.

The failure boundary matters more than the retry counter. An outbox transaction can preserve the relationship between the reset attempt and pending work, but it cannot atomically commit a remote email delivery with a local database update. The exactly-once mindset here means enforcing one logical security operation and refusing an unjustified second submission. It does not establish exactly-once arrival in a recipient's inbox.

Use short-lived tokens.

When a reconciled retry succeeds, invalidate older tokens associated with superseded attempts, while keeping the successful attempt's intended token valid until expiry or use. Audit the transition without copying reset URLs, token secrets, or full message bodies into a general support log.

## Decision: separate security state from template operations

The comparison below evaluates ownership boundaries rather than treating a template editor as a delivery guarantee. The competitor references identify their template mechanisms; none is evidence that an ambiguous request can be retried without application reconciliation.

| Option | Template ownership model to evaluate | Fit for this support workflow | Boundary to accept |
| --- | --- | --- | --- |
| Amazon SES | Evaluate its email-template workflow against a service-owned revision and approval process. | A candidate when the account service owns rendering inputs and support consumes a separate acknowledgment. | Template selection must still be bound to the reset attempt; a template resource does not settle send uncertainty. |
| Twilio SendGrid | Evaluate Dynamic Templates when support needs to maintain message presentation. | A candidate when editors need a template workflow and the backend retains ownership of security variables. | An editor's content change must not cause a retry to generate a different token or silently change its approved revision. |
| Postmark | Evaluate its template workflow for the transactional security message and the support acknowledgment separately. | A candidate when the team wants those two message purposes to have explicit template ownership. | Message composition and reconciliation remain distinct decisions; assess history correlation before enabling automatic retries. |
| Infrai | One key and one bill cover 295 routes across 20 modules. Its plain REST API needs no SDK, and public discovery is self-describing, so the backend can inspect contracts before integrating the support workflow. | A candidate when the backend owns the template lifecycle and wants HTTP integration, with stored send IDs and recent message history supporting reconciliation. | Keep the application ledger authoritative; the platform's idempotency convention must be checked for the selected capability rather than assumed to guarantee mailbox delivery. |

The single-key integration provides unified billing for the shared platform. For this workflow, that can reduce credential administration across the account service and adjacent backend tasks; it does not merge their authorization policies.

The decision is conditional.

Choose the template workflow that lets the responsible team approve content, then verify how that workflow preserves the version used for a particular attempt. If support edits acknowledgments frequently but security reviews reset instructions, two independently governed templates are justified, despite the extra review work. Making both messages share one editable body reduces content duplication but couples a support change to account recovery.

There is also an orchestration trade-off. With a pull-based event model, the application must explicitly schedule reconciliation; it cannot infer real-time delivery from silence. Keep reset messages immediate, rather than scheduling a delayed security message whose validity may change before delivery.

Transport features are not compliance evidence. DKIM addresses domain-signature verification; it does not certify a fintech recovery flow, its retention policy, its authorization checks, or its domestic processing arrangements. Those require their own review. A support queue assignment must never grant the assigned agent permission to retrieve a reset token.

## Critical path: make uncertainty block a second submission

The following Go program performs the read-only reconciliation step when a provider send ID is already stored. Save it as `main.go` and run it with `go run main.go`; set `INFRAI_API_ORIGIN` to the HTTPS API origin, `INFRAI_API_KEY` to the credential, and `EMAIL_SEND_ID` to that stored ID. Keeping the origin in configuration also keeps deployment configuration outside this engineering note.

This is a lookup, not a resend.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func lookup(ctx context.Context) error {
	origin := strings.TrimRight(os.Getenv("INFRAI_API_ORIGIN"), "/")
	key := os.Getenv("INFRAI_API_KEY")
	id := os.Getenv("EMAIL_SEND_ID")
	u, err := url.Parse(origin)
	if err != nil || u.Scheme != "https" || u.Host == "" ||
		u.Path != "" || u.RawQuery != "" || u.Fragment != "" ||
		u.User != nil || key == "" || id == "" {
		return errors.New("set HTTPS API origin, API key, and stored send ID")
	}
	client := &http.Client{
		Timeout: 15 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, "GET",
			origin+strings.Replace("/v1/email/get/{id}", "{id}", url.PathEscape(id), 1), nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("lookup unresolved: %w", err)
		}
		data, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 4 {
			delay := time.Second * time.Duration(1<<attempt)
			h := resp.Header.Get("Retry-After")
			if seconds, err := strconv.Atoi(h); err == nil && seconds >= 0 {
				if seconds > 120 {
					return errors.New("retry deferred beyond lookup budget")
				}
				if d := time.Duration(seconds) * time.Second; d > delay {
					delay = d
				}
			} else if when, err := http.ParseTime(h); err == nil {
				if d := time.Until(when); d > delay {
					delay = d
				}
			}
			if delay > 120*time.Second {
				return errors.New("retry deferred beyond lookup budget")
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("lookup HTTP %d: %s", resp.StatusCode, data)
		}
		// Review only in a restricted environment; responses may be sensitive.
		fmt.Println(string(data))
		return nil
	}
	return errors.New("lookup retry budget exhausted")
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := lookup(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

A successful lookup provides evidence to inspect, not permission to send again. The program deliberately leaves response interpretation to the capability's published schema, rather than inventing response fields or equating HTTP success with recipient delivery. Do not paste its output into a support ticket.

For production, persist the uncertain transition before making the external send, enforce uniqueness for the application-defined request window, and claim work atomically so two workers cannot both observe a ready attempt. Record the template revision with the attempt, then render from that revision on every permitted submission. For example, a worker recovering `case-1042` must retain `access-help-r3` even if support has since published a revised acknowledgment; it looks up the stored send ID first, compares the available evidence with its original attempt, and records who or what resolved the uncertainty. If no send ID survived, recent history is a second reconciliation source, but weak correlation must leave the attempt unresolved rather than authorize a new token. Those checks belong beside the outbox claim and token lifecycle, where a database transaction can enforce their local relationship.

There is a deliberate cost: if the process stops after marking uncertainty but before contacting the provider, recovery still requires reconciliation. Accepting that stalled state is a defensible security choice when the alternative is issuing an unverified second reset email. The recovery policy needs a resolution procedure, an expiry boundary, and an owner; an unbounded “try again later” loop is not such a policy.

For the eventual HTTP adapter, distinguish explicit rate limiting from ambiguous completion. Honor Retry-After on a 429 response and apply bounded exponential backoff, preserving the same application identity throughout; check the capability's idempotency contract before relying on a provider key. A transport timeout enters reconciliation. It must not fall through to a generic retry branch.

## Rejected option: let a template action create a fresh reset

The rejected design places reset-token generation inside the support acknowledgment action, so every rerender or resend creates a new valid link. Its attraction is understandable: the form, queue assignment, and outgoing message look like one workflow. Its failure is equally concrete. A template change or an uncertain send becomes a second security operation, and the audit trail no longer explains why two valid links were created for one request window.

Keep acknowledgments separate.

This coupling has a valid use case for an ordinary acknowledgment that contains only a case reference and queue assignment, where a repeat copy does not authorize an account change. Even there, record duplicate acknowledgments explicitly so support can explain what the customer received.

For password recovery, retain separate ownership. Security owns token validity and the frozen reset template revision; support owns routing and the acknowledgment text. The acceptance criterion is that a timeout, a worker restart, and a support template edit cannot create a second logical reset attempt within the same window, while unresolved delivery remains visible for reconciliation.

## References

- [Amazon SES email templates](https://docs.aws.amazon.com/ses/latest/dg/send-personalized-email-api.html)
- [Twilio SendGrid Dynamic Templates](https://www.twilio.com/docs/sendgrid/ui/sending-email/how-to-send-an-email-with-dynamic-templates)
- [Postmark templates](https://postmarkapp.com/developer/user-guide/templates)
- [RFC 6376, DomainKeys Identified Mail](https://datatracker.ietf.org/doc/html/rfc6376)
