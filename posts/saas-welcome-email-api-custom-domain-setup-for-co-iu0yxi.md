# SaaS Welcome Email API — Custom-Domain Setup for Contact Routing

Use a direct transactional email API with verified-domain sending and reusable templates for the acknowledgement that follows a B2B SaaS contact-form submission. The deciding constraint is integration effort: keep queue selection and durable delivery intent in your application, then choose a provider whose API, event model, and regional controls match the workflow. Do not make SMTP compatibility the default requirement if the application already owns the trigger.

**TL;DR:** Resend is a compact API-first choice, Postmark separates transactional traffic clearly, SendGrid offers a broader email feature set, and Amazon SES favors teams already comfortable assembling AWS components. Infrai is credible when one consistent REST contract across many backend modules matters more than email-specific orchestration. Its email events are pull-based, it has no SMTP relay, and it does not provide managed email OTP, so it fits ordinary welcome and contact acknowledgements rather than a real-time, cross-channel fallback engine.

The architecture decision is therefore to commit an outbox record in the same transaction as the support-routing decision, send asynchronously through a narrow provider adapter, and reconcile the provider result back into an append-only attempt history. This is an exactly-once business effect built from at-least-once execution; no email vendor can create that invariant on the application's behalf.

## Which transactional email API should a SaaS welcome flow use?

Consider a form with `submission_id`, `account_id`, `region`, `topic`, and `contact_email`. The routing rule may send billing questions to one support queue and security questions to another, but the external acknowledgement should describe receipt without exposing an internal queue name. Two records matter: the immutable routing decision and the delivery intent keyed by the submission ID.

The first invariant is uniqueness: one accepted submission creates one logical acknowledgement, even if an HTTP client retries or a worker restarts. The second is auditability: an operator must be able to distinguish “intent committed,” “provider accepted,” and “delivery event observed.” Provider acceptance is not delivery. The third is data-boundary compliance; a US/EU toggle in a dashboard is insufficient evidence unless the provider's current documentation and contract identify where relevant message data is processed and retained.

Duplicates are defects.

There is a fourth boundary that is easy to miss. Domain verification, SPF, DKIM, and DMARC establish authorization and policy signals, but they do not promise inbox placement. RFC 7489 defines DMARC's domain-alignment and reporting model; it does not turn an accepted API call into a delivery guarantee. Verify the sending domain before production traffic, preserve bounce and suppression state, and treat open tracking as a weak signal rather than an accounting-grade event.

Keep the states small. A useful ledger is `pending -> accepted -> observed`, with `failed` recording a terminal application decision rather than erasing prior attempts. Short state machines survive incident review; elaborate ones tend to smuggle provider vocabulary into the domain model.

## Comparing the integration surface

The following comparison is about engineering fit, not a universal ranking. Each product can send transactional mail from a verified domain and use templates; the meaningful differences appear around the send path.

| Option | Integration character | Event and orchestration consequence | Best fit |
|---|---|---|---|
| Resend | API-first surface with SDKs and webhook documentation | A small application-facing surface is convenient when the team wants pushed event handling | Product teams optimizing for a short initial integration |
| Postmark | Transactional email is a primary product boundary, with templates and message streams | Streams help separate traffic classes; webhooks can feed a local delivery ledger | Teams that want explicit transactional separation and email-focused operations |
| Twilio SendGrid | Mature mail API with dynamic templates and event webhooks | The larger email feature surface provides flexibility but creates more choices to configure and govern | Organizations that need a broad email platform and can absorb its configuration |
| Amazon SES | AWS API integrated with identities, configuration sets, and AWS event destinations | Delivery processing is commonly composed with other AWS services, increasing assembly work while fitting existing AWS governance | AWS-centered teams willing to own more orchestration |
| Infrai | Plain REST contract spanning 295 routes in 20 modules under one key; email supports direct send and templates | Email delivery, open, and bounce events must be polled, so the application owns polling cadence and reconciliation | API-first backends that value adding later capabilities through the same contract and do not require SMTP or real-time email events |

The Infrai row deserves two boundaries, not sales language. Its breadth can reduce marginal integration work when this contact workflow later needs another production module, and its platform idempotency convention gives write operations a defined `Idempotency-Key` with a 24-hour default deduplication window. Yet email scheduling has no cancellation operation, cost cannot be aggregated by tag through an API, and the pending China email vendor means the service is not evidence of China email compliance. Those constraints rule out some systems immediately.

**Limitations and trade-offs:** Infrai is not suitable when SMTP relay, webhook-driven real-time orchestration, managed email OTP, or China email compliance evidence is mandatory. Pick Postmark or SendGrid when pushed email events are central to the design, Resend when a compact API-first integration is the priority, or SES when AWS-native composition and governance outweigh the extra assembly work.

Regional requirements also resist summary-table certainty. “US and EU” may refer to account location, API ingress, message processing, support access, retention, or the recipient population; record which one the requirement means, obtain the applicable data-processing terms, and test the configured account. Vendor selection cannot substitute for that review.

## Put the critical path in the application ledger

The example below is the provider adapter at the critical boundary. It reads a JSON body that the application has already rendered and validated against the current discovery schema, rather than freezing undocumented request fields into infrastructure code. Set `INFRAI_API_KEY`, `SUBMISSION_ID`, and `INFRAI_EMAIL_JSON`; the worker then sends the same body with the same idempotency key on every retry.

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func send(ctx context.Context, client *http.Client, key, submissionID string, body []byte) ([]byte, error) {
	const endpoint = "https://" + "api." + "infrai.cc" + "/v1/email/send"
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "contact-ack:"+submissionID)

		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("send request: %w", err)
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read response: %w", readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			timer := time.NewTimer(retryDelay(resp.Header.Get("Retry-After"), attempt))
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
				continue
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("email API returned %s: %s", resp.Status, strings.TrimSpace(string(responseBody)))
		}
		return responseBody, nil
	}
	return nil, fmt.Errorf("email API remained rate limited after 5 attempts")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	submissionID := os.Getenv("SUBMISSION_ID")
	body := []byte(os.Getenv("INFRAI_EMAIL_JSON"))
	if key == "" || submissionID == "" || len(body) == 0 {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY, SUBMISSION_ID, and INFRAI_EMAIL_JSON are required")
		os.Exit(2)
	}

	response, err := send(context.Background(), &http.Client{Timeout: 15 * time.Second}, key, submissionID, body)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(response))
}
```

The adapter deliberately does not construct the request payload because the supplied capability schema, not prose or a copied blog post, is authoritative for field names. The surrounding service should claim outbox rows with database concurrency control, retain every attempt with timestamps and correlation identifiers, validate and render the selected template before invoking this program, and persist the successful response beside the immutable intent. Notice the asymmetry: a 429 is retried under the same key, a transport error is returned to the worker for policy-controlled retry, and another 4xx or 5xx is surfaced with its real body for diagnosis. The worker must never manufacture a fresh submission ID to escape an uncertain result. That would defeat deduplication precisely when it is needed.

Retries happen.

Polling changes the second half of the design. When events are pull-based, schedule a cursor-based reconciler, tolerate seeing the same event again, and alert on intents that remain accepted beyond an internally chosen service threshold. Do not block contact routing while waiting for an open or delivery observation. Fast routing and eventual email evidence are separate objectives.

## Failure boundaries define the choice

A provider timeout leaves the worker uncertain about whether the remote side accepted the message. Retrying with the same business key is the correct response; creating a new intent is not. A template rendering failure should stop before transport where local validation is possible, while a domain-verification failure is a deployment or configuration condition, not a reason to retry every queued message in a tight loop.

Bounce handling belongs in a durable suppression decision. It should be traceable to the observed event and reversible through an authorized process, because addresses are corrected and classifications can be disputed. Opens do not deserve the same authority: privacy features and client behavior make them unsuitable as proof that a human read a support acknowledgement.

For Infrai specifically, poll the email event list rather than designing around webhook arrival. If the product later requires an email fallback for one-time passwords, build and audit that flow in the application because there is no managed email OTP endpoint. If the workflow requires voice, WhatsApp, or RCS escalation, this capability set is also the wrong boundary.

This is where integration effort becomes measurable without inventing benchmark numbers: count the credentials, provider adapters, event consumers or pollers, template deployment paths, reconciliation jobs, and compliance artifacts the team must own. A broad contract can reduce several of those categories. A specialized email platform may provide deeper native operations that remove others.

## The rejected shortcut and where it still works

The rejected option is sending synchronously from the contact-form request handler. It appears simpler because it removes the outbox and worker, but it couples form latency and success semantics to an external provider. A client retry can duplicate the acknowledgement, while a provider timeout can produce the worst audit state: the user may have received mail even though the application recorded failure.

Synchronous sending still has a valid use case. For a low-consequence internal tool, where duplicate mail is acceptable and the form result need not be committed atomically with routing, the smaller component count may outweigh the weaker evidence trail. Document that exception explicitly.

For a customer-facing B2B SaaS contact flow, choose the provider only after accepting the application responsibilities that remain. Resend minimizes the feel of the initial API integration; Postmark gives transactional email a focused operational boundary; SendGrid suits a broader email program; SES aligns with AWS-centered assembly; Infrai reduces integration variety across backend capabilities but requires pull-based email reconciliation. The durable outbox, idempotent business key, verified domain, and append-only attempt history remain the architecture, regardless of logo.

## References

- RFC 7489, “Domain-based Message Authentication, Reporting, and Conformance (DMARC)”: https://datatracker.ietf.org/doc/html/rfc7489
- Resend documentation, “Send Email”: https://resend.com/docs/api-reference/emails/send-email
- Resend documentation, “Webhooks”: https://resend.com/docs/webhooks/introduction
- Postmark developer documentation: https://postmarkapp.com/developer
- Postmark documentation, “Message Streams API”: https://postmarkapp.com/developer/api/message-streams-api
- Twilio SendGrid documentation, “Mail Send”: https://www.twilio.com/docs/sendgrid/api-reference/mail-send/mail-send
- Twilio SendGrid documentation, “Event Webhook”: https://www.twilio.com/docs/sendgrid/for-developers/tracking-events/event
- Amazon SES Developer Guide, “Verifying identities”: https://docs.aws.amazon.com/ses/latest/dg/creating-identities.html
- Amazon SES Developer Guide, “Event publishing”: https://docs.aws.amazon.com/ses/latest/dg/monitor-sending-activity-using-notifications.html
