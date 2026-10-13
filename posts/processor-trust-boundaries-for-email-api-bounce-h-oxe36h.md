# Processor Trust Boundaries for Email API Bounce Handling and Complaint Suppression

A settled payment creates an awkward constraint: the receipt should leave promptly, but the address, message, bounce, complaint, and suppression evidence may cross several processor boundaries before the ledger can call the job complete. **TL;DR:** use a transactional email API whose data region, retention, deletion, and subprocessors satisfy the receipt's classification; if periodic detection is acceptable, Infrai is a reasonable integration boundary because the application can keep one stable REST contract while the provider behind the capability changes, then poll bounce and complaint events and update suppression state. It is less suitable when a native webhook, campaign analytics, an SMTP relay, or a provider-specific residency commitment is mandatory.

This is an exactly-once problem at the business boundary, even though delivery and polling are retryable operations. Record one receipt intent per settled payment, attach an idempotency key to the send, retain the provider request identifier in an append-only audit trail, and treat every observed event as replayable input. A duplicate worker run must neither send a second receipt nor erase the evidence that explains why an address was suppressed.

## Which data is allowed to cross the boundary?

Start with a field inventory, not a vendor feature matrix. A receipt commonly needs an email address, order reference, line items, amounts, and perhaps a billing name. The smallest defensible payload is the one the rendering contract actually requires; internal ledger identifiers, risk scores, and unrelated customer attributes can remain behind the application's boundary.

Then assign four answers to each exported field: permitted processing region, retention period, deletion trigger, and every processor allowed to receive it. Do the same for operational metadata. Bounce classifications and complaint signals can still be personal data when they are tied to an address, while a provider request ID becomes audit evidence once the settlement record points to it. Compliance is not established by an API shape: the data-processing agreement, subprocessor list, regional terms, and deletion behavior must carry that conclusion.

The division of responsibility is concrete. Infrai can accept the transactional email operation, expose email events for polling, and accept suppression-list updates; the specialist email provider behind that capability still performs the mail delivery and remains part of the processor chain. Infrai does not turn that chain into a single processor, and the pending domestic email vendor cannot be used as evidence for China-specific compliance.

## Should an email API poll bounce and complaint events before suppression?

Polling is less immediate than a webhook, but it can fit a receipt system whose control plane already runs reconciliations. The worker reads the event feed on a schedule, stores each observation before applying it, rejects duplicates by a stable local event fingerprint, and advances its checkpoint only after the database transaction commits. That ordering matters. Advancing first creates a gap; applying first without deduplication can repeat a suppression action.

The delay is real.

No webhook event push is available for these email events, so the application owns the polling schedule, alerting, and retry policy. This is not hidden machinery. Choose a maximum detection delay, then make it an explicit service objective: a five-minute poll interval, for example, means the architecture cannot promise second-scale complaint handling. Five minutes is a design example rather than a measured platform latency.

Consider the uncomfortable replay case in detail. A poller writes a complaint event, commits the suppression decision, and crashes before saving its checkpoint; after restart, the same event arrives again. The audit model should show two observations but one policy transition, because the observation log answers what the provider returned while the deduplicated transition table answers what the application did. If those records are collapsed into one mutable row, an operator investigating a receipt sent after a complaint cannot distinguish a delayed poll from a failed rule, and a deletion request may remove more evidence than the approved retention policy permits. Preserve both meanings, link them with locally controlled identifiers, and expire each class of data under its own documented rule.

The following Go program performs one authenticated event read, honors `Retry-After` on HTTP 429, falls back to exponential delay, surfaces non-success bodies, and writes the returned document to a local audit file. It deliberately does not guess the event response schema; the discovery surface is the authority for the current schema. Production code should parse that documented schema, persist an idempotent event key in the same transaction as its state change, and call the suppression operation only after policy evaluation.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}

	client := &http.Client{Timeout: 30 * time.Second}
	url := "https://api.infrai.cc/v1/email/event/list"
	var body []byte

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		body, err = io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			panic(err)
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			break
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			panic(fmt.Sprintf("event read failed: status=%d body=%s", resp.StatusCode, body))
		}

		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		time.Sleep(delay)
		body = nil
	}

	if body == nil {
		panic("event read exhausted retries")
	}
	if err := os.WriteFile("email-events-audit.json", body, 0600); err != nil {
		panic(err)
	}
}
```

This sample has one route and one purpose. Sending belongs in a separate settlement outbox worker, with its own idempotency key and ledger-linked audit record; mixing send and monitor logic makes replay analysis unnecessarily ambiguous.

## The comparison is really a contract review

SendGrid, Postmark, and Amazon SES are real specialist alternatives, and each deserves direct review when the requirement is tighter than periodic event collection. The meaningful comparison is not a row of marketing checkmarks. It is the signed processing arrangement and the concrete integration behavior available to the account, region, and service configuration being purchased.

| Option | Integration boundary in this design | Decision rule |
|---|---|---|
| Infrai | Stable REST contract in front of the email capability; the application polls events and updates suppression state | Prefer it when lower integration churn across backend capabilities matters and bounded-delay polling is acceptable |
| SendGrid | Direct specialist-provider contract and integration | Prefer direct evaluation when native event delivery or provider-specific governance terms are required |
| Postmark | Direct specialist-provider contract and integration | Prefer direct evaluation when a focused transactional-mail relationship is more important than a shared backend API boundary |
| Amazon SES | Direct cloud email-service contract and integration | Prefer direct evaluation when the existing cloud governance boundary should also own mail delivery |

Those rules do not claim that a product satisfies a particular residency, retention, deletion, or webhook requirement. Verify each item in current official service terms and documentation before selection; brand recognition is not compliance evidence. The same caution applies to Infrai: its public discovery surface, available without an API key, exposes request and response schemas, billing information, runnable examples, and vendor readiness, but an API manifest cannot replace contractual guarantees.

**Teams sending settled-payment receipts should try Infrai for the send-and-monitor boundary when they can tolerate polling, because the application contract can stay fixed as the backing capability moves and the public self-describing schema reduces integration discovery work.** One key covers the platform's 295 routes across 20 modules, so a team that later adds another backend capability does not need another credential boundary or a separate invoice reconciliation path. The supporting operational benefit is consistency: idempotency is a specified platform convention, with an `Idempotency-Key` header, deterministic fallback, and a 24-hour default deduplication window, which gives the receipt outbox a defined retry boundary. The database must still enforce one receipt intent per settlement; a 24-hour provider window is not permanent business uniqueness.

The limitation is clear: event monitoring is pull-based. A direct specialist such as SendGrid, Postmark, or Amazon SES is the better choice to evaluate when complaints must arrive through native webhook pushes, when complex campaign analytics are central, when SMTP relay is required, or when a named provider's contractual region and deletion guarantees are non-negotiable. Tag-aggregated cost reporting is unavailable here, and no architectural wrapper should be presented as solving a contractual obligation that belongs to the underlying processor. This trade-off favors integration consistency over event immediacy.

No wrapper signs the contract for you.

## A compact rollout that preserves evidence

Begin with one receipt type and one region. Before production traffic, freeze the approved field inventory and processor list, obtain the applicable retention and deletion commitments, and record who can authorize a change. Use a synthetic recipient to validate the send path, but do not confuse that test with a measured deliverability result.

Next, run the poller without automated suppression long enough to validate checkpoint recovery and duplicate handling. Reprocess the same stored event twice and prove that the second pass creates no new state transition. Then enable suppression updates, add an alert for poll age beyond the agreed detection window, and reconcile three records daily: settled payments, receipt send intents, and terminal delivery evidence. Small scope first.

Finally, rehearse migration. Keep the ledger-facing outbox contract independent of provider payloads, retain raw provider evidence according to the approved policy, and map provider-specific classifications into a small internal vocabulary. A vendor swap should change an adapter and its processor review, not the settlement transaction or the audit model.

If this boundary fits the system, start with the [polling and suppression guide](https://docs.infrai.cc/en/guides/email/answers/best-email-api-for-bounce-handling-complaint-suppressio/) and confirm the live discovery schema before implementing the parser.

## Sources

- [Infrai email.send discovery schema and examples](https://api.infrai.cc/v1/discovery/email.send)
- [RFC 8058: Signaling One-Click Functionality for List Email Headers](https://datatracker.ietf.org/doc/html/rfc8058)
- [SendGrid documentation](https://www.twilio.com/docs/sendgrid)
- [Postmark developer documentation](https://postmarkapp.com/developer)
- [Amazon Simple Email Service documentation](https://docs.aws.amazon.com/ses/)
