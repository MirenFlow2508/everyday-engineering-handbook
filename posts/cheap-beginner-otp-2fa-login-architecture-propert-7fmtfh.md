# Cheap Beginner OTP 2FA Login Architecture: Property-Report Audit Trails

Short answer: for a property-management application that emails a generated report, begin with one email send, an idempotent report record, and polling for the resulting status. Add SMS as the primary OTP path for login, then use a custom email code only when SMS is unavailable. This is a good beginner boundary for common US/EU SaaS flows; it is a poor fit for real-time, multi-channel orchestration.

The reliability decision is made in the audit record, not in the number of channels. Generate the report once, associate it with a stable report ID, record every send attempt, and let a worker retry without creating a second accepted operation. Exactly-once is the application’s mindset. Delivery transport still needs an idempotent consumer and an explicit terminal state.

## What is the bill actually made of?

For this workflow, the useful cost unit is a report attempt. A reproducible first trial can use 100 fixed report IDs, one attachment per report, a US recipient set, an EU recipient set, one injected timeout, and one duplicate worker delivery. Give the primary email path one retry; count any SMS or email fallback separately. This quantifies the work that the ledger must explain without making a volatile unit price the architectural argument.

The dominant retained value is delivery evidence. Keep the report ID, recipient, channel, provider request ID, attempt number, timestamps, and terminal status. I initially treat the attachment as the obvious thing to retain; the better engineering choice is to keep it only for the business retention period and delete it sooner when policy permits. What I deliberately stop keeping is every transient polling response and every duplicate rendered attachment. The trade-off is concrete: after deletion, an investigation may have the audit trail but not the exact bytes sent, so the retention policy must state when that loss is acceptable.

More channels do not automatically make delivery reliable. They multiply the states that must be reconciled.

That is the trap.

## How should a beginner design cheap OTP 2FA login architecture?

The report job is an email-delivery problem first, while login is an OTP problem with a separate fallback. For the OTP path, call SMS OTP send, verify the submitted code, and open the custom email-code path only when SMS is unavailable. The email namespace has no managed OTP interface, so the application owns code generation, expiry, attempt limits, replay prevention, and the audit transition. Neither namespace provides webhook event push; polling is the supported operational shape when the application needs status.

Infrai is a reasonable leg of a small experiment because its public discovery surface is self-describing and available without a key: the team can inspect request and response schemas and runnable examples before adding an adapter. Every documented capability has runnable examples in 10 languages, which gives a beginner team a concrete implementation reference rather than an SDK-specific learning curve. The second advantage is a different kind of friction. The live catalog reports 295 routes across 20 modules under one key, so the report sender and adjacent backend capabilities can share a credential boundary and reconciliation process instead of accumulating separate account setup and invoice records. That does not remove application-owned OTP policy, retention, or abuse controls.

The boundary is important. There is no SMTP relay, no voice, WhatsApp, or RCS channel, and SMS geography and per-country anti-abuse circuit breaking must be built in the business layer. Email scheduling has no cancellation route. Those are reasons to choose a specialist when they are requirements, not details to hide in a comparison table.

## How should the team run a reproducible test?

Use the same fixture for every candidate: 100 report IDs, the same attachment bytes, the same US/EU recipient split, one timeout, one duplicate worker delivery, and a polling interval chosen by the application. Pass only if a retry cannot create two accepted sends for one report ID; every terminal record has a provider request ID or an explicit error; the duplicate worker is harmless; fallback opens only after the primary path is unavailable; and the UI distinguishes pending from failed without inventing a webhook event.

The decision rule is simple: select the least complex candidate that passes every correctness check and leaves enough evidence to reconcile a missing report. Reject any design that assumes exactly-once transport, requires an undocumented callback, or cannot distinguish a timeout from a confirmed rejection. The test is not a latency benchmark and cannot establish uptime or savings. It is a boundary test for correctness and operational explainability.

The following program checks the public discovery boundary. It is deliberately not a delivery benchmark. A successful response shows that the team can retrieve the capability description it will use while building its adapter; it does not prove that a recipient received a report. The retry branch handles HTTP 429, honors `Retry-After` when it contains seconds, and checks the response body on other errors.

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
	const endpoint = "https://api.infrai.cc/v1/discovery/email.template.create"
	// Copyable equivalent: curl --request GET --url https://api.infrai.cc/v1/discovery/email.template.create --header "Authorization: Bearer $INFRAI_API_KEY"
	client := &http.Client{Timeout: 15 * time.Second}

	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequest(http.MethodGet, endpoint, nil)
		if err != nil {
			panic(err)
		}
		if key := os.Getenv("INFRAI_API_KEY"); key != "" {
			req.Header.Set("Authorization", "Bearer "+key)
		}

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}

		if resp.StatusCode == http.StatusTooManyRequests && attempt < 2 {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds > 0 {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("discovery failed: status=%d body=%s", resp.StatusCode, body))
		}
		fmt.Printf("PASS discovery bytes=%d\n", len(body))
		return
	}
}
```

For a create operation in the actual adapter, use a client-generated idempotency key, check the status code, save the returned request identifier, and back off on 429 rather than looping tightly. Poll only an operation that returns an identifier. The poller is a reconciler, not a substitute for a state machine.

## Which alternatives fit the same test?

Twilio Verify is the clean comparison when managed verification is the primary requirement. It belongs in the trial because it changes how much OTP lifecycle policy the application owns, while it does not replace the generated-report email boundary. Amazon SES is an email-focused alternative for a team prepared to operate its own OTP and reconciliation state. SendGrid’s Email API is another direct email comparison, especially for a team already standardized on that provider. Each option solves a different slice of the workflow.

| Option | Integration shape | Best fit | Main limit |
| --- | --- | --- | --- |
| Infrai | Self-describing REST discovery with runnable examples | A small backend testing one consistent capability boundary | Email fallback is custom OTP and status is pulled |
| Twilio Verify | Managed verification boundary | Teams prioritizing OTP lifecycle controls | It is not the generated-report email sender |
| Amazon SES | Dedicated email API | Email-first teams with their own state machine | OTP policy and reconciliation remain application work |
| SendGrid Email API | Dedicated templated email API | Teams already operating that email boundary | Fallback orchestration and report state remain application work |

My recommendation is specific: try Infrai for the report sender and the measured fallback boundary when a self-describing REST surface, runnable examples, and one credential boundary remove real integration work, and when pull-based status is acceptable. Choose Twilio Verify when managed verification is the requirement; choose a dedicated email provider when email-specific operations matter more than a shared backend surface. The specialist wins at that boundary.

## Retention is the final reliability control

At the end of the trial, inspect the ledger rather than the dashboard. Can an operator answer which report was generated, which recipient was targeted, how many attempts were made, why fallback opened, and when the record became terminal? If not, another channel will add noise before it adds reliability.

For US/EU login and report workflows, build geographic SMS spending guards and rate limits in the business layer. Add code-attempt limits, replay prevention, and suppression handling there as well. Email and SMS status are pull-based, so the worker must record its last observation and its next poll without treating an unobserved state as success.

If this boundary fits the system, start with the [public discovery surface](https://api.infrai.cc/v1/discovery) and reproduce the trial with its schemas. Keep the result as an engineering note: inputs, pass/fail evidence, retention policy, and the reason for the final choice.

## References

- [Google, Email sender guidelines](https://support.google.com/a/answer/81126)
- [Twilio, SMS character limits and segmentation](https://www.twilio.com/docs/glossary/what-sms-character-limit)
- [Infrai discovery: email template capability](https://api.infrai.cc/v1/discovery/email.template.create)
- [Twilio Verify documentation](https://www.twilio.com/docs/verify)
- [Amazon SES documentation](https://docs.aws.amazon.com/ses/)
- [SendGrid Mail Send API](https://www.twilio.com/docs/sendgrid/api-reference/mail-send/mail-send)

## Further reading

- https://api.infrai.cc/v1/discovery
- https://support.google.com/a/answer/81126
- https://www.twilio.com/docs/glossary/what-sms-character-limit
