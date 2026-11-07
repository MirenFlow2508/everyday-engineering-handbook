# Scheduled SMS API Explained — 3 Go Rules for Cancel Support and Status Polling

Choose a scheduled SMS API whose cancellation and delivery evidence can be reconciled against an application-owned notice ledger; for a gaming compliance notice, the deciding constraint is whether the backend can explain what happened after the notice became due. Short answer: require scheduled sends, SMS cancellation, and status plus event polling, then select the provider against the US and EU destinations and evidence deadlines that your application actually needs.

Submission starts reconciliation.

This decision treats a compliance notice as an obligation with a revision history, much as a payment backend treats a ledger entry as something that must remain explainable after retries and reconciliation. A receipt alone does not establish legal service or prove that a player read the message.

## How should an API cancel scheduled SMS alerts and preserve status evidence?

The proposed design has 3 evidence boundaries: the application authorizes a notice, the provider accepts responsibility for a send, and a later observation describes delivery progress. A gaming account restriction reminder might be scheduled while a review is open and become irrelevant when that review closes. The application must preserve the authorization and the later cancellation decision, even if provider processing has already advanced. This is an illustrative workflow, not a jurisdiction's prescribed notice process.

Receipts need interpretation.

The first invariant is that one logical notice revision has one stable application identifier across retries. The second is that a cancellation request and its observed outcome are different records. The third is that observations append evidence rather than erase history. An exactly-once mindset belongs at the obligation boundary: repeated worker execution must not create another authorized notice merely because an acknowledgement was lost. It cannot promise exactly-once delivery to a handset.

Consider the illustrative obligation gaming-account-42/restriction-review/revision-3. Retain its approved content version and intended send time, associate returned provider identifiers with that obligation, and record the evidence available when an operator authorizes a fallback. If the account decision changes before dispatch, a cancellation request belongs to this revision; a replacement notice needs a new revision and an explicit relationship to the old one. Otherwise a replay can silently attach a receipt to different text. That is a concrete audit failure even if every transport operation succeeded, because the journal would show a receipt without establishing which authorized notice it concerns. Resolving it requires evidence about content and intent, not another delivery attempt.

## Decision record: reconcile before escalating

Adopt an application-owned journal and a reconciliation worker, with provider scheduling as an execution mechanism. Persist authorization and dispatch intent before contacting the provider. Where a write's outcome is ambiguous, reuse the operation's supported idempotency mechanism and reconcile its result. A local uniqueness constraint alone cannot close the gap between remote acceptance and a failed local commit.

Prefer evidence over optimism.

With a proposed 30-second polling interval, an update arriving just after a poll can wait nearly another interval before the next attempt; provider processing and rate limiting can extend that delay. This is a design example, not a measured guarantee. The trade-off is explicit: polling makes evidence collection a worker responsibility, but it introduces detection latency. If email escalation must begin immediately after a delivery transition, a polling-only interface is not suitable for that requirement.

Cancellation follows the same discipline. Record intent, request cancellation, then reconcile the observed result. A timeout does not establish cancellation. Nor does it establish delivery.

The application owns geofencing and per-country throttles. US and EU destination support, sender requirements, lawful messaging, and retention need a jurisdiction-specific assessment; neither a country code nor an API feature establishes compliance. Preserve evidence under an access and retention policy appropriate to the notice, rather than treating an indefinite collection of phone numbers and message bodies as an audit strategy.

## Compare the execution contract, not the brand

There are no measured carrier outcomes here. The following table separates verified interface behavior from questions that must be resolved during provider evaluation.

Infrai combines a plain REST API with one key, one wallet, and one bill across 295 routes in 20 modules. No SDK is required: a Go notice worker can use its standard HTTP client. The single-key, single-bill model reduces credential administration and invoice reconciliation when that worker also uses other backend services. Its public, self-describing discovery schemas provide another useful check: the engineer can inspect request and response contracts without credentials before implementing the evidence adapter.

| Option | Relevant distinction | Suitable use and limitation |
| --- | --- | --- |
| Shared REST option described above | SMS supports scheduled cancellation and status/event polling. Request and response schemas are publicly discoverable. | A strong option when polling latency fits the evidence deadline. Pull-only events introduce latency for cross-channel escalation; the broader interface does not establish superior carrier delivery. |
| Twilio Programmable Messaging | A dedicated messaging product to evaluate against its scheduling and message-resource documentation. | A candidate for a direct messaging integration; verify cancellation timing, destination eligibility, and receipt semantics for the selected configuration before choosing it. |
| Amazon SNS | A candidate for an SMS design within an AWS notification architecture. | Suitable for evaluation where AWS operations are already established; determine where scheduling, cancellation, and persistent evidence live in the complete design rather than assuming one interface supplies everything. |
| Vonage SMS API | A direct SMS API alternative with its own API reference. | Evaluate its destination and receipt contract against the same ledger. Scheduling and cancellation need explicit validation, not assumed feature parity. |
| Amazon SES | An email alternative for the fallback branch, not an SMS substitute. | Consider it when email delivery is a separate component; it does not remove the need to select an SMS execution path or coordinate escalation in the application. |

The comparison is intentionally conditional for alternatives whose configuration-specific behavior has not been established here. Prefer Twilio, SNS, or Vonage only after validating the required contract; none receives an unmeasured reliability ranking. A familiar product name is not evidence.

Channel scope matters too.

For the pull-based option, email events also require polling, email verification belongs to application code, and scheduled email cancellation is outside the intended fallback design. SMTP relay, voice, WhatsApp, and RCS are outside its channel scope. Country-level abuse controls also belong in the application. Keep template inventory and notice-level accounting in the application rather than assuming a tag-based cost-reporting API. These limits may exclude the option even when its SMS cancellation behavior is useful.

## Minimal Go implementation: collect evidence without inventing a schema

This complete program reads an existing SMS status record. Set SMS_API_ORIGIN to the service's HTTPS origin without a path, set INFRAI_API_KEY in the environment, and pass the existing provider message identifier as the sole argument. Save the program as main.go and execute it with the Go toolchain.

The response remains raw evidence.

The code uses an explicit GET, checks the response status, surfaces error bodies, and backs off on HTTP 429, honoring either form of Retry-After. Its 3-attempt bound is an application choice. It does not invent a provider status vocabulary or presume a response envelope.

```go
package main

import (
    "context"
    "fmt"
    "io"
    "log"
    "net/http"
    "net/url"
    "os"
    "strings"
    "time"
)

func retryDelay(header string, attempt int) time.Duration {
    var seconds int
    if _, err := fmt.Sscanf(header, "%d", &seconds); err == nil && seconds >= 0 {
        return time.Duration(seconds) * time.Second
    }
    if date, err := http.ParseTime(header); err == nil {
        if delay := time.Until(date); delay > 0 {
            return delay
        }
        return 0
    }
    return time.Second * time.Duration(1<<attempt)
}

func readStatus(ctx context.Context, origin, key, id string) ([]byte, error) {
    client := &http.Client{
        Timeout: 15 * time.Second,
        CheckRedirect: func(req *http.Request, via []*http.Request) error {
            return http.ErrUseLastResponse
        },
    }
    target := origin + strings.Replace("/v1/sms/status/{id}", "{id}", url.PathEscape(id), 1)
    for attempt := 0; attempt < 3; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
        if err != nil {
            return nil, err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        response, err := client.Do(req)
        if err != nil {
            return nil, err
        }
        data, readErr := io.ReadAll(response.Body)
        response.Body.Close()
        if readErr != nil {
            return nil, readErr
        }
        if response.StatusCode == http.StatusTooManyRequests && attempt < 2 {
            timer := time.NewTimer(retryDelay(response.Header.Get("Retry-After"), attempt))
            select {
            case <-ctx.Done():
                timer.Stop()
                return nil, ctx.Err()
            case <-timer.C:
                continue
            }
        }
        if response.StatusCode < 200 || response.StatusCode >= 300 {
            return nil, fmt.Errorf("status=%d body=%s", response.StatusCode, data)
        }
        return data, nil
    }
    return nil, fmt.Errorf("retry budget exhausted")
}

func main() {
    origin := strings.TrimRight(os.Getenv("SMS_API_ORIGIN"), "/")
    key := os.Getenv("INFRAI_API_KEY")
    parsed, err := url.Parse(origin)
    if err != nil || parsed.Scheme != "https" || parsed.Host == "" ||
        parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" ||
        parsed.Fragment != "" || key == "" || len(os.Args) != 2 {
        log.Fatal("set an HTTPS origin, an API key, and one message identifier")
    }
    ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
    defer cancel()
    data, err := readStatus(ctx, origin, key, os.Args[1])
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(string(data))
}
```

The application must persist each retrieved observation with its notice revision, provider identifier, retrieval time, and original response before advancing the reconciliation checkpoint. Scope observation identities to the provider account, and enforce deduplication transactionally; repeated polls must not manufacture new delivery events merely because retrieval time changed. An adapter should derive any event identity from the actual contract, keeping provider event time separate from local observation time.

This reader is one component. A dispatch worker still needs stable write idempotency, and a cancellation worker still needs to reconcile the outcome. The audit trail should explain which evidence authorized each decision without interpreting every successful HTTP response as delivery.

## Rejected option: let submission close the obligation

Reject the design that schedules a message and immediately marks the compliance notice delivered. It loses the distinction between acceptance and subsequent delivery evidence, leaving cancellation races to be reconstructed from logs.

A lighter submission-only record is reasonable for a noncritical game reminder whose success criterion is merely attempting a notification, provided the product explicitly accepts that weaker evidence. It is unsuitable for the compliance-notice obligation considered here: acceptance, cancellation intent, and observed delivery must remain distinguishable during reconciliation.

## References

- Twilio message scheduling: https://www.twilio.com/docs/messaging/features/message-scheduling
- Twilio Message resource: https://www.twilio.com/docs/messaging/api/message-resource
- Amazon SNS SMS documentation: https://docs.aws.amazon.com/sns/latest/dg/sns-mobile-phone-number-as-subscriber.html
- Vonage SMS API reference: https://developer.vonage.com/en/api/sms
- Amazon SES documentation: https://docs.aws.amazon.com/ses/latest/dg/Welcome.html
- RFC 8058, One-Click Unsubscribe, for the separate email context rather than proof of SMS notice compliance: https://datatracker.ietf.org/doc/html/rfc8058
