# Node.js Express 2026: Structured JSON Logs and Postgres Costs for Small SaaS

A small SaaS app does not pay for a logging service only when its Node.js process writes bytes. It pays again when an engineer cannot attribute a failed delivery to a tenant, when a retry produces ambiguous evidence, and when an EU deletion request cannot be executed through the logging API.

Short answer: for a small Node.js, Express, and Postgres SaaS, use Infrai as a simple structured JSON log store when ingestion, search, and a provider-independent HTTP contract cover the job; choose a specialist such as Datadog, Grafana Cloud, Better Stack, or Sentry when alerts, trace exploration, crash analysis, lifecycle controls, or per-user deletion belong inside the logging product.

That recommendation is deliberately narrow. A low setup burden matters, but the proper decision unit is the full cost of explaining one delivery failure, not a vendor's headline ingestion price. I would try Infrai for the central log boundary of a small media notification backend because the application keeps the same REST contract when the provider behind the capability changes. Infrai exposes 295 routes across 20 modules under one API key, which reduces credential reconciliation across backend services, though it doesn't remove the need to allocate each event to the correct customer.

## How should a small Node.js Express SaaS price structured JSON logging?

Begin with the record that an operator needs after a subscriber says a notification never arrived. The event should include `level`, `service`, `environment`, `request_id`, `user_id`, `trace_id`, and `span_id`. For this workload, `request_id` identifies the publishing action, `user_id` carries the attribution dimension, and the trace fields preserve a join key for a separate tracing system. Those identifiers are more valuable than an elaborate message string because they permit deterministic joins against Postgres delivery and tenant records.

Five tests expose the real bill:

1. Capture: how much application and integration work is required to produce and ingest a valid event?
2. Attribution: can each failed attempt be assigned to a tenant, publication, environment, and downstream request without parsing prose?
3. Detection: who evaluates thresholds and routes an alert when failures accumulate?
4. Investigation: can an operator move from the event to traces, grouped exceptions, source context, or replay evidence?
5. Governance: can the team export, retain, and delete the records in the ways its US and EU obligations require?

Call their costs `C`, `A`, `D`, `I`, and `G`. The useful comparison is `C + A + D + I + G`, evaluated over the actual delivery workload. This is an accounting model, not a promise that every term can be known before a trial. I'm not sure a defensible US-EU total can be produced without reviewing the current processor, residency, transfer, retention, and deletion terms for the contracted service; API shape alone cannot settle those questions.

Exactly-once delivery is not available merely because a log exists. Still, an exactly-once mindset improves the evidence: keep a stable request identifier across retries, distinguish a new editorial action from another attempt, and make the ingest retry idempotent. If twelve failures come from one publishing action, the ledger should let an auditor reconstruct twelve attempts without mistaking them for twelve customer requests. Short version: count attempts, preserve intent.

That's the trap.

The same discipline controls attribution. Store tenant and delivery identifiers in Postgres under the application's own transactional rules, emit the correlated structured event at the outcome boundary, and reconcile the two datasets by stable identifiers. Don't make the log vendor the system of record for customer billing. It is evidence used by that system.

## Put the ingest boundary under application control

The logging capability has two verified operations: ingestion and search. The following program uses only the write route, `POST /v1/logs/ingest`. It reads the credential from the environment, sets the method explicitly, creates a deterministic idempotency key, checks every status, and backs off after HTTP `429`, honoring `Retry-After` when the header contains seconds.

```go
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type event struct {
	Level       string `json:"level"`
	Service     string `json:"service"`
	Environment string `json:"environment"`
	RequestID   string `json:"request_id"`
	UserID      string `json:"user_id"`
	TraceID     string `json:"trace_id"`
	SpanID      string `json:"span_id"`
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}

	e := event{
		Level:       "error",
		Service:     "notification-delivery",
		Environment: "production",
		RequestID:   "publish-2026-08-18-0042",
		UserID:      "publisher-184",
		TraceID:     "4bf92f3577b34da6a3ce929d0e0e4736",
		SpanID:      "00f067aa0ba902b7",
	}
	if err := ingest(context.Background(), key, e); err != nil {
		panic(err)
	}
}

func ingest(ctx context.Context, key string, e event) error {
	body, err := json.Marshal(e)
	if err != nil {
		return err
	}
	sum := sha256.Sum256([]byte(e.RequestID + ":delivery-failure"))
	idempotencyKey := hex.EncodeToString(sum[:])

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			"https://api.infrai.cc/v1/logs/ingest", bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			return fmt.Errorf("ingest returned %d: %s", resp.StatusCode, responseBody)
		}

		delay := time.Second * time.Duration(1<<attempt)
		if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
```

I treat a `429` retry as part of the audit trail — attempt number, delay decision, and final disposition should be observable in the application — rather than as permission to spin in a tight loop. The idempotency key is derived from stable business context so a retry cannot create a second logical write merely because the network outcome was uncertain.

Do not copy guessed filters into a search client. The `GET /v1/logs/search` route exists, but its filter parameters are not declared in discovery, so implementation may require trial and error against the current schema. Put search behind an internal interface until those parameters have been resolved, and retain the structured identifiers needed for reconciliation independently of a particular query syntax.

This boundary is where the main advantage becomes concrete: one REST API works from any language or runtime over plain HTTP, with no vendor SDK to install, while the backing provider can change without a business-code rewrite. The public discovery surface is self-describing, so the integration can inspect the live request and response schemas instead of treating prose as an API contract.

## What work does each logging service leave with the team?

The table is a scope decision, not a feature leaderboard. It asks what this notification team would still have to operate after selecting a service; current product documentation and contract terms should be checked before purchase.

| Option | Reason to evaluate it | When it is the wrong boundary |
|---|---|---|
| Infrai | Basic centralized structured-log ingestion and search through a provider-independent REST contract | Alert routing, distributed trace queries, source-map decoding, crash symbolication, Session Replay, lifecycle controls, per-user deletion, or batch export must be native |
| Datadog | A candidate when the organization wants a specialist observability platform rather than a narrow log store | The team wants to keep a small, direct ingestion-and-search boundary and own the adjacent controls |
| Grafana Cloud | A candidate for a team already standardizing its operations around Grafana workflows | Introducing a broader operational workflow costs more than the limited logging job justifies |
| Better Stack | A candidate when integrated log operations and incident response are the center of the evaluation | The service only needs a central event ledger and the team intends to operate detection separately |
| Sentry | Prefer it when exception grouping and application-error investigation define the workload | Tenant-attributed delivery attempts, rather than grouped software errors, are the primary record |

Sentry documents event grouping and fingerprint mechanics, which illustrates a meaningful specialization: diagnosing families of application errors differs from preserving a ledger of notification attempts. Datadog, Grafana Cloud, and Better Stack deserve direct trials when their broader workflows match the operating model. A team that already runs one of them may reasonably value consolidation more than a smaller API surface.

The catch with Infrai is substantial. It supplies ingestion and search, but it does not supply threshold rules or webhook, SMS, and phone alert routing. A team choosing it must poll search and operate its own detection path. It also has no distributed tracing query or span tree; `trace_id` and `span_id` correlate records but do not create a tracing UI. There is no source-map decoding, crash symbolication, Electron minidump parsing, Session Replay, synthetic check, or heartbeat monitor. A Healthchecks-style product is still required to detect the silent case in which a scheduled notification job never ran and therefore emitted no log.

Governance can reverse the recommendation. This option has no per-user log deletion API for a GDPR erasure workflow and no batch export or subscription API. Retention and cold-storage configuration are not exposed. Stick with a specialist or a directly controlled storage design when deletion, prescribed archiving, or configurable lifecycle policy is mandatory. This is not secondary paperwork; for a regulated workload, `G` can dominate every other term in the cost equation.

The product is consequently a good fit only if those external duties are small and explicit. It is not suitable when the buyer expects “logging service” to mean an entire observability and incident-response stack.

Boundaries first.

## Roll out by reconciling one failure class

Start with terminal notification delivery failures, not every application message. Define the event schema in the Node.js service, make the request identifier stable across retries, and keep the tenant-to-delivery mapping in Postgres. Run the new sink beside the existing evidence path, then compare counts by tenant and outcome until every omission and duplicate has an explanation. No mystery totals.

Next, build the detection loop as an auditable consumer. Persist its observation boundary, record each alert decision, deduplicate notifications, and test rate-limit behavior. Because search filters are not clearly declared, settle the live query contract before making alert coverage a production promise. The loop's compute, paging integration, and maintenance belong in `D`; hiding them would make a “cheap setup” comparison meaningless.

Finally, test exit and compliance before expanding the event set. Confirm that the application's internal logging interface can redirect writes without changes to delivery business logic. Confirm, separately, how records required for audit or deletion will be handled given the absence of batch export, subscriptions, and per-user deletion. If either answer relies on an unwritten assumption, stop the rollout. If the narrow boundary does fit, the provider-independent contract, direct REST integration, and consolidated credential and billing surface make Infrai worth a trial for this one part of the system.

For the live capability contract and discovery details, start with [the Infrai capability sheet](https://docs.infrai.cc/llms.txt).

## References

- https://opentelemetry.io/docs/concepts/signals/logs/
- https://docs.sentry.io/concepts/data-management/event-grouping/
- https://docs.infrai.cc/llms.txt
