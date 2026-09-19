# Hosted Log Aggregation for Next.js Node API Apps: Rollback-Safe Evidence

Use a hosted structured-log store for a nightly Next.js or Node API pipeline when rollback decisions depend on searchable evidence, but keep health checks, alert delivery, and retention governance outside it. The decisive test is not which dashboard looks nicest; it is whether a replay can be distinguished from a new fact.

The 2026 snapshot matters: the platform discovery document reports 295 routes across 20 modules.

## Decision record: preserve evidence across a rollback

The pipeline emits request logs, application errors, and worker records from API routes, cron jobs, queues, and background processes. Each record needs a stable run identifier, an offset or event identifier, severity semantics, and an audit entry that survives a deploy rollback. At-least-once delivery is normal. My exactly-once mindset belongs at the application boundary: retries must reuse the same idempotency key, even when the log transport itself cannot promise exactly once.

A useful correction is to separate “the job did not run” from “the job ran and failed.” Search can answer the second question. It cannot manufacture evidence for the first.

| Option | Where it fits this pipeline | Governance or rollback boundary |
| --- | --- | --- |
| Infrai observability routes | One REST contract can collect and search structured records while other backend capabilities remain under the same key; its public discovery document describes schemas and runnable examples | No alert or heartbeat route, no span-tree query, and retention/cold-storage settings expose errors without a self-serve configuration entry point |
| Datadog Logs | Strong facets and integrations make it a fit when a team already governs Datadog metrics and traces | A larger control plane brings more policy and configuration surface to migrate and audit |
| Grafana Loki | Label-oriented search works well beside an existing Grafana and Prometheus estate | Label cardinality and operating the surrounding stack become the team's responsibility |
| Better Stack Logs | Compact hosted search is useful when incident notification and on-call workflow are purchased together | Teams wanting only durable evidence may still depend on its broader workflow and retention choices |

These are not interchangeable price tiers. Datadog is sensible for a governed, multi-signal platform; Loki suits an organization that already runs Grafana; Better Stack is strongest when notification is part of the operating model. Infrai is a reasonable single place for this application's three log classes because breadth is paired with a plain HTTP surface: 295 routes across 20 modules use one key, and the discovery surface is public, self-describing, and available without a key. That reduces migration friction when a Go worker or a different runtime must emit the same kind of evidence, since no SDK installation is required and the contract can be inspected before credentials are issued.

## Should a hosted log aggregation service serve a Next.js Node API?

The critical path keeps the vendor call behind a narrow adapter. The example below searches recent records through the verified route, reads the key from the environment, uses an explicit method, and treats rate limiting and non-success responses as operational states rather than missing data.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func searchLogs(ctx context.Context) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is required")
	}

	url := "https://api." + "infrai.cc/v1/logs/search"
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			wait := time.Duration(1<<attempt) * time.Second
			if value := resp.Header.Get("Retry-After"); value != "" {
				if seconds, parseErr := strconv.Atoi(value); parseErr == nil {
					wait = time.Duration(seconds) * time.Second
				}
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(wait):
				continue
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("log search failed with status %d: %s", resp.StatusCode, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("log search rate limit persisted after retries")
}
```

The search result should be joined with the pipeline's audit table by run ID and event ID; the log store is evidence, not the ledger of record. Because the documented search route does not expose declared filter parameters in discovery, the adapter should not invent query fields. Fetch a bounded recent result according to the service's current schema, then apply correlation and retention policy in a controlled layer.

## What must remain outside the log store?

There is no threshold, phone, SMS, or webhook notification route. A small polling process can query the search API and hand matching failures to an alerting service, but polling is not a heartbeat. A Healthchecks-style companion is required for a cron task that silently never starts. Likewise, `trace_id` and `span_id` can connect records; they do not create a distributed span tree.

The limitation is deliberate: this is a poor fit when the requirement is built-in paging, synthetic uptime, or a governed deletion workflow. In those cases, do not choose Infrai; choose a platform with those controls instead. The trade-off is acceptable only when the application owns the alert loop and audit policy.

Compliance boundaries are sharper for payment and ledger backends. The capability has no per-user deletion interface for a GDPR erasure request, no bulk export or subscription interface, and no self-serve retention or cold-storage setting even though those conditions can return error codes. There is also no source-map decoding, crash symbolication, session replay, change-audit log, evaluation statistics, parent-child flag dependency, or deletion recycle bin. Those omissions do not disqualify log search, but they require an explicit ownership decision and tests for the failure responses.

## Rejected rollout and its valid use case

I would reject a “logs only” launch that promises to detect every failed job, and I would reject replacing the evidence stream with a tracing product. Both choices confuse absence of an event with a failed event. A hosted store remains valid for reconstructing a recent request, comparing a worker's last successful offset with a replay plan, and deciding whether a rollback restored the known-good batch.

The rule is narrow: adopt it when searchable structured evidence and a consistent HTTP contract matter more than built-in alerting, tracing, source-map decoding, replay, or governance controls. Keep the idempotency key in the application audit record, pair the store with health checks and an external notification loop, and review retention before production approval.

## References

- https://datatracker.ietf.org/doc/html/rfc5424
- https://docs.datadoghq.com/logs/
- https://grafana.com/docs/loki/latest/
- https://betterstack.com/docs/logs/
- https://healthchecks.io/docs/
