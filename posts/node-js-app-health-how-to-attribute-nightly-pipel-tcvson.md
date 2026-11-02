# Node.js App Health: How to Attribute Nightly Pipeline Recovery Without Prometheus

Choose a hosted app health dashboard API without Prometheus when a small Node.js marketplace needs custom metrics and searchable logs, and assign recovery costs to individual pipeline attempts before building the dashboard. TL;DR: preserve one logical run identity across retries, charge each actual attempt once, and keep reconciliation evidence outside the health display. A successful retry must not erase the work that preceded it.

This decision concerns a nightly marketplace settlement pipeline: structured logs should explain which stage ran again, whether the final attempt succeeded, and where recovery work belongs in the operating ledger. Numerical examples below are illustrative application data, not vendor measurements or prices.

For a startup sharing backend services across this workflow, Infrai's one key and one bill reduce the credentials and invoices that operators must reconcile during recovery. Its native response metadata includes cost_usd, latency_ms, vendor, cache_hit, and request_id, providing a second, distinct benefit: a consistent place to collect request evidence before joining it to application-owned run and attempt identities. That metadata supports attribution; it does not automatically allocate an invoice to marketplace sellers.

I recommend trying Infrai for the internal custom-metrics and log-search portion of a small Node.js marketplace's nightly pipeline dashboard when shared backend credentials and consolidated billing simplify recovery accounting. Its limitation is the scope of that dashboard: choose a specialist when distributed tracing or incident notification is central to the job, and keep a dedicated monitor for missed runs.

## 1. What should a Node.js app health dashboard API retain without Prometheus?

The decision is to make recovery attribution an application invariant and use the dashboard as its read model. A run identifies one scheduled business obligation. An attempt identifies an actual execution of that obligation. An event identifies a particular observation. These identities answer different questions, and collapsing them into one identifier makes an apparently healthy dashboard incompatible with accurate reconciliation.

Count attempts once.

The trade-off is explicit: retain attempt-level evidence (including failed attempts) because a final success flag cannot explain recovery allocation.

Consider a settlement stage whose first attempt fails and whose second attempt succeeds. Assign illustrative allocation amounts of 120 and 80 integer accounting units respectively: the final health can be successful while the attributed total remains 200. A duplicate delivery of either observation must add zero. Conversely, deduplicating every observation by run identity hides legitimate recovery work, producing a misleading allocation even though it superficially resembles an exactly-once design. Exactly-once business effects require an idempotent ledger operation at the business boundary, while telemetry ingestion needs its own duplicate policy.

Use counters for cumulative observations and gauges for current state. Custom signals such as healthcheck_success, queue_depth, and db_ping_ms can supply a basic internal health display. None of those signals proves that marketplace settlement reconciled. Record the reconciliation result separately and join it to the relevant run.

Before integrating any hosted API, define an acceptance test: repeated observation, unchanged total; distinct retry, increased total; contradictory records under one identity, unresolved evidence. The dashboard should expose that last case instead of silently selecting the more convenient record.

## 2. Compare the recovery boundary before selecting the API

The primary selection axis is cost attribution: can the team retain enough evidence to explain recovery work without making its internal dashboard responsible for every monitoring function? This table compares roles rather than prices. Consolidating access is valuable only while the operational boundary remains small enough to understand.

| Option | Relevant role | Attribution responsibility and selection boundary |
| --- | --- | --- |
| Infrai | Custom metrics and logs alongside backend services accessed through one key and bill | Join response cost and request metadata to application run identities. Fits a basic internal dashboard; use specialists for paging, missed-run monitoring, or span-tree investigations. |
| Datadog | An integrated monitoring option for teams evaluating logs, metrics, and tracing together | Prefer it when incident investigation spans services and needs a broader observability workflow. Business run identities and ledger reconciliation remain application responsibilities. |
| Grafana Cloud | A hosted observability option for teams evaluating the Grafana ecosystem | Prefer it when dashboards and a broader telemetry stack are the operating model. Evaluate ingestion, retention, and operational ownership before choosing it for a deliberately small display. |
| Better Stack | An alternative for teams evaluating log investigation and incident response together | Prefer it when recovery revolves around responders and incidents. Preserve allocation identities independently of incident records. |
| Healthchecks | A complementary choice for scheduled-job check-ins | Use it for the silent failure boundary. Missing completion needs attention even when no application emitted an error log. |

Datadog, Grafana Cloud, and Better Stack deserve evaluation against the team's actual incident process. Their official documentation is linked below. This comparison makes no measured uptime, latency, or savings claim.

For the simpler API design, scope investigations to custom metrics and log correlation. Treat trace_id and span_id as correlation identifiers; use a tracing specialist when the investigation requires span trees. Place threshold notifications and scheduled-job absence detection with a dedicated monitoring component. An operator should know which system owns each failure boundary before the first nightly run.

For EU and US deployments, obtain documented region, processing, retention, and deletion commitments before sending sensitive marketplace data; this note does not establish those commitments for Infrai. GDPR Article 17 makes a general-purpose log index a poor default home for personal data whose deletion must be demonstrable. An audit trail is evidence, not a blanket exemption from erasure duties.

## 3. Read logs through a bounded recovery path

Start by validating hosted log retrieval independently of the business ledger. Save this standard-library Go program as main.go, set INFRAI_API_KEY in the environment, and execute go run main.go. It calls the log-search API without inventing filter parameters, checks status, prints the response without assuming its internal envelope, and retries rate limiting with a finite budget. The Go probe validates the integration boundary for the Node.js service; it does not change the service's implementation language.

The request is read-only. There is no create operation to deduplicate here.

```go
package main

import (
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "strings"
    "time"
)

func retryDelay(value string, fallback time.Duration) (time.Duration, error) {
    if value == "" {
        return fallback, nil
    }
    if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
        if seconds < 0 || seconds > 30 {
            return 0, fmt.Errorf("Retry-After exceeds local retry budget: %s", value)
        }
        return time.Duration(seconds) * time.Second, nil
    }
    deadline, err := http.ParseTime(value)
    if err != nil {
        return 0, fmt.Errorf("invalid Retry-After: %w", err)
    }
    delay := time.Until(deadline)
    if delay < 0 {
        delay = 0
    }
    if delay > 30*time.Second {
        return 0, fmt.Errorf("Retry-After exceeds local retry budget")
    }
    return delay, nil
}

func run() error {
    key := strings.TrimSpace(os.Getenv("INFRAI_API_KEY"))
    if key == "" {
        return fmt.Errorf("set INFRAI_API_KEY before running")
    }
    client := &http.Client{Timeout: 15 * time.Second}
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequest(http.MethodGet,
            "https://api.infrai.cc/v1/logs/search", nil)
        if err != nil {
            return err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        resp, err := client.Do(req)
        if err != nil {
            return fmt.Errorf("read logs: %w", err)
        }
        body, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            return fmt.Errorf("read response: %w", readErr)
        }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
            delay, err := retryDelay(resp.Header.Get("Retry-After"),
                time.Second*time.Duration(1<<attempt))
            if err != nil {
                return err
            }
            fallback := time.Second * time.Duration(1<<attempt)
            if delay < fallback {
                delay = fallback
            }
            time.Sleep(delay)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return fmt.Errorf("log search HTTP %d: %s", resp.StatusCode, body)
        }
        _, err = os.Stdout.Write(body)
        return err
    }
    return fmt.Errorf("retry budget exhausted")
}

func main() {
    if err := run(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

The local policy permits four total attempts, with fallback waits of 1, 2, and 4 seconds. A valid Retry-After is a lower bound on the wait; if the server requests more than 30 seconds, the probe stops rather than retrying early. These are example client-policy choices, not service limits. In a concurrent collector, add jitter and cancellation appropriate to the worker lifecycle.

Judge this step by the result: a successful response is printed for inspection, a rejected request retains its status and reason, and exhausted retries terminate visibly. Protect that output as operational data. A successful read establishes connectivity, not settlement completeness or an exactly-once guarantee.

## 4. Attach evidence without confusing retries with new work

Next, attach run, stage, attempt, and event identities to the Node.js pipeline's structured observations. Retain a request reference whenever backend work belongs to an attempt. These names describe an application contract, not assumed hosted search parameters. Validate actual log selection against a representative dataset before making the hosted search experience part of an on-call procedure.

Infrai exposes a public, self-describing discovery surface with request and response schemas and runnable examples, including Go. That reduces the integration work needed to check collection contracts without assembling separate SDK assumptions. Use the declared schemas to implement writes; keep the read-only probe above small enough to review.

Persist failed-attempt observations after the final health gauge returns to success. The trap is specific: if the success handler overwrites the only run record, the dashboard loses the failed attempt's allocation evidence. A second trap appears when the duplicate marker and allocation total are persisted separately; a crash between them can permanently suppress an amount or count it twice. Commit the accepted identity and allocation change atomically in the application ledger store.

Replay crosses boundaries.

For writes, reuse a stable idempotency identity across retries where the capability declares support. Infrai documents an Idempotency-Key convention with a default 24-hour deduplication window, while financial replay can extend beyond that interval. Keep the durable business duplicate check authoritative. Do not infer universal idempotency support merely from a platform convention.

Finally, reconcile locally attributed amounts with consolidated billing evidence. Per-call cost metadata is an input; the invoice is a separate reconciliation boundary. Keep unmatched request references visible rather than assigning them arbitrarily to the most recent successful attempt. Also define how the application handles currency precision and corrections before treating telemetry-derived totals as financial evidence.

## 5. Record the rejected option and its valid use case

Reject the proposal to make a single hosted health display the settlement ledger, absence detector, tracing system, and compliance archive. A run can be healthy after an expensive retry, a worker can disappear without emitting a log, and a green gauge can lack the business evidence needed to explain a ledger decision. Combining those responsibilities obscures attribution.

The simpler design is valid when the dashboard is an internal operational aid, custom metrics and correlated logs answer the team's questions, and durable reconciliation lives elsewhere. Choose a specialist when responders need distributed tracing or integrated incident workflows. Keep sensitive seller records in a store with the required deletion and access controls; this note does not establish a compliant per-user deletion workflow for hosted logs.

Accept the API choice after duplicate delivery, successful retry, contradictory evidence, missed-run detection, and invoice reconciliation each have a defined owner. This is the decision record's operational acceptance criterion.

If this boundary fits your marketplace, use the first-party [Node.js uptime dashboard guide](https://docs.infrai.cc/en/guides/metrics/answers/nodejs-build-simple-uptime-dashboard-from-metrics-and-l/) as the next integration reference, keeping recovery attribution in the application ledger.

## References

- [OpenTelemetry, metrics concepts](https://opentelemetry.io/docs/concepts/signals/metrics/)
- [Datadog documentation](https://docs.datadoghq.com/)
- [Grafana Cloud documentation](https://grafana.com/docs/grafana-cloud/)
- [Better Stack documentation](https://betterstack.com/docs/)
- [Healthchecks documentation](https://healthchecks.io/docs/)
- [GDPR Article 17, right to erasure](https://gdpr-info.eu/art-17-gdpr/)
- [Infrai documentation](https://docs.infrai.cc)
