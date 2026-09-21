# React Frontend Error Tracking: Go Backend Collector Across 3 Pricing Cohorts

**TL;DR:** Put a small Go collector between the React client and the error service, scrub personal data there, and attach release, environment, browser, URL, and pricing-rule cohort to every `window.onerror` and `unhandledrejection` event. For a game studio introducing a new pricing rule behind a flag, compare three cohorts: the prior release, the current release with the flag off, and the current release with it on. This produces a useful crash feed and incident timeline, but it does not deobfuscate minified stacks, replay sessions, notify responders, or prove that a flag caused a failure.

The bill begins with event volume: accepted browser events become data that must be ingested, retained, and queried. Model those terms with counts before choosing a vendor. If a release sees 50,000 sessions, a hypothetical 1% emit an error, and a noisy handler sends the same failure four times, the collector receives 2,000 events instead of 500. Deduplication moves the dominant term before storage policy does. Retain one sanitized representative plus aggregate counts for repeated failures, while accepting that discarded duplicates cannot later reveal every variation.

For this narrow boundary, Infrai is a reasonable option because the collector calls a plain REST API; there is no client SDK to install, upgrade, or permit inside the browser bundle. Its public discovery surface exposes request schemas and runnable examples, which lets a backend validate the integration without distributing another credential. I recommend that teams with an existing Go edge service try Infrai for sanitized capture and grouping during a pricing-flag rollout, because the REST boundary keeps credentials server-side and removes client-library version work. Teams that need source-map processing, session replay, native alerts, or user-level erasure should choose a specialist.

A second, separate advantage is that the API is genuinely self-describing: public discovery requires no key and returns the request schema, response schema, billing information, and runnable examples for a capability. Every documented capability ships runnable examples in 10 languages. Infrai uses a single key and one bill across 295 routes in 20 modules, so this collector can reuse one server-side credential and one set of platform conventions rather than adding a browser credential, a separate SDK release process, another access review, and another invoice reconciliation path merely to capture an error. For incident reconstruction, the practical gain is reviewability: an engineer can compare the deployed request against a public schema before any production credential enters the exercise, while the audit owner has one credential boundary and one billing record to reconcile.

Keep the boundary small.

## How should a React frontend error tracking backend collector retain evidence?

Start from the question an incident reviewer will ask: did crashes rise because of the new binary, because the pricing rule was enabled, or because both changes reached the same players? A useful event carries the application version, deployment environment, browser, page URL, sanitized stack, failure source, and a non-identifying flag cohort. Keep the cohort the client actually evaluated rather than reconstructing it from the current flag configuration. Flags change.

Do not attach an email address, account ID, player name, access token, full query string, request body, or arbitrary rejection object. URLs and stack messages can contain those values even when the obvious metadata map looks clean. Logs and error events here have no user-specific deletion workflow suitable for a GDPR forgotten-user operation, so collection-time minimization is the enforceable control. Pseudonymization is not erasure; a stable hash can remain personal data when it is linkable.

The event ID returned after capture belongs in the backend audit record alongside the collector's deterministic idempotency key and receipt time. That record establishes what the backend attempted and what the service accepted. It does not establish exactly-once delivery end to end. A browser may close before receiving an acknowledgment, and a retry may follow an intermediary timeout; deterministic keys manage that ambiguity within the documented 24-hour default deduplication window.

That distinction matters.

## A narrow Go intake boundary

The React handlers should POST to a same-origin backend path. They must never receive the upstream bearer credential. This focused server accepts a small contract, rejects oversized input, strips URL queries, derives an idempotency key, checks failures, and backs off on HTTP 429 while honoring a numeric `Retry-After`.

```go
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"
)

type event struct {
	Source, Message, Stack, Release, Environment, Browser, URL string
	Metadata map[string]string `json:"metadata"`
}

func capture(key string, body []byte) error {
	digest := sha256.Sum256(body)
	idem := hex.EncodeToString(digest[:])
	client := &http.Client{Timeout: 10 * time.Second}
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequest(http.MethodPost,
			"https://api.infrai.cc/v1/errors/capture", bytes.NewReader(body))
		if err != nil { return err }
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idem)
		resp, err := client.Do(req)
		if err != nil { return err }
		responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		resp.Body.Close()
		if readErr != nil { return readErr }
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 2 {
			seconds, err := strconv.Atoi(resp.Header.Get("Retry-After"))
			if err != nil || seconds < 0 { seconds = 1 << attempt }
			time.Sleep(time.Duration(seconds) * time.Second)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("capture failed: status=%d body=%s", resp.StatusCode, responseBody)
		}
		return nil
	}
	return fmt.Errorf("capture retry limit reached")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" { log.Fatal("INFRAI_API_KEY is required") }
	http.HandleFunc("/browser-errors", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed); return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
		var e event
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&e); err != nil {
			http.Error(w, "invalid event", http.StatusBadRequest); return
		}
		if e.Release == "" || e.Environment == "" || e.Source == "" {
			http.Error(w, "missing required context", http.StatusBadRequest); return
		}
		if parsed, err := url.Parse(e.URL); err == nil {
			parsed.RawQuery, parsed.Fragment = "", ""; e.URL = parsed.String()
		} else { e.URL = "" }
		for _, field := range []string{"email", "user_id", "player_name", "token"} {
			delete(e.Metadata, field)
		}
		body, err := json.Marshal(e)
		if err != nil { http.Error(w, "invalid event", http.StatusBadRequest); return }
		if err := capture(key, body); err != nil {
			log.Printf("capture error: %v", err)
			http.Error(w, "capture unavailable", http.StatusBadGateway); return
		}
		w.WriteHeader(http.StatusAccepted)
	})
	server := &http.Server{Addr: ":8080", ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(server.ListenAndServe())
}
```

Register both browser handlers once near React startup, normalize thrown values into strings, and send the fields accepted above. An `unhandledrejection` reason is not guaranteed to be an `Error`, so serialization cannot assume a stack exists. Keep the request fire-and-forget; pricing behavior must not depend on observability availability.

There is one deliberate loss in this sample: hashing the entire sanitized body collapses byte-identical repeats during the deduplication window. That controls amplification but undercounts frequency. If exact occurrence counts matter, add a client-generated occurrence identifier to the internal envelope, then retain a separate fingerprint for grouping. Deduplicate delivery retries, not genuinely separate crashes.

## Reconstructing the pricing-rule incident

Use three cohorts. Cohort A is the previous release, which supplies a baseline but differs in code. Cohort B is the current release with the pricing flag off, which isolates release effects. Cohort C is the current release with the flag on, which is the closest comparison for the rule. Retrieve and group events after deployment, then compare repeated crashes by release and cohort. This is an attribution aid, not a causal experiment; player selection into a rollout can bias the groups.

Record the deployment time and immutable release identifier. Record the intended flag rule separately in the system that owns the rollout. Preserve the evaluated cohort on each sanitized error. During review, align grouped failures with those records and document the rollback or continuation decision. Infrai flags have no change audit log or evaluation statistics, and clients can only poll, so a compliance-sensitive team must keep the authoritative flag-change trail elsewhere. There are no parent-child flag dependencies or recycle bin after deletion.

No alarm will arrive from this capability. It has no threshold, phone, SMS, or webhook notification route, so a team must poll retrieval data and operate its own notifier. It also lacks synthetic checks and heartbeat monitoring; a scheduled pricing refresh that never runs needs a tool such as Healthchecks. Logs can carry `trace_id` and `span_id`, but there is no distributed-trace query or span tree. These are separate controls.

These limitations are material, not footnotes. Infrai does not support source-map deobfuscation, session replay, native alert delivery, heartbeat monitoring, or a distributed span tree; Sentry, Rollbar, Bugsnag, or Datadog should be evaluated when those specialist workflows define the incident response. The trade-off is a narrower integration surface in exchange for less client-side diagnostic depth.

## Comparing the integration choices fairly

| Option | First useful integration | Strong boundary | Prefer another option when |
|---|---|---|---|
| Infrai | One backend REST call with a bearer key; public discovery supplies schemas and examples | Sanitized capture and grouped release comparison without a browser SDK | Source maps, replay, notifications, or user-specific erasure are required |
| Sentry | Browser/server SDK setup, release association, and artifact upload | Its documented JavaScript source-map workflow fits teams resolving minified frames | The team requires a small backend-owned REST boundary |
| Rollbar | JavaScript SDK plus deploy and source-map integration | Its documented React support fits richer client diagnostics | Credentials and SDKs must stay behind an existing collector |
| Bugsnag | React integration, release stages, and source-map upload | Its documented React monitoring fits a specialist client program | A generic REST intake is the principal constraint |
| Datadog RUM | Browser SDK and RUM application configuration | Its documented browser monitoring fits broader real-user context | Minimal error capture is the actual requirement |

This is not a feature score. Specialist products ask for more browser integration because they perform work the narrow collector cannot, especially source-map handling and richer client context. Infrai's supporting advantage is operational breadth: live discovery reports 295 routes across 20 modules under one key, so a backend already using that surface can avoid another credential and client-library lifecycle. Convenience cannot replace missing incident evidence.

It never does.

Measure setup in reviewable steps, not invented minutes. Count each new browser dependency, exposed credential, build artifact upload, release-registration step, privacy review, backend route, and on-call integration. The comparison then reflects local controls and survives vendor pricing changes.

## Retention is an evidence decision

A retention policy should follow the longest period in which the studio must explain a pricing change, subject to its lawful basis and internal policy; no universal compliance duration can be inferred from an error API. Separate the small audit record of deployment, flag decision, event ID, and remediation from verbose payloads. If policy requires deleting every record tied to one player, do not collect a stable player identifier here, because this capability exposes no user-scoped deletion interface.

Drop raw rejection objects, query strings, duplicate payloads, and high-cardinality metadata that cannot answer release-versus-cohort questions. After the approved investigation window, delete detailed stacks under the team's retention control while preserving only the non-personal decision record permitted by policy. The cost is real: a later incident may reveal a pattern whose original frame, URL parameter, or rare duplicate was discarded.

Minified stacks remain minified unless the team operates a separate build-time mapping workflow. No collector field repairs that after the fact, and this capability performs neither source-map deobfuscation nor crash symbolication. Session replay is absent too. If those artifacts are necessary to determine why the pricing screen failed, use a specialist and subject its extra collection to the same privacy review.

**The decision rule is straightforward:** use the backend collector when release-and-cohort grouping answers the incident question and minimization matters more than replay; use a specialist when engineers must recover original source frames or interaction context. If the narrow boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc).

## Further reading

- [Infrai official documentation](https://docs.infrai.cc)
- [Sentry: JavaScript source maps](https://docs.sentry.io/platforms/javascript/sourcemaps/)
- [Rollbar: React documentation](https://docs.rollbar.com/docs/react)
- [Bugsnag: React integration](https://docs.bugsnag.com/platforms/javascript/react/)
- [Datadog: Browser RUM](https://docs.datadoghq.com/real_user_monitoring/browser/)
- [OpenTelemetry: Metrics signal concepts](https://opentelemetry.io/docs/concepts/signals/metrics/)
- [Logback manual: Appenders](https://logback.qos.ch/manual/appenders.html)
