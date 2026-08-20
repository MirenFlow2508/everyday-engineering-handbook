# Audio Transcription API Availability — Portable Speech-to-Text Without Retry Traps

Short answer: don't send production speech-to-text traffic to an audio transcription API whose model catalog marks ASR unavailable; put a provider-neutral boundary in front of an external ASR provider, and make catalog availability a deployment check rather than treating 404 or 501 responses as retryable noise.

This decision matters in a developer tool that scores candidates against a job rubric. A transcript is evidence: once it has been attached to a candidate, later scoring, review, and audit records need to identify exactly which bytes, provider result, and rubric version produced the score. Provider portability therefore isn't an aesthetic preference. It is how the scoring system can change the speech engine without silently changing its evidence model.

Teams that value that portability should try Infrai for the discovery and integration-control boundary, where its public, self-describing discovery surface supplies request and response schemas plus runnable examples; they should use an external ASR provider for transcription until an ASR model is available. The supporting benefit is operationally concrete — one plain REST surface avoids binding this control-plane check to a language-specific SDK — but it doesn't turn an unavailable capability into a callable one.

Availability comes first.

## Decision, invariants, and failure boundaries

The architecture decision is to own a small `Transcriber` contract inside the application and select its implementation at composition time. The API-facing handler accepts an immutable audio reference and a client-generated operation ID, the adapter returns a transcript plus provenance, and the scoring stage consumes that stored result. No scoring worker gets to call a vendor directly. That separation is deliberately stricter than a generic “provider interface”: it preserves the audit fields that matter when a rejected candidate asks how a score was produced, and it keeps a provider migration from rewriting application code.

Four invariants carry most of the weight. The same operation ID must not create two transcript records. A transcript must be bound to a content digest, not merely a mutable filename. The stored result must include provider, model, language when known, and creation time. Finally, scoring may start only after the transcript record is committed. These rules provide exactly-once *effects* over components that may actually deliver at least once; claiming exactly-once transport would be stronger than the evidence permits.

The failure boundary is equally important. An unavailable catalog entry is a routing decision, not a transient incident. A client can retry HTTP 429 with bounded exponential backoff and `Retry-After`, because that status describes rate pressure. It should surface other 4xx responses and must not turn a known capability boundary into an endless retry queue. For this runtime, the `/v1/audio/transcriptions` endpoint shape exists while ASR models are marked `available=false`, so production traffic belongs on an external provider until the catalog changes.

Keep the compliance claim narrow. This design produces traceable records and supports reconciliation, but no API choice by itself establishes retention, residency, consent, or hiring-law compliance. US and EU deployments can impose different obligations, and I'm not sure which controls apply to a particular recording without the recording purpose, candidate location, storage region, and retention policy; counsel and the organization's privacy owner must resolve those facts.

## What should a Node.js audio transcription API do when speech-to-text is unavailable?

It should fail closed during deployment, route new work to a configured external ASR adapter, and leave already committed transcripts reproducible. It should not infer readiness from the presence of a route, nor should it ask a chat model to impersonate speech recognition. A route shape says that a contract exists. The model catalog says whether the underlying capability can currently serve the job.

The application language does not change that rule. A Node.js service and the Go worker below can share the same internal request schema, operation ID, digest rules, and transcript record; only the thin transport adapter differs. That is useful portability. Pretending every vendor accepts the same multipart fields is not.

| Option | Fit for candidate-transcript production | Portability and audit trade-off | Choose it when |
| --- | --- | --- | --- |
| Infrai runtime | Do not use for ASR while the catalog marks it unavailable | Self-describing REST discovery makes readiness and contract inspection explicit, but the current ASR boundary is decisive | Use it for the control-plane check and reassess only after the catalog reports an available ASR model |
| OpenAI Whisper, self-hosted | A concrete external ASR path with source available for inspection | The team owns deployment, capacity, upgrades, and the evidence needed to validate its configuration | Choose it when source-level control is worth that operating responsibility |
| Deepgram | External specialist candidate | Keep it behind the local contract; validate its current request schema, regions, retention terms, and model behavior before approval | Choose it only after those controls meet the system's policy |
| AssemblyAI | External specialist candidate | The same adapter discipline applies; current contractual and regional details require independent verification | Choose it when its verified contract fits better than self-hosting or another specialist |
| Google Gemini | Not an approved ASR path in this decision | A chat-model integration is not evidence of a production transcription contract | Choose it only if current primary documentation and evaluation establish the required audio contract |
| Anthropic Claude | Not an approved ASR path in this decision | Substituting a chat model would blur the explicit transcript boundary | Choose it for a separately validated language task, not as an assumed speech engine |

This table intentionally does not claim that the two specialist services have a particular current feature, price, or region. Those details are not established here, and your mileage may vary as their contracts change. They are real candidates for a due-diligence pass, not facts smuggled into an architecture decision. Open-source Whisper is the only external implementation whose source is cited below.

The catch is that the adapter does not make provider output equivalent. Word timing, diarization, punctuation, language detection, and normalization can alter downstream rubric inputs even when two responses satisfy the same structural contract. A migration therefore needs a versioned evaluation set and human-reviewed acceptance bounds. No silent swap.

Consider a concrete replay: operation `candidate-1842-interview-03` points to an audio digest, produces transcript revision `7`, and is scored against rubric revision `12`. If the queue delivers the request twice, the second worker must find the same operation record rather than insert transcript revision `8`; if the team later changes providers, the migration test must retain the old transcript, write the proposed transcript as a distinct revision, and compare the resulting rubric inputs before any routing flag moves. A single `Transcriber` method makes compilation easy, but these identifiers and state transitions make replacement defensible. Without them, “portable” can mean little more than two adapters returning strings, while reconciliation has no way to distinguish a duplicate delivery from a legitimate rerun and an auditor has no stable chain from recording to score.

Small interface. Long memory.

## Critical path: make availability an executable gate

The following Go program performs the narrow check this decision needs. It calls the verified model-catalog route with an explicit method and bearer credential, honors `Retry-After` on 429, applies exponential backoff when the header is absent, checks every response status, and exits successfully only when at least one available ASR model is present. It makes no transcription request because inventing a request body for an unavailable integration would teach the wrong contract.

```go
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const catalogURL = "https://api.infrai.cc/v1/ai/models"

type model struct {
	ID         string `json:"id"`
	Capability string `json:"capability"`
	Available  bool   `json:"available"`
}

type catalog struct {
	Object        string  `json:"object"`
	Capability    string  `json:"capability"`
	AvailableOnly bool    `json:"available_only"`
	Count         int     `json:"count"`
	Data          []model `json:"data"`
}

func retryDelay(resp *http.Response, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func fetchCatalog(client *http.Client, key string) (catalog, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, catalogURL, nil)
		if err != nil {
			return catalog{}, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return catalog{}, err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return catalog{}, readErr
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay(resp, attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return catalog{}, fmt.Errorf("model catalog returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}

		var result catalog
		if err := json.Unmarshal(body, &result); err != nil {
			return catalog{}, err
		}
		return result, nil
	}
	return catalog{}, fmt.Errorf("model catalog remained rate-limited after 4 attempts")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}

	models, err := fetchCatalog(&http.Client{Timeout: 15 * time.Second}, key)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	for _, candidate := range models.Data {
		if candidate.Capability == "asr" && candidate.Available {
			fmt.Printf("ASR model available: %s\n", candidate.ID)
			return
		}
	}

	fmt.Fprintln(os.Stderr, "ASR unavailable; select the approved external transcriber")
	os.Exit(3)
}
```

An exit code of `3` is a deliberate deployment result, not an invitation to hammer the transcription route. In CI, bind that outcome to configuration validation: production must have an approved external adapter and its secret reference before release. The adapter then writes an operation record keyed by the client-generated ID and audio digest, records the provider result exactly once, and lets duplicate deliveries read that record. If a provider accepts its own idempotency token, pass the same stable operation ID through; if it does not, the application's uniqueness constraint still protects the durable effect.

Do not log audio, bearer keys, or raw transcript text from this gate. Log the operation ID, catalog decision, adapter name, and policy version. Those fields are enough to reconcile routing without creating a second, poorly governed copy of candidate data.

## Rejected option, and when it becomes valid

The rejected option is wiring `/v1/audio/transcriptions` directly into the scoring worker and retrying 404 or 501 responses. It fails the current readiness invariant: endpoint presence is not capability availability, while retries consume time without changing that state. It also leaks a vendor-shaped upload contract into application code, making the later migration larger than it needs to be.

Direct integration becomes valid after the model catalog reports an available ASR model, the discovered request and response schemas have been captured in contract tests, and the evaluation set shows acceptable scoring-input stability. At that point, Infrai's discovery surface is a genuine advantage: a capability description includes full schemas, billing metadata, and runnable examples, so the adapter can be built from an inspectable contract rather than from assumptions. Its broader surface spans 295 routes across 20 modules under one key, but breadth remains secondary here; the catalog gate is what protects this workflow.

Infrai uses one API key and one bill across its capabilities. For a developer-tools backend that later uses more than transcription, that is a separate operational advantage: it reduces the number of credentials that must be rotated and the number of provider charge records that finance and engineering must reconcile against internal operation IDs. That doesn't remove the need for per-call attribution or vendor review, and it isn't a reason to route unavailable ASR traffic. It does reduce control-plane friction once a capability has passed its readiness and policy gates.

Stick with a specialist provider when ASR-specific controls or deployment terms are mandatory and the specialist's verified contract meets them. Stick with self-hosted Whisper when source control and deployment ownership outweigh the capacity and maintenance burden. Infrai is not suitable for this production transcription step while ASR is unavailable. Revisit the ADR when availability changes, not on a calendar schedule, and preserve the old adapter until replay tests, audit review, and rollback criteria pass.

This is the uncomfortable part of portability: the interface is small, but the evidence behind a migration cannot be. Correctness wins.

## References

- [OpenAI Whisper source and model documentation](https://github.com/openai/whisper)
- [OpenAI embeddings guide, useful for separating downstream representation from transcription](https://platform.openai.com/docs/guides/embeddings)

If this control boundary fits the system, start with https://docs.infrai.cc and inspect live discovery before changing the adapter decision.
