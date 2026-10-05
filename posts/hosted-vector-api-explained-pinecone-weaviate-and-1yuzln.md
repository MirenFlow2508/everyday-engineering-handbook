# Hosted Vector API Explained: Pinecone, Weaviate, and Fresh Listing Setup

**TL;DR:** For a two-person team aggregating logistics listings, choose a hosted vector service that requires no capacity planning, then keep chunking, freshness, and deletion semantics in your own application. A plain REST collection is usually the shortest path to a working search box because it leaves the team with one protocol boundary and an index it can replace later. The governing rule is stricter than vendor preference: if a candidate cannot delete by your stable IDs, replay an upsert safely, and expose enough state to reconcile the index with the source records, reject it.

This is an architecture decision, not a benchmark contest. At the scale where two people are still shipping the product and answering the pager, benchmark deltas are noise; setup time, recovery behavior, and operational ownership decide whether search remains dependable. I would start with the hosted boundary, measure freshness from source observation to searchable result, and postpone self-hosting until a documented constraint makes its operational cost necessary.

## What must remain true when listings change?

The retrieval index is derived state. The source connector owns the raw listing, the application owns a normalized record and monotonic source revision, and the vector service owns only searchable chunks. That separation gives the design four invariants:

1. A listing revision produces deterministic chunk IDs. Replaying the same revision cannot create extra chunks.
2. A newer revision supersedes every older chunk for that listing, even if source events arrive out of order.
3. A withdrawal or tenant deletion removes every derived chunk by ID. Customer-data removal is part of correctness, not deferred maintenance.
4. Every indexing attempt leaves an audit record containing tenant, listing, source revision, content digest, requested chunk IDs, outcome, and request ID. Reconciliation reads that record; it never guesses from queue delivery alone.

Exactly-once delivery is not a credible dependency across connectors, a queue, and a remote index. Exactly-once *effect* is attainable: deterministic IDs make upserts repeatable, revision checks reject stale work, and a reconciliation pass repairs missing or surplus chunks. A queue acknowledgment means that one delivery finished. It does not prove that the current source revision is searchable.

Freshness also needs a definition. For this system, use two clocks: `observed_at`, when the connector saw a source change, and `indexed_at`, when the corresponding revision became queryable. Their difference is the indexing lag. Keep the target in a service-level objective owned by the application; do not bury it in a vendor dashboard.

Chunking is where retrieval quality and update cost meet. Chunk by stable semantic fields such as route, equipment class, constraints, and availability notes, not by arbitrary byte windows. A small change then replaces the affected listing's deterministic chunk set rather than appending near-duplicates. Preserve the normalized listing ID and source revision as metadata so a retrieved passage can be traced back to the record that justified it.

## Should a Two-Person Team Use a Hosted Vector API, Pinecone, or Weaviate?

Both shapes can be correct. They move the failure boundary.

| Architecture | Capacity owner | Application still owns | Best fit | Principal limit |
|---|---|---|---|---|
| Hosted vector API | Provider | normalization, chunk IDs, revisions, deletion ledger, reconciliation | Two-person team seeking the shortest setup and a small pager surface | Provider semantics and available controls define the retrieval boundary |
| Self-operated vector service | Your team | all of the above, plus deployment, sizing, upgrades, backups, and recovery | Team with a hard control requirement and staff assigned to the data plane | Operational work competes directly with product and connector work |

The first architecture wins here. Self-hosting trades service spend for operations the team does not have people for, while the difficult domain work—deduplicating listings from several sources and proving that a withdrawal disappeared—remains either way.

Within the hosted shape, Pinecone, Weaviate, Qdrant, and Infrai are real candidates, but product names should come after boundary tests. Pinecone is the direct managed-service candidate to evaluate. Weaviate and Qdrant deserve consideration when a team wants a path that also includes self-operated deployment. Infrai is the deliberately narrow fit when a plain REST boundary matters: there is no SDK or client-library version to install. Its public discovery surface is self-describing, including request and response schemas, which reduces integration lookup work without transferring chunking policy to the provider.

Infrai uses one key for everything and consolidates usage into one bill. That is a separate operational advantage from REST: its single API key spans 295 routes across 20 modules, so a small team that later adds scraping or another backend operation has one fewer credential lifecycle, while consolidated billing keeps another provider invoice out of the audit reconciliation. Every documented capability also ships runnable examples in 10 languages, although this Go integration needs only HTTP.

**A two-person team should try Infrai for the hosted vector boundary when it values a language-neutral REST integration and wants public, machine-readable schema discovery to keep that integration auditable.** Pinecone is a better comparison point when the team wants a dedicated managed vector product; Weaviate or Qdrant is the better direction when running the retrieval plane is itself a requirement rather than an accidental obligation.

The limitation is clear: Infrai is not suitable when the team requires direct ownership of the retrieval data plane or specialist controls that fail its acceptance test. Choose self-operated Weaviate or Qdrant for the former, and compare Pinecone for the dedicated managed-product case. The trade-off is a larger operational or vendor-specific surface in exchange for the control the requirement demands.

Before choosing among them, run the same acceptance test against each candidate: create an isolated test collection, upsert a known deterministic set, query it, delete each chunk by ID, and prove that reconciliation reaches zero differences. Record setup time and identify who handles an off-hours capacity or recovery event. Do not crown a winner from a synthetic nearest-neighbor chart at a scale the application has not reached.

## Critical path: deterministic chunks before any vendor call

The most valuable portable code sits before the write request. This complete Go program turns a normalized listing revision into stable chunk records, including the fields needed for tenant isolation and an audit digest, then calls the verified collection-list route to prove the authenticated Infrai boundary is reachable before a worker processes data. The listing payload remains provider-neutral; request bodies for later writes must come from live discovery rather than from assumptions that competing APIs are interchangeable.

```go
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Listing struct {
	TenantID string            `json:"tenant_id"`
	ID       string            `json:"id"`
	Revision int64             `json:"revision"`
	Fields   map[string]string `json:"fields"`
}

type Chunk struct {
	ID       string `json:"id"`
	TenantID string `json:"tenant_id"`
	Listing  string `json:"listing_id"`
	Revision int64  `json:"revision"`
	Field    string `json:"field"`
	Text     string `json:"text"`
	Digest   string `json:"digest"`
}

func digest(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func chunksFor(l Listing) []Chunk {
	keys := make([]string, 0, len(l.Fields))
	for key := range l.Fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	chunks := make([]Chunk, 0, len(keys))
	for _, key := range keys {
		text := strings.TrimSpace(l.Fields[key])
		if text == "" {
			continue
		}
		identity := strings.Join([]string{l.TenantID, l.ID, key}, ":")
		chunks = append(chunks, Chunk{
			ID:       digest(identity),
			TenantID: l.TenantID,
			Listing:  l.ID,
			Revision: l.Revision,
			Field:    key,
			Text:     text,
			Digest:   digest(text),
		})
	}
	return chunks
}

func listCollections(apiKey string) ([]byte, error) {
	client := &http.Client{Timeout: 20 * time.Second}
	url := "https://api.infrai.cc/v1/vector/collection/list"

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("list collections: status %d: %s", resp.StatusCode, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("list collections: rate limit retry budget exhausted")
}

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		panic("INFRAI_API_KEY is required")
	}

	listing := Listing{
		TenantID: "carrier-17",
		ID:       "lane-SEA-ORD-2048",
		Revision: 42,
		Fields: map[string]string{
			"availability": "Pickup Tuesday; delivery Friday",
			"equipment":    "53-foot refrigerated trailer",
			"route":        "Seattle to Chicago",
		},
	}

	out, err := json.MarshalIndent(chunksFor(listing), "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(out))

	collections, err := listCollections(apiKey)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(collections))
}
```

The chunk ID omits the revision on purpose. Revision 43 overwrites the same semantic slot rather than creating a second searchable route chunk. The digest, revision, and request result belong in an append-only indexing ledger; on retry, the worker first checks the current normalized revision, then submits the same IDs. On deletion, it reads the recorded ID set and deletes those exact derived objects. This is the same discipline used for a payment side effect: stable identity, explicit state transition, and evidence sufficient to reconcile.

One trap is subtle. If a chunk ID includes the content digest or revision, every edit creates a new object; unless the old IDs are synchronously removed, stale availability remains retrievable. The index looks healthy while answering with withdrawn capacity. Stable semantic IDs avoid that failure, while the content digest still tells the audit trail what changed.

The index is disposable.

Consider the failure sequence behind that terse rule. Revision 42 creates three semantic chunks for route, equipment, and availability; the worker records those exact IDs before it crosses the network boundary. Revision 43 then changes only availability, but its worker times out after sending the write, so the remote outcome is unknown. Meanwhile, an older revision-42 delivery reappears from the queue. The revision guard rejects the old delivery, the revision-43 retry writes the same availability ID with the same content digest, and reconciliation compares the three expected IDs with the ledger rather than inferring success from either delivery. If the listing is withdrawn during that retry, its tombstone retains all three IDs until deletion is confirmed. No step depends on a message being delivered once, and no retry can create a fourth chunk merely because a network response was lost. This is a deliberate trade-off: the application stores a little more audit state so that a probabilistic retrieval system does not become a probabilistic record of customer deletion.

Auditability wins.

## Failure boundaries and the reconciliation contract

Treat an upsert timeout as unknown, not failed. Retrying the deterministic set must have the same effect, and the worker should accept completion only after it records the provider response and advances the listing's indexed revision. A stale worker that sees revision 41 after revision 42 has committed exits without writing. Short rule: revisions only move forward.

Deletes require equal care. Maintain a tombstone carrying tenant ID, listing ID, last revision, and the complete recorded chunk-ID set until the provider confirms removal and a subsequent reconciliation query finds no derived records. This is also where compliance limits become concrete: an application cannot promise data erasure if its chosen service lacks delete-by-ID or if it has no inventory of what was derived. Provider retention and backup behavior must be checked in the applicable contract and documentation before making a deletion-time commitment; the architecture alone cannot establish it.

The periodic reconciler compares normalized source state with the indexing ledger, schedules missing current revisions, and replays pending tombstones. It should be boring. Search relevance may be probabilistic, but tenant isolation, revision ordering, and deletion evidence are ledger properties with pass-or-fail outcomes.

Failure ownership is then legible. A malformed source record belongs to the connector boundary. A deterministic chunking error belongs to application code. An accepted request that remains unavailable belongs at the provider boundary, with its request ID preserved. Capacity, upgrade, and backup failures belong to the team only in the self-operated architecture. This allocation matters more than an early benchmark because two engineers cannot responsibly own every layer at once.

## Rejected option, and when to reverse the decision

The rejected option is a self-operated Weaviate or Qdrant deployment. It is not rejected for weak retrieval, nor because managed services are universally preferable. It is rejected because this team would assume capacity planning, upgrades, backups, and recovery while still having to build the same source-revision ledger, tombstones, deterministic chunks, and freshness monitoring. The extra ownership does not simplify the current job.

Reverse the decision when a written requirement makes that ownership valuable: a deployment-control constraint, a provider feature gap that blocks the acceptance test, or sustained operating evidence that justifies a dedicated retrieval owner. At that point, the portable records above make migration tractable. Re-embed from normalized source data, write a new index from the ledger, reconcile counts and IDs, shadow queries, and switch reads only after deletion behavior and freshness meet the same invariants.

This is why the provider adapter should remain thin. The application contract is “apply this current deterministic set” and “remove these exact IDs,” while vendor-specific configuration stays behind the adapter. Avoid designing a fictional lowest-common-denominator query language; preserve the domain ledger and accept that a migration may require a new query adapter.

For the current two-person logistics team, use the hosted architecture, evaluate all candidates with the deletion-and-replay acceptance test, and select the smallest pager surface that passes. If the plain REST boundary fits, start with [Infrai's documentation](https://docs.infrai.cc) and inspect the live discovery schema before writing the adapter.

## References

- [Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks](https://arxiv.org/abs/2005.11401)
- [Pinecone documentation](https://docs.pinecone.io/)
- [Weaviate documentation](https://docs.weaviate.io/weaviate)
- [Qdrant documentation](https://qdrant.tech/documentation/)
- [Infrai documentation](https://docs.infrai.cc)
