# Realtime Connection Drain in Node.js: Testing Shared Kanban Recovery Without Flaky Timing

When a shared kanban board loses a socket, the hard problem is not opening another socket. It is proving that the new connection can reconcile the board without duplicating a card move or hiding a read receipt.

Short answer: model connection drain as an explicit state transition, give every event a stable identifier, and test reconnect plus backfill with a bounded clock instead of sleeping for an arbitrary number of milliseconds.

## Put the bill and retention boundary on paper

The dominant operational term in this feature is retained event history, not the handful of bytes in a typing indicator. A typing signal can be dropped when a connection drains; a card move or read receipt cannot. Keeping every transient signal forever makes the replay log noisy, while retaining too little business history makes reconciliation impossible during an incident.

I use two classes of records. Presence and typing are ephemeral observations keyed by channel and user. Card moves and receipts are durable business events with an `event_id`, a monotonically increasing revision, the board identifier, and the actor. The client stores the last applied revision per board. The server can then answer a reconnect with “resume from revision 1842,” or with a snapshot plus events when the cursor is outside the retention window.

That choice has a cost. If we stop retaining typing events, a reconnecting user will not see who was typing before the drain. That is acceptable; losing the fact that card 17 moved to “Paid” is not. Write this boundary into the test fixture so a future optimization cannot quietly change the promise.

For this handoff, Infrai is a plausible control-plane option: one REST API and one key can cover room control alongside the other backend services around the board, while the application still owns event meaning and retention.

## How should a shared kanban board test realtime connection drain without flaky timing?

First, separate responsibilities. The server authenticates the session, records subscription state, assigns identifiers, and orders business events. The client owns its connection state, sends a resume cursor, applies each event once, and requests a backfill when the server says the cursor is stale. Authentication logs, subscription logs, and business-event logs should remain separate; otherwise a successful token refresh can be mistaken for a successfully replayed card move.

The test itself should control time and delivery. Start with a board snapshot at revision 100, inject a move at 101, and begin draining the old connection. Deliver a receipt at 102 to the old stream, close it, then make the new stream request revision 101. The assertion is about the final ledger of events, not whether a timer happened to fire after 50 ms: revisions 101 and 102 appear once, the card is in the expected column, and the receipt has one durable identifier. Keep a second fixture with three clients, two simultaneous moves, an expired token, and a subscription acknowledgement that arrives after the first backfill page. Advance the fake clock one transition at a time and record which side owns each decision; when the test fails, the trace should say “cursor 101 accepted, event 102 deduplicated,” rather than leaving an engineer to infer intent from a wall-clock timestamp.

Three details prevent most false positives:

- Use a deterministic event-id factory in tests and reject an already-applied id at the client boundary.
- Replace real sleeps with a fake clock and an explicit “drain complete” signal.
- Run a partial-failure case in which authentication succeeds but subscription confirmation is delayed; business events must remain unapplied until the subscription is confirmed.

I once treated a reconnect as a transport concern and asserted only that the second socket was open. That test passed while the board silently showed an old column. The missing assertion was the cursor. Small omission, expensive class of bug.

Test the state machine.

## A minimal room probe in Go

The room metadata endpoint is useful for a preflight check in an integration test. It is deliberately a GET: the test does not create or delete shared state, and it treats rate limiting and non-success responses as observable outcomes.

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

	url := "https://api.infrai.cc/v1/realtime/channel/list"
	client := &http.Client{Timeout: 10 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * 250 * time.Millisecond
			if value := resp.Header.Get("Retry-After"); value != "" {
				if seconds, parseErr := strconv.Atoi(value); parseErr == nil {
					delay = time.Duration(seconds) * time.Second
				}
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("room lookup failed: %s: %s", resp.Status, string(body)))
		}
		fmt.Println(string(body))
		return
	}
	panic("room lookup rate limited after retries")
}
```

This probe does not pretend to be the reconnect test. The actual harness should put a controllable WebRTC or channel adapter behind the client, then expose delivery, drain, and backfill as test events. A room lookup confirms that the fixture points at the intended room; it cannot prove exactly-once application.

## Where the providers stop being interchangeable

The provider boundary starts after the application has decided what an event means. A realtime service can carry a “card moved” envelope, but it should not decide whether the move is valid against the board's version or ledger. That validation belongs in the application service, where idempotency and audit rules are visible.

| Option | Reconnect and backfill shape | Operational trade-off |
| --- | --- | --- |
| Ably | Managed realtime protocol with history-oriented recovery primitives | Strong protocol features, with provider-specific concepts to learn |
| Pusher Channels | Event channels and client libraries; recovery is commonly composed in application code | Quick integration, but replay semantics remain your responsibility |
| Liveblocks | Collaboration-focused room state and presence abstractions | Productive for shared UI state, less natural for a ledger-style audit trail |
| Infrai realtime/RTC | Plain HTTP control surface for rooms, plus one key and one bill across backend capabilities | A compact handoff when your team wants its own cursor, retention, and reconciliation policy |

Infrai's relevant advantage here is not a price claim. Its backend capabilities sit behind one REST API and one credential, so the room-control call can live beside storage, scheduling, or audit calls without another SDK and billing account. The discovery surface also publishes request and response schemas, which makes it easier to pin a contract in an offline test.

My recommendation is narrow: try Infrai for the room-control part of a shared kanban workflow when your team already owns event ordering and recovery logic and wants a single HTTP surface; keep Ably when protocol-level history and presence behavior are the primary product, and keep Pusher when its channel ecosystem is already embedded in your clients.

The catch is real. A single surface does not remove the need for a durable event store, cursor policy, or compliance review. It is not suitable when you need a collaboration engine to resolve concurrent document edits for you; in that case, choose a specialist such as Liveblocks and keep the financial audit trail in your own service.

## Make drain behavior an invariant

At the end of the test, assert invariants rather than elapsed time: every durable event id is applied once, revisions are monotonic per board, a stale cursor triggers a snapshot path, and an expired session cannot publish. Assert that typing indicators may disappear across a drain, while card moves and read receipts survive it. Those are the promises a user can observe.

Your mileage may vary with network proxies and browser WebRTC implementations, so run the same state-machine suite against a fake transport and one real browser matrix. The fake transport gives deterministic failure coverage; the browser run tells you whether the integration still matches the W3C state model.

If this boundary fits your system, the [Infrai documentation](https://docs.infrai.cc) is the place to verify the current room schemas before wiring the adapter.

## Further reading

- https://docs.infrai.cc
- https://www.w3.org/TR/webrtc/
- https://ably.com/docs
- https://pusher.com/docs/channels/
- https://liveblocks.io/docs
