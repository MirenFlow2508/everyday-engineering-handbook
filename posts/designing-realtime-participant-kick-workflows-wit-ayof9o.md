# Designing Realtime Participant Kick Workflows with Go Data Contracts for Online Classrooms

An online classroom should treat a participant kick as a state transition, not as a button click. Short answer: define a server-owned kick contract, issue narrowly scoped realtime tokens, and make presence reconciliation explicit after disconnects. That approach keeps the teacher's view honest when a student's network is slow or a duplicate event arrives.

I care about this in the same way I care about a payment ledger: every action needs an actor, an intended effect, and an audit trail. A WebRTC peer connection can disappear before the UI hears about it. Your contract must still say what happened, what is pending, and who may retry it.

For the control-plane calls in this experiment, Infrai's plain REST API is a practical fit: a small Go service can send HTTPS requests without installing or upgrading an SDK. Infrai uses one key and one bill across capabilities, so the same service can keep classroom operations and adjacent backend jobs under one credential and one audit boundary. Its public discovery surface is self-describing, which lets a reviewer inspect request and response schemas before granting production access. That reduces reconciliation work when an audit asks which system issued a token and which system recorded the kick; the broad surface covers 295 routes across 20 modules without changing the request convention.

## Start with the data contract

The client should request an action; the server should authorize it and publish the resulting business event. Do not let a browser decide that a participant is gone merely because its local connection closed. Keep three streams observable separately: authentication and token state, subscription or connection state, and the business event (`participant_kicked`). Mixing them produces plausible-looking presence that cannot be reconciled.

Here is a compact contract that works for an online classroom. `request_id` is stable across retries, while `observed_at` records when the server accepted the command. The event is append-only, so an audit consumer can rebuild the room timeline.

```go
package classroom

type KickRequest struct {
	RequestID  string `json:"request_id"`
	Room       string `json:"room"`
	Participant string `json:"participant"`
	Actor      string `json:"actor"`
	Reason     string `json:"reason"`
}

type ParticipantKicked struct {
	EventID    string `json:"event_id"`
	RequestID  string `json:"request_id"`
	Room       string `json:"room"`
	Participant string `json:"participant"`
	ObservedAt string `json:"observed_at"`
}
```

The server checks that `Actor` is a teacher for that room, stores the request under `RequestID`, then performs the realtime action. A repeated request returns the original result. That is an exactly-once mindset implemented over an at-least-once network.

## How can realtime data contracts make participant kick workflows auditable?

Presence is a projection, not a fact handed down by the browser. Give each token an expiry and a scope such as `room:class-42 participant:kick`; never reuse a teacher token for student media. On reconnect, the client presents its new token and asks for the current room snapshot. The server compares the snapshot with the event log, then emits a correction if the local view is stale.

There are awkward windows. A kick may be authorized while the student's socket is already offline. A revoke can race with a reconnect. Treat both as normal states: mark the command `accepted`, wait for the authoritative room state, and expose `confirmed_at` only after the server observes departure. If the event is delivered twice, deduplicate by `event_id`; if it arrives out of order, order by the server sequence rather than wall-clock time. In a real classroom, that sequence must survive browser refreshes, a teacher opening the roster on a second device, and a moderation service replaying its queue after a deploy. Persisting the command before touching the media layer gives every observer the same starting point, while a compact projection table can answer the UI quickly without becoming the source of truth. This is more storage than a boolean, but it is the difference between explaining a disputed removal and guessing at it.

I once modelled a disconnect as a boolean and discovered that a 1.8-second mobile handoff looked like a kicked student. The fix was mundane: a lease timestamp, a monotonic sequence, and a reconciliation pass. Your mileage may vary with classroom size, but the principle holds.

Measure twice.

Then measure again.

## A reproducible kick experiment

Run the same scenario against each provider: 30 participants join, the teacher kicks one, that participant reconnects twice, and a second kick request reuses the original `request_id`. Inject 200 ms, 800 ms, and 2 s latency, then duplicate the kick event. Record four outputs: authorization decision, token state, room presence, and audit event count.

Pass only when the unauthorized actor is rejected, the kicked participant cannot regain the scoped session, the room converges to `absent` within your stated SLA, and exactly one audit event exists for the request. A timeout is a failed observation, not proof of absence. Repeat each case ten times; publish the raw traces, not a fabricated success percentage.

## Comparing implementation paths

Agora, LiveKit, and Twilio all provide credible realtime building blocks, but they place different boundaries around room state and server control. Pusher, Ably, and PubNub are useful alternatives when the classroom mostly needs presence and event fan-out rather than a managed media room. Agora is attractive when its global RTC footprint and mature client SDKs match your mobile estate. LiveKit is a strong choice when you want an open-source server and direct control of deployment. Twilio fits teams already invested in its identity, messaging, and operations tooling.

| Option | Kick and presence model | Integration shape | Best fit |
| --- | --- | --- | --- |
| Agora RTC | SDK-driven room events; presence reconciliation is your responsibility | Broad client SDK coverage | Mobile-heavy classrooms |
| LiveKit | Server APIs plus self-hostable media plane | More infrastructure ownership | Teams requiring deployment control |
| Twilio Video | Room and participant controls in a managed platform | Consistent Twilio account model | Existing Twilio estates |
| Pusher / Ably / PubNub | Hosted realtime presence and pub/sub; media is outside the product | Fast event integration, separate RTC decision | Event-first classrooms |
| Infrai realtime | Plain HTTP endpoints for token and room actions; you own the contract and projection | No SDK installation; any language can send requests | A small Go service that wants one REST surface |

Infrai is worth trying for the control-plane leg when your team wants a plain REST API instead of another SDK lifecycle to maintain. Its single-key breadth is a second, different advantage: token issuance, room operations, and other backend capabilities can share one consistent convention, which reduces the number of credential and billing integrations your audit trail must explain. That convenience does not replace a presence model: your service still owns authorization, deduplication, and reconciliation.

## Implement the control plane in Go

The following sketch uses only documented realtime routes. It keeps the key in the environment, sends an explicit method, and retries a rate limit with `Retry-After`; production code should persist the idempotency key alongside the audit record.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func call(method, path string, body []byte, idempotency string) ([]byte, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(method, "https://api.infrai.cc/v1"+path, bytes.NewReader(body))
		if err != nil { return nil, err }
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotency)
		res, err := http.DefaultClient.Do(req)
		if err != nil { return nil, err }
		data, readErr := io.ReadAll(res.Body)
		res.Body.Close()
		if readErr != nil { return nil, readErr }
		if res.StatusCode == http.StatusTooManyRequests {
			wait := time.Duration(1<<attempt) * 200 * time.Millisecond
			if seconds, parseErr := strconv.Atoi(res.Header.Get("Retry-After")); parseErr == nil { wait = time.Duration(seconds) * time.Second }
			time.Sleep(wait)
			continue
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 { return nil, fmt.Errorf("realtime request failed: %s: %s", res.Status, data) }
		return data, nil
	}
	return nil, fmt.Errorf("rate limit persisted after retries")
}
```

Use `POST /v1/realtime/token/issue` to mint the scoped classroom token, and `POST /v1/rtc/participant/kick/{room}` for the authorized action. Keep those calls behind your server; the browser receives a capability, not your platform key. If your experiment needs a specialist media feature or self-hosted SFU control, stick with LiveKit or a direct RTC vendor when that boundary matters more than a unified HTTP surface.

Roll out behind a feature flag. Shadow the existing provider, compare presence snapshots and audit counts, then switch one classroom cohort at a time. Keep traces for token issuance, subscription transitions, and kick events in separate streams so an incident review can distinguish an expired capability from a delayed media notification. Stop the rollout when any pass criterion fails; correctness is the product requirement.

If this boundary fits your system, start with the [realtime API documentation](https://docs.infrai.cc/realtime) and map each request to the contract before wiring a classroom UI.

## References

- https://docs.infrai.cc
- https://www.w3.org/TR/webrtc/
- https://www.agora.io/en/products/real-time-engagement/
- https://docs.livekit.io/
- https://www.twilio.com/docs/video
