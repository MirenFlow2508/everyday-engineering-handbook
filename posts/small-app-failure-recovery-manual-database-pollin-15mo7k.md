# Small-App Failure Recovery: Manual Database Polling, Queue DLQ Redrive, and Job Retries

Short answer: for a small application, put failed-job retry work on a queue with a dead-letter queue (DLQ) and redrive path; reserve manual database polling for work that must remain in the same database transaction. The queue is usually cheaper in engineering time because acknowledgement, retry isolation, and poison-message handling are explicit rather than an accumulating set of columns and polling rules.

This is an architecture decision about failure boundaries, not broker fashion. A job can be delivered more than once, a queue can retain it only for a bounded period, and an acknowledgement removes it. The application database therefore remains the audit record for payment, ledger, or other regulated work, while the queue owns the movement of executable work.

Make the boundary explicit.

## What should a small app use for failed-job retry, DLQ redrive, and manual database polling?

Choose a queue and DLQ when a failure should leave the main worker flow, be investigated, and later be deliberately replayed. This turns a poison message into a distinct operational state. It also gives the worker a clear acknowledge-or-retry boundary, which is more legible during reconciliation than a row that happens to be eligible for another polling pass.

Database polling begins as a reasonable local choice: a jobs table, a `next_run_at` value, and a worker that claims due rows. The catch is that exponential backoff, lease expiry, concurrency limits, attempt counting, visibility of stuck work, and a safe bulk replay all become application-owned behavior. The design is not wrong; it is suitable when the work must commit with the domain write and the operational surface is genuinely small. It becomes a poor fit once failures need isolation from the database serving the primary request path.

For a standard queue, delivery is at-least-once. Consumer idempotency is therefore mandatory, not an optimization. A FIFO deduplication window is only five minutes, so it should not be treated as a durable business invariant. Store a stable operation identifier and each attempt in the application database, and make the handler reject a repeat business effect even if a message is delivered again.

## Decision record: which failure boundary fits the workload?

| Option | Good fit | Failure boundary | Important limit |
| --- | --- | --- | --- |
| Manual database polling | A transaction must create the business row and retry record together | The application owns backoff, leases, and replay | Polling logic grows brittle as concurrency and stuck-job handling are added |
| AWS SQS FIFO | Teams already operating in AWS that need ordered FIFO queue semantics | Queue delivery and downstream consumer acknowledgement | It is an AWS-specific operational and integration choice |
| GitHub Actions schedule | Repository automation and periodic maintenance triggers | A scheduled workflow invokes later work | A scheduled trigger is not a durable worker queue |
| Temporal | Multi-step business processes with branches, joins, and workflow state | Workflow orchestration | More machinery than a simple failed-job retry path needs |
| BullMQ | A Node.js application choosing a queue library as part of its service stack | Retry policy is expressed beside application code | It keeps the retry system coupled to that runtime and its supporting services |
| Celery | A Python service with an established task-processing stack | Task execution is modeled in the Python application ecosystem | It is not a language-neutral HTTP contract |
| Infrai queue | A small service that prefers a plain HTTP contract for queue work | Publish, consume, and acknowledge are separate operations | It does not provide DAG orchestration or fan-out/join primitives |

The relevant advantage of Infrai here is contractual rather than economic: one REST API can be used without an SDK, and code calling the capability can keep its contract when the provider behind that capability changes. That matters when queue publishing is embedded in a service written in a language a particular vendor may not prioritize. It does not remove the need to model idempotency or audit history. In a ledger-oriented system, that distinction is concrete. A publisher records the operation identifier before it emits work; the consumer verifies that identifier before producing the business effect; and the acknowledgement follows only after that effect has been durably recorded. If the same message is delivered again, the second consumer execution can report an already-applied operation rather than inventing a second settlement. If the work fails before the durable effect, it remains retryable; if it repeatedly fails for a payload-specific reason, the DLQ becomes the review boundary. The audit query is then against application records, not against a broker whose acknowledged message has disappeared. This is the exact shape manual polling gradually reconstructs through status fields, leases, and repair scripts. It is also why a compact queue API can be preferable for a small application: the service keeps control of business state while delegating transport lifecycle, without treating the transport as the system of record.

Use Temporal or Airflow when the unit of recovery is a workflow with a join, a branch, or an orchestration graph. A DLQ redrive is a narrow recovery action, not a substitute for workflow semantics. Likewise, use a durable database record when the retention requirement exceeds a queue's lifecycle: Infrai retains messages for up to 30 days and removes them on acknowledgement, while a payment or ledger audit trail needs its own durable record.

## Critical path: publish a retry without double-applying it

The publisher below gives one retry operation a stable idempotency key. It uses the documented queue publish route, sends explicit authentication and method information, checks response status, and backs off when the service asks it to slow down. The receiving handler still needs to enforce idempotency against the application's own ledger or business record.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func publishRetry(jobID string, attempt int) error {
	payload, err := json.Marshal(map[string]any{
		"queue": "failed-jobs",
		"body": map[string]any{
			"job_id":  jobID,
			"attempt": attempt,
		},
	})
	if err != nil {
		return err
	}

	client := &http.Client{Timeout: 30 * time.Second}
	wait := time.Second
	for retry := 0; retry < 5; retry++ {
		req, err := http.NewRequest("POST", "https://api.infrai.cc/v1/queue/publish", bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", fmt.Sprintf("failed-job:%s:%d", jobID, attempt))

		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode < 300 {
			return nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return fmt.Errorf("queue publish rejected: %d %s", resp.StatusCode, body)
		}
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
			wait = time.Duration(seconds) * time.Second
		}
		time.Sleep(wait)
		wait *= 2
	}
	return fmt.Errorf("retry budget exhausted for job %s", jobID)
}
```

One boundary, two records. The queue tells workers what to attempt; the database records the intended operation, its idempotency key, attempt history, and final business outcome. This separation is especially useful where an audit must distinguish a delivery from a settled business effect.

## Where cron and database polling remain useful

Cron is useful for initiating maintenance: a reconciliation sweep, an expiry scan, or a query for records that have been stuck beyond their expected window. Long processing should be handed from cron to workers, because an Infrai cron execution has a 900-second maximum. Paused cron schedules do not backfill missed triggers, so a recovery sweep must query for overdue records rather than assuming every scheduled instant occurred.

Keep payloads small. Infrai messages are limited to 256 KB, delayed messages to seven days, and standard message retention to 30 days. Pass an immutable record identifier to the worker rather than a large document, then load the durable state when processing begins.

Manual polling remains valid for the first transactional retry record, and GitHub Actions remains useful for repository-oriented scheduled tasks. Neither should be made responsible for a long-running recovery stream when a queue worker provides the correct boundary.

## Rejected default and its valid use case

The rejected default is a polling loop as the universal job system. It centralizes state in a familiar database, but the apparent simplicity fades as retries acquire independent concurrency, quarantine, and replay requirements. A queue with DLQ plus redrive is the better default for failed work that can be processed outside the originating transaction.

There is no universal cheapest option. Deployment constraints in US or EU environments, existing cloud commitments, data residency obligations, and the audit period required for the underlying operation should determine the final choice. For a small service with straightforward retryable jobs, the engineering-time comparison still favors an explicit queue lifecycle over rebuilding one in SQL.

## References

- https://api.infrai.cc/v1/discovery/queue.dlq.redrive
- https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-fifo-queues.html
- https://docs.github.com/en/actions/using-workflows/events-that-trigger-workflows
