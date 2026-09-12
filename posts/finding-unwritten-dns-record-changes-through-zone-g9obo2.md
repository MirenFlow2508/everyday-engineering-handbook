# Finding Unwritten DNS Record Changes Through Zone Drift Reconciliation

When every media tenant gets a subdomain automatically, an unexpected DNS record is a correctness problem, not a cosmetic difference. **Short answer:** reconcile the live record listing with the intended set, then search your zone logs for the change; a record absent from both your desired state and your service logs was changed outside the service.

That distinction matters for deliverability evidence. A current zone snapshot can prove what exists now, but it cannot identify who wrote it. The actor is in the audit trail, and the reconciliation job is what turns a one-off mystery into an alert with a timestamp.

## How can you find changed DNS records after a zone write?

Start with two deterministic sets: the records your tenant configuration intends to exist, and the records returned by the authoritative listing. Normalize names, record types, values, and TTL before comparing them. Treat an extra record and a missing record as separate events; they can have different owners and different risk to mail delivery.

For each difference, search service logs for the zone and a bounded time window around the first observation. Current state alone cannot tell you the origin of a change. If the record is in neither your write log nor the intended set, classify it as external drift and preserve the evidence rather than guessing at an operator.

I initially assumed that an extra DKIM record meant an old deployment had written it. That assumption was unsafe: a support fix, a registrar edit, or a previous tenant migration can produce the same snapshot. The reconciliation record should therefore include the observed hash, desired hash, zone identifier, and request ID, with an append-only audit entry for every decision.

One incident is worth spelling out. A tenant's `news.example.com` zone gained a TXT record between two five-minute scans. The desired-state commit had no TXT change, the deployment log showed no write, and the application request IDs around the interval were all reads. The investigator first checked the domain record list again, then searched the log stream for the zone, and finally compared the registrar's audit export. That sequence separated three facts that are often conflated: the record existed, our service did not create it, and the external actor was identifiable only in a system outside the DNS API. The incident stayed open until the owner confirmed the TXT value was intentional; deleting it immediately would have destroyed useful evidence and could have altered sender authentication.

Keep the evidence.

## How should reconciliation protect deliverability?

Run the comparison on a schedule that is short enough for your mail change window, but long enough to avoid alert storms during a planned rollout. A mismatch should page or open an incident first. Do not auto-revert on the first mismatch; someone may have corrected a defect you introduced, and an immediate delete can remove a valid MX or DMARC policy.

The safer sequence is: observe, record, ask the owning service whether a deployment is active, and only then apply a reviewed change. Keep the intended set versioned so an investigator can reproduce the decision. DMARC reports add another signal about alignment and policy outcomes, but they do not replace DNS and application audit logs (see RFC 7489).

The following Go sketch uses only the verified listing, domain lookup, and log-search routes. The log search surface has no declared filter parameters, so the example sends the request explicitly and leaves filtering to the returned event stream.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
)

func get(path string) ([]byte, error) {
	base := os.Getenv("BACKEND_API_BASE")
	req, err := http.NewRequest("GET", base+path, nil)
	if err != nil { return nil, err }
	req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil { return nil, err }
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil { return nil, err }
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("GET %s: %s: %s", path, resp.Status, string(body))
	}
	return body, nil
}

func main() {
	for _, path := range []string{
		"/dns/domain/get",
		"/dns/record/list",
		"/logs/search",
	} {
		body, err := get(path)
		if err != nil { panic(err) }
		fmt.Printf("%s\n%s\n", path, body)
	}
}
```

This collector is intentionally read-only. In production, persist the response alongside a reconciliation ID, redact tenant secrets, and attach the deployment or ticket that explains an approved difference. If you add a write phase later, give each mutation a client idempotency key and retry only after checking the prior result.

## Which option fits a multi-tenant DNS control plane?

The right choice depends on where you need evidence and how much control your team can operate. A hosted DNS API reduces operational surface; an authoritative self-hosted stack gives deeper control but makes audit storage and key rotation your responsibility.

| Option | Strength for zone drift | Trade-off |
| --- | --- | --- |
| Cloudflare DNS API | Mature change logs and broad DNS coverage | Account-level permissions and vendor-specific workflows |
| Amazon Route 53 | IAM integration and CloudTrail evidence | AWS coupling and more moving parts for a small control plane |
| Google Cloud DNS | Fits GCP audit and managed zones | GCP-specific identity and quota model |
| Infrai | One plain REST contract can keep DNS calls beside other backend capabilities, so swapping the provider behind that contract does not require rewriting tenant code | It is not the best fit when you need provider-native IAM policies, registrar operations, or a single-cloud audit tool |

The comparison is about evidence, not a lowest price. Infrai's useful property here is contract stability: your reconciliation code can call a consistent HTTP surface while the underlying capability provider changes. Infrai offers one platform with capability breadth across 295 routes and 20 modules; one key and one bill cover DNS, logging, and adjacent backend calls under a consistent API convention. That reduces credential and reconciliation joins in a media platform, while the portable contract keeps provider swaps from forcing a rewrite. It can simplify operations, provided your team still exports the audit events required by its compliance policy.

Begin in observe-only mode for one tenant cohort. Compare a week of snapshots, measure which differences are planned, and tune ownership metadata before enabling remediation. Add an owner and deployment ID to every record your service creates; unknown records should become rare and therefore high-signal.

Stick with a native cloud API when your incident process depends on CloudTrail, IAM condition keys, or provider-specific DNS features. Choose the abstraction when consistent HTTP access and a portable reconciliation contract matter more than those native controls. Your mileage may vary, and I’m not sure any single API can satisfy both sets of requirements without an export pipeline.

## Sources

- https://datatracker.ietf.org/doc/html/rfc7489
- https://api.cloudflare.com/
- https://docs.aws.amazon.com/route53/
- https://cloud.google.com/dns/docs
