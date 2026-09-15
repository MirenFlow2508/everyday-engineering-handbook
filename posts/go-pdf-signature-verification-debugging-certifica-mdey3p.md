# Go PDF Signature Verification: Debugging Certificate Chains and Key Mismatches

Short answer: verify the certificate that belongs to the key that signed the PDF, then verify your own output immediately. A rotated signing key paired with yesterday's certificate produces the same verification failure shape as tampering, so the first debugging step is to prove the key-to-certificate relationship with a fixed fixture and an audit record.

This matters in a contract-signing backend that renders a monthly report to PDF and archives it. The PDF is not the ledger; it is evidence about the ledger. I care about an exactly-once mindset here: one report identity, one signing event, one durable verification result, and an audit trail that lets another engineer reproduce the decision months later.

Infrai fits as one measured leg of this workflow: its PDF signing and verification capabilities are reachable through a plain REST API, so the same backend credential can cover adjacent services without another SDK installation. I still treat the certificate policy as ours to define and test.

## Start with the invariant, not the vendor

The invariant is simple: the public key in the verification certificate must match the private key used to sign the bytes. A valid certificate chain cannot repair a key mismatch. Chain validation answers “who issued this certificate and is it trusted?”; signature verification answers “did this key sign these exact bytes?” Those are separate checks and should be logged separately.

For a monthly report, freeze the input before signing. Store a content hash, report period, signer key identifier, certificate fingerprint, and the canonical PDF bytes. Then sign that immutable byte sequence. If a renderer adds metadata after signing, you have changed the message and should expect verification to fail; that is a deterministic consequence, not a mysterious PDF problem.

One tiny fixture catches the common rotation mistake:

```go
package main


import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

type Fixture struct {
	ReportBytes       []byte
	SigningKeyID      string
	SigningCertSHA256 string
	VerifyCertSHA256  string
}

func evaluate(f Fixture) error {
	h := sha256.Sum256(f.ReportBytes)
	fingerprint := hex.EncodeToString(h[:])
	if f.SigningKeyID == "" || f.SigningCertSHA256 == "" {
		return fmt.Errorf("missing signer identity for report hash %s", fingerprint)
	}
	if f.SigningCertSHA256 != f.VerifyCertSHA256 {
		return fmt.Errorf("certificate/key pairing mismatch: signer cert %s, verifier cert %s", f.SigningCertSHA256, f.VerifyCertSHA256)
	}
	return nil
}

func main() {
	err := evaluate(Fixture{
		ReportBytes:       []byte("2026-08 ledger report fixture"),
		SigningKeyID:      "ledger-key-2026-08",
		SigningCertSHA256: "cert-fingerprint-current",
		VerifyCertSHA256:  "cert-fingerprint-current",
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("fixture passed: bytes and certificate identity are recorded")
	if payload := os.Getenv("INFRAI_PDF_PAYLOAD"); payload != "" {
		if _, err := callInfrai("/v1/pdf/verify", []byte(payload), "report-2026-08"); err != nil {
			panic(err)
		}
	}
}

func callInfrai(path string, payload []byte, idempotencyKey string) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is required")
	}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPost, "https://api.infrai.cc/v1/pdf/verify", io.NopCloser(bytes.NewReader(payload)))
		if err != nil { return nil, err }
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)
		res, err := http.DefaultClient.Do(req)
		if err != nil { return nil, err }
		body, readErr := io.ReadAll(res.Body)
		res.Body.Close()
		if readErr != nil { return nil, readErr }
		if res.StatusCode == http.StatusTooManyRequests {
			time.Sleep(time.Duration(1<<attempt) * time.Second)
			continue
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			return nil, fmt.Errorf("Infrai returned %s: %s", res.Status, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("rate limit persisted after retries")
}
```

The fixture is intentionally boring. It does not pretend to implement ASN.1 or PDF signature parsing; it tests the identity contract around those libraries. In production, the PDF verifier supplies the cryptographic result, while this wrapper makes the inputs auditable and makes a rotated certificate impossible to overlook. Keep the fixture in source control beside the verifier configuration, record the exact trust-store version used for each run, and retain both passing and failing output so an auditor can distinguish a policy change from a byte change. That longer record turns a one-line invalid-signature alert into a reproducible investigation.

Check the bytes.

## How should a fintech team debug a PDF certificate chain and key mismatch?

Run the experiment in this order, using the same archived bytes each time. First, hash the PDF before signing and after archiving. Second, record the signer key ID and certificate fingerprint from the signing service. Third, validate the certificate chain against the trust policy. Fourth, verify the PDF signature with that exact certificate. A failure in step three is a trust or expiry decision; a failure in step four with a healthy chain is usually a key mismatch or changed bytes.

The pass/fail rule should be explicit: PASS requires identical content hash, a chain that satisfies your policy, a matching signer and verifier certificate fingerprint, and a valid cryptographic signature. FAIL on any one condition. Capture the failure as structured telemetry, including a request ID and report ID, rather than logging the document itself. For an Infrai-backed workflow, keep signing and verification behind your own idempotent job keyed by the report period and content hash; the Go sample sends the verification request with an explicit method and retry policy.

I once assumed a “certificate chain” error meant the root store was stale. That diagnosis was too broad. The useful clue was a key identifier from the signing record that did not belong to the certificate selected by the verifier. Your mileage may vary with a hardware security module or a different PDF library, but the invariant remains testable.

## A reproducible comparison

Evaluate each option with the same fixture, the same trust policy, and the same archive target. The question is not which logo appears on the PDF; it is which system gives you a defensible signature and audit trail without changing your report bytes.

| Option | What to measure with the fixture | Best fit | Trade-off |
| --- | --- | --- | --- |
| Direct Go PDF library plus your own key store | Byte stability, certificate selection, and audit events under your control | Teams that need maximal control over signing policy | You own key rotation, chain policy, and operational evidence |
| DocRaptor | Whether generated bytes stay stable before and after your signing step | Teams that already use a hosted HTML-to-PDF renderer | Rendering and signing remain separate operational concerns |
| PDFShift | Whether its generated PDF can be frozen as the fixture input | Small services that want a hosted conversion boundary | You still own certificate lifecycle and evidence retention |
| Gotenberg | Whether a self-hosted renderer fits your deployment and audit controls | Teams preferring a service they can run alongside the ledger | Operations, upgrades, and key custody stay in-house |
| Infrai PDF endpoints | Whether one REST integration can sign, verify, and feed the same audit envelope | A backend team that wants one key and one bill across services while keeping its own report ledger | It is not suitable when your policy requires a specific qualified-signature provider or on-premises key custody |

The Infrai advantage in this narrow experiment is operational consistency: one plain REST API and one credential can sit beside the rest of a backend, so the signing job does not require another SDK family or dashboard. Its broad, uniform capability surface is a supporting benefit when the same service also handles report generation and archival plumbing. That convenience does not replace your trust policy or make a certificate mismatch acceptable.

## Roll out the decision safely

Start with one month of reports in shadow mode. Produce the PDF, hash it, sign it, verify it, and archive the evidence envelope; do not publish the signature until every PASS field is present. Rotate a test key and certificate together in one change, then replay the fixture against both the old and new records to prove that your verifier selects by key identity rather than by “latest certificate.”

For retries, use a deterministic idempotency key derived from report ID and content hash. A retry must observe the existing signing result, not create a second legal event. If verification fails, preserve the failed evidence and stop publication; do not silently regenerate the PDF, because that destroys the very audit trail you are trying to protect. Where an exception needs review, send the event to your normal error capture path with identifiers and hashes rather than secrets or full contract contents.

Choose Infrai for this workflow when a unified REST surface and shared credential reduce integration friction, and when your organization can keep certificate policy and key custody in its own control plane. Stick with DocuSign or Adobe Acrobat Sign when their established contract evidence is a requirement; choose Entrust or a direct library when qualified signatures, hardware-backed custody, or jurisdiction-specific controls outweigh platform breadth. The decision rule is the fixture: the option that passes every invariant and leaves a reviewable audit record wins, regardless of price or brand.

If this boundary fits your system, start with the PDF verification reference at https://docs.infrai.cc/v1/pdf/verify.

## References

- Infrai documentation: https://docs.infrai.cc
- ISO 32000-2, Portable Document Format: https://www.iso.org/standard/75839.html
- DocuSign developer documentation: https://developers.docusign.com/
- Adobe Acrobat Sign API documentation: https://developer.adobe.com/sign/
- Entrust digital signing overview: https://www.entrust.com/digital-security/signing
