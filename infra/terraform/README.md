# Infrastructure

Terraform for the GCP deployment: Pub/Sub, Cloud SQL, BigQuery, Cloud Run,
IAM, Workload Identity Federation, the SLO and the canary release.

> ## This has never been applied
>
> There is no GCP project behind this repository and no spend budget, so
> **none of this has been through `terraform plan` or `terraform apply`
> against a real API.** It is validated, not deployed.
>
> That distinction is the same one the rest of the project insists on, so it
> is stated here rather than implied. What *is* checked, on every push:
>
> * `terraform fmt -check -recursive`
> * `terraform init -backend=false`, which resolves the real providers
> * `terraform validate`, which type-checks every resource against the
>   provider's actual schema and resolves the dependency graph
>
> What that catches is substantial — it already caught a dependency cycle
> between the push subscription and the Cloud Run service it delivers to,
> which no amount of re-reading would have found. What it cannot catch is
> anything the API only rejects at apply time: a quota, an org policy, an IAM
> role that does not grant what its name suggests, a field that is valid in
> isolation and refused in combination. **Assume a first apply will need
> fixes.**

## Layout

| File | What it holds |
|---|---|
| `versions.tf` | Provider pinning and the GCS backend |
| `variables.tf` | Inputs, and the environment shared by every workload |
| `names.tf` | Resource names, computed once — see the cycle note below |
| `pubsub.tf` | Topics, the push subscription, the DLQ and its two IAM grants |
| `storage.tf` | Cloud SQL, BigQuery, the snapshot bucket |
| `network.tf` | Private VPC, the SQL peering range, the Cloud Run connector |
| `iam.tf` | One service account per workload |
| `run.tf` | Four services and three jobs, all from one image |
| `canary.tf` | Traffic splitting, and why only the processor gets it |
| `scheduler.tf` | The collector tick and the maintenance jobs |
| `monitoring.tf` | The SLO restated for Cloud Monitoring, burn-rate alerts |
| `wif.tf` | Keyless CI auth, scoped to this repository |

## Bootstrap

The state bucket cannot be created by the configuration that stores its state
in it, so it is made once by hand:

```bash
gcloud storage buckets create "gs://${PROJECT_ID}-tf-state" \
  --location=us-east1 --uniform-bucket-level-access
gcloud storage buckets update "gs://${PROJECT_ID}-tf-state" --versioning
```

Then:

```bash
terraform init -backend-config="bucket=${PROJECT_ID}-tf-state"
terraform apply \
  -var="project_id=${PROJECT_ID}" \
  -var="image=us-east1-docker.pkg.dev/${PROJECT_ID}/transitpulse-dev/transitpulse@sha256:..."
```

The image must be pinned by digest; a variable validation rejects a tag.

## Three things worth reading the comments for

**The dead-letter policy needs two IAM grants that are not implied by
configuring it.** Pub/Sub performs the move as its own service agent, which
needs `publisher` on the dead-letter topic and `subscriber` on the source
subscription. Without them the move fails silently — messages are redelivered
for ever, the DLQ stays empty, and nothing reports a permissions problem. This
is the most common way a dead-letter policy is configured and does nothing.

**Names are computed, not read back off resources.** The push subscription's
endpoint is the processor's URI, and the processor's environment names the
subscription it consumes — a cycle Terraform refuses to plan. Neither resource
needs the other to exist, only its name, so `names.tf` decides the names once.

**`canary_percent` defaults to 0, and that is the useful default.** An apply
with a new image creates the revision, starts it, runs its startup probe and
sends it no traffic. Promoting is a second decision; rolling back is setting
the number to 0, with no rebuild, because the previous revision was never
replaced.

## What is deliberately not here

- **A migration job.** The deploy workflow calls one and warns if it is
  absent. Schema changes during a canary mean two revisions share a database,
  so migrations have to be backward compatible with the revision still
  serving — that is a constraint on how migrations are written, and a Cloud
  Run Job that applies them blindly would hide it.
- **The Postgres DSN as an output.** It lives in Secret Manager. An output
  would put it in plaintext in the state file and in the log of every CI run
  that calls `terraform output`.
- **`fsync=off`.** The local stack runs with it, which is why every write-path
  figure in [BENCHMARKS.md](../../BENCHMARKS.md) carries a caveat. Nothing of
  the sort appears here: Cloud SQL does not expose `fsync`, and
  `synchronous_commit` is left at its default.
