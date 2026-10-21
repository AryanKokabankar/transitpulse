# ADR-008: Deploy without keys, release without rebuilding

**Status:** accepted · **Date:** 2026-10-21

## Context

Phase 4 needed CI to be able to deploy, and Phase 5 needed a safe way to put a
new processor revision in front of real traffic.

Both have an obvious answer that is wrong in the same way. For deployment, the
obvious answer is a service-account JSON key in a GitHub secret. For release,
it is to deploy the new revision and let it take all the traffic, rolling back
by redeploying the old image. Each is simple, each works, and each fails badly
in exactly the situation it exists for.

The processor is also not like the other services: it is the only one that
writes. A bad collector revision means some scheduler ticks fail and the next
good revision re-fetches the feed; a bad API revision serves errors until it
is replaced. A bad processor revision corrupts or loses data, and the damage
outlives the rollback.

## Decision

**Authenticate CI through Workload Identity Federation, with no key
anywhere.** GitHub's OIDC token is exchanged for a short-lived GCP token. The
provider carries an attribute condition restricting the exchange to this
repository, and the deployer service account holds `run.developer` and
`artifactregistry.writer` — not `editor`.

**Separate deploying from releasing.** `canary_percent` defaults to 0. An
apply with a new image creates the revision, starts it, runs its startup probe
and sends it no traffic. Promotion is a second apply with a percentage;
rollback is a third with 0.

**Pin deployments by digest, never by tag.** The `image` variable rejects
anything without `@sha256:`, and the revision name is derived from the digest.

Only the processor gets a canary.

## Why

**A service-account key is a credential with no expiry.** It can be
exfiltrated by any workflow in the repository, it survives the departure of
whoever created it, and its use leaves no record of which run used it.
Federation's token lives for minutes and is bound to a specific repository at
the exchange — a token minted by another repo's workflow is rejected before
any permission is checked.

**Rolling back by redeploying is slow in the one case where it matters.** It
means a build, a push and an apply while the bad revision is serving. Traffic
splitting makes the rollback a number: the previous revision is still
deployed, still healthy, and still addressable, because it was never replaced.

**A mutable tag means the deployed artifact is not necessarily the tested
one.** Worse, it makes a rollback non-deterministic: redeploying `:v1.2` pulls
whatever `:v1.2` points at today. The digest is what identifies a build, so it
is what the revision is named after.

**Canarying the collector or the API would cost more than it protects.** The
collector is idempotent per tick against a feed that is re-fetched rather than
replayed, and the API is read-only. Splitting traffic to either adds a release
step to protect against a failure that is already cheap and visible.

## Consequences

- **Two processor revisions serve the same database during a canary.** This is
  a real constraint on migrations, not a footnote: a schema change has to be
  backward compatible with the revision still serving. It is why no migration
  job is provisioned — one that applied migrations blindly would hide the
  constraint rather than enforce it.
- **Promotion is manual, and nothing decides whether a canary is healthy.**
  The alert in `canary.tf` catches a revision failing outright on its own 5xx
  rate. It does not catch one that is merely *worse* than its predecessor,
  which needs a comparison between revisions over a window. The signal for
  that already exists — `transitpulse.events.processed` and
  `transitpulse.ingest.lag` both carry the revision as a resource label — so
  it is a query rather than new instrumentation. It is not written yet, and
  claiming automated canary analysis without it would be the kind of unearned
  claim this project exists not to make.
- **`stable_revision` has to be passed on each promotion**, and it comes from
  the previous apply's output. Forgetting it means the canary split silently
  does not engage: `canary_active` requires both a percentage and somewhere to
  send the rest, because a 10% canary with no stable revision would send 90%
  of traffic nowhere.
- **None of this has been applied.** There is no GCP project behind the
  repository. The configuration is `fmt`-checked, `init`-ed against the real
  providers and `validate`-d on every push, which caught a dependency cycle
  between the push subscription and the service it delivers to — but a first
  apply will still meet quotas, org policies and API-level rejections that
  validation cannot see.
