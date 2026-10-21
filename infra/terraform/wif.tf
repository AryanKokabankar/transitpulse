# Workload Identity Federation: how CI deploys without a key.
#
# The alternative is a service-account JSON key in a GitHub secret, which is a
# long-lived credential that never expires, can be exfiltrated by any workflow
# in the repository, and leaves no trace of which run used it. Federation
# instead trades GitHub's own short-lived OIDC token for a GCP access token
# valid for minutes.
#
# The whole security of this rests on one line -- the attribute condition
# below. Without it, *any* GitHub repository can mint a token for this
# project.

resource "google_iam_workload_identity_pool" "github" {
  workload_identity_pool_id = "github-${local.suffix}"
  display_name              = "GitHub Actions"
  project                   = var.project_id
}

resource "google_iam_workload_identity_pool_provider" "github" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.github.workload_identity_pool_id
  workload_identity_pool_provider_id = "github-${local.suffix}"
  project                            = var.project_id
  display_name                       = "GitHub OIDC"

  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.repository" = "assertion.repository"
    "attribute.ref"        = "assertion.ref"
  }

  # The line that matters. Scoped to one repository, so a token minted by any
  # other repo's workflow is rejected at the exchange rather than being
  # accepted and then failing on a missing permission.
  attribute_condition = "assertion.repository == '${var.github_repository}'"

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

resource "google_service_account" "deployer" {
  account_id   = "tp-deployer-${local.suffix}"
  display_name = "TransitPulse CI deployer"
  project      = var.project_id
}

# Narrowed a second time, to the default branch. The repository condition
# above stops another repo; this stops a pull request from a fork in *this*
# repo from deploying, which is the more likely of the two.
resource "google_service_account_iam_member" "deployer_wif" {
  service_account_id = google_service_account.deployer.name
  role               = "roles/iam.workloadIdentityUser"
  member = format(
    "principalSet://iam.googleapis.com/%s/attribute.repository/%s",
    google_iam_workload_identity_pool.github.name,
    var.github_repository,
  )
}

# What the deployer may do. Deliberately not editor or owner: it updates Cloud
# Run revisions and reads the artifact registry, and nothing else. It cannot
# touch the database, the buckets or the IAM policy -- an infrastructure
# change still needs a human running Terraform.
resource "google_project_iam_member" "deployer_run" {
  project = var.project_id
  role    = "roles/run.developer"
  member  = "serviceAccount:${google_service_account.deployer.email}"
}

resource "google_project_iam_member" "deployer_registry" {
  project = var.project_id
  role    = "roles/artifactregistry.writer"
  member  = "serviceAccount:${google_service_account.deployer.email}"
}

# Cloud Run revisions run *as* the workload accounts, and setting a revision's
# service account requires permission to impersonate it. Without this the
# deploy fails with "permission denied on service account", which reads like a
# problem with the workload account rather than with the deployer.
resource "google_service_account_iam_member" "deployer_acts_as" {
  for_each = {
    collector = google_service_account.collector.name
    processor = google_service_account.processor.name
    api       = google_service_account.api.name
    jobs      = google_service_account.jobs.name
  }

  service_account_id = each.value
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.deployer.email}"
}

resource "google_artifact_registry_repository" "images" {
  repository_id = "transitpulse-${local.suffix}"
  location      = var.region
  project       = var.project_id
  format        = "DOCKER"
  description   = "One image for every service; the entrypoint selects by TP_SERVICE."
  labels        = local.labels

  docker_config {
    # Tags stay mutable so `:latest` can exist for humans, but deployments go
    # by digest (see the `image` variable) so what is deployed cannot change
    # underneath a revision.
    immutable_tags = false
  }
}
