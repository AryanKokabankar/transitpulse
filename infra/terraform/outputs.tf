# What a deploy workflow and an operator need, and nothing that is a secret.

output "api_url" {
  description = "Public serving API."
  value       = google_cloud_run_v2_service.api.uri
}

output "collector_url" {
  description = "Internal collector; Cloud Scheduler POSTs /collect here."
  value       = google_cloud_run_v2_service.collector.uri
}

output "processor_url" {
  description = "Internal processor; the push subscription delivers here."
  value       = google_cloud_run_v2_service.processor.uri
}

output "processor_revision" {
  description = <<-EOT
    Revision this apply created.

    Feed it back as `stable_revision` on the next deploy: that is what gives
    a canary somewhere to send the traffic it is not taking.
  EOT
  value       = "tp-processor-${local.suffix}-${local.image_tag}"
}

output "serving_traffic" {
  description = "Where processor traffic is currently going."
  value = local.canary_active ? format(
    "%d%% to the canary (%s), %d%% to %s",
    var.canary_percent,
    "tp-processor-${local.suffix}-${local.image_tag}",
    100 - var.canary_percent,
    var.stable_revision,
  ) : "100% to the latest revision"
}

output "workload_identity_provider" {
  description = "Pass as `workload_identity_provider` to google-github-actions/auth."
  value       = google_iam_workload_identity_pool_provider.github.name
}

output "deployer_service_account" {
  description = "Pass as `service_account` to google-github-actions/auth."
  value       = google_service_account.deployer.email
}

output "artifact_registry" {
  description = "Where to push the image."
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.images.repository_id}"
}

output "sql_connection_name" {
  description = "Cloud SQL connection name, for the connector and for `gcloud sql connect`."
  value       = google_sql_database_instance.main.connection_name
}

output "snapshot_bucket" {
  description = "Replay corpus. The load harness reads from here (ADR-006)."
  value       = google_storage_bucket.snapshots.name
}

output "dead_letter_subscription" {
  description = "What the dlq-replayer job pulls from."
  value       = google_pubsub_subscription.dlq_replayer.name
}

# Deliberately not an output: the Postgres DSN. It is in Secret Manager, and
# an output would put it in plaintext in the state file and in the console of
# every CI run that calls `terraform output`.
