# One service account per workload, each holding only what that workload uses.
#
# The temptation is a single "transitpulse" account with the union of every
# permission, which is one compromised container away from full access to the
# serving database, the history and the broker. Splitting them means the
# collector -- the service that parses untrusted protobuf off the public
# internet -- cannot reach Postgres at all, because it genuinely has no reason
# to (ADR-006).

resource "google_service_account" "collector" {
  account_id   = "tp-collector-${local.suffix}"
  display_name = "TransitPulse collector"
  project      = var.project_id
}

resource "google_service_account" "processor" {
  account_id   = "tp-processor-${local.suffix}"
  display_name = "TransitPulse processor"
  project      = var.project_id
}

resource "google_service_account" "api" {
  account_id   = "tp-api-${local.suffix}"
  display_name = "TransitPulse serving API"
  project      = var.project_id
}

resource "google_service_account" "jobs" {
  account_id   = "tp-jobs-${local.suffix}"
  display_name = "TransitPulse scheduled jobs"
  project      = var.project_id
}

# Pub/Sub impersonates this to call the private processor, and Cloud Scheduler
# impersonates its own to call the collector. Separate from the workloads: an
# identity that *invokes* a service should not also be one that service runs
# as.
resource "google_service_account" "pubsub_push" {
  account_id   = "tp-pubsub-push-${local.suffix}"
  display_name = "Pub/Sub push identity"
  project      = var.project_id
}

resource "google_service_account" "scheduler" {
  account_id   = "tp-scheduler-${local.suffix}"
  display_name = "Cloud Scheduler identity"
  project      = var.project_id
}

# --- collector: publish, archive, export telemetry. No database. -----------
resource "google_pubsub_topic_iam_member" "collector_publisher" {
  topic   = google_pubsub_topic.vehicle_events.id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.collector.email}"
  project = var.project_id
}

resource "google_storage_bucket_iam_member" "collector_snapshots" {
  bucket = google_storage_bucket.snapshots.name
  # objectCreator, not objectAdmin: the collector archives and never deletes.
  # Retention is the bucket lifecycle rule's job.
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.collector.email}"
}

# --- processor: consume, write both sinks ----------------------------------
resource "google_project_iam_member" "processor_sql" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.processor.email}"
}

resource "google_bigquery_dataset_iam_member" "processor_writer" {
  dataset_id = google_bigquery_dataset.history.dataset_id
  project    = var.project_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.processor.email}"
}

resource "google_project_iam_member" "processor_bq_jobs" {
  project = var.project_id
  # The Storage Write API needs this in addition to dataEditor; without it
  # appends fail with a permission error that names neither the table nor the
  # missing role.
  role   = "roles/bigquery.jobUser"
  member = "serviceAccount:${google_service_account.processor.email}"
}

resource "google_secret_manager_secret_iam_member" "processor_dsn" {
  secret_id = google_secret_manager_secret.sql_dsn.id
  project   = var.project_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.processor.email}"
}

# --- api: read the serving tables ------------------------------------------
resource "google_project_iam_member" "api_sql" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.api.email}"
}

resource "google_secret_manager_secret_iam_member" "api_dsn" {
  secret_id = google_secret_manager_secret.sql_dsn.id
  project   = var.project_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.api.email}"
}

# --- jobs: schema maintenance, static GTFS, DLQ replay ---------------------
resource "google_project_iam_member" "jobs_sql" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.jobs.email}"
}

resource "google_secret_manager_secret_iam_member" "jobs_dsn" {
  secret_id = google_secret_manager_secret.sql_dsn.id
  project   = var.project_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.jobs.email}"
}

# The replayer pulls from the dead-letter subscription and republishes to the
# main topic. Both halves are needed, and only by this account.
resource "google_pubsub_subscription_iam_member" "jobs_dlq_subscriber" {
  subscription = google_pubsub_subscription.dlq_replayer.id
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${google_service_account.jobs.email}"
  project      = var.project_id
}

resource "google_pubsub_topic_iam_member" "jobs_republisher" {
  topic   = google_pubsub_topic.vehicle_events.id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.jobs.email}"
  project = var.project_id
}

# --- telemetry: every workload exports traces and metrics ------------------
resource "google_project_iam_member" "telemetry" {
  for_each = {
    collector = google_service_account.collector.email
    processor = google_service_account.processor.email
    api       = google_service_account.api.email
    jobs      = google_service_account.jobs.email
  }

  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${each.value}"
}

resource "google_project_iam_member" "tracing" {
  for_each = {
    collector = google_service_account.collector.email
    processor = google_service_account.processor.email
    api       = google_service_account.api.email
    jobs      = google_service_account.jobs.email
  }

  project = var.project_id
  role    = "roles/cloudtrace.agent"
  member  = "serviceAccount:${each.value}"
}

# --- invocation ------------------------------------------------------------
resource "google_cloud_run_v2_service_iam_member" "pubsub_invokes_processor" {
  name     = google_cloud_run_v2_service.processor.name
  location = google_cloud_run_v2_service.processor.location
  project  = var.project_id
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.pubsub_push.email}"
}

resource "google_cloud_run_v2_service_iam_member" "scheduler_invokes_collector" {
  name     = google_cloud_run_v2_service.collector.name
  location = google_cloud_run_v2_service.collector.location
  project  = var.project_id
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.scheduler.email}"
}
