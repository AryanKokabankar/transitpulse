# Inputs. Defaults are chosen to match the local stack wherever a value has a
# counterpart there, so that reading docker-compose.yml and reading this
# describes the same pipeline with the same names.

variable "project_id" {
  description = "GCP project to deploy into."
  type        = string
}

variable "region" {
  description = "Region for Cloud Run, Cloud SQL and Cloud Scheduler."
  type        = string
  default     = "us-east1"
}

variable "environment" {
  description = "Deployment environment; becomes a resource-name suffix and a label."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "environment must be dev or prod."
  }
}

variable "image" {
  description = <<-EOT
    Fully qualified container image for every service, by digest.

    One image runs all four services and all three jobs -- the entrypoint
    picks by TP_SERVICE -- so they share a build, a scan and a digest, and a
    dependency bump cannot leave one of them behind.

    By digest rather than by tag, deliberately. A mutable tag means the
    revision Terraform creates is not necessarily the image that was tested,
    and a rollback to a previous revision would re-pull whatever that tag
    points at today.
  EOT
  type        = string

  validation {
    condition     = can(regex("@sha256:[0-9a-f]{64}$", var.image))
    error_message = "image must be pinned by digest, e.g. REGION-docker.pkg.dev/p/r/i@sha256:..."
  }
}

# --- feed ------------------------------------------------------------------
variable "feed_url" {
  description = "GTFS-Realtime vehicle positions endpoint."
  type        = string
  default     = "https://cdn.mbta.com/realtime/VehiclePositions.pb"
}

variable "feed_agency" {
  description = "Agency slug; labels every metric and partitions the snapshot archive."
  type        = string
  default     = "mbta"
}

variable "agency_timezone" {
  description = <<-EOT
    Timezone the agency's static schedule is expressed in.

    Not cosmetic: GTFS stop times are local to the agency and relative to a
    service day, so a wrong value produces schedule delays that are off by
    whole hours while still looking entirely plausible.
  EOT
  type        = string
  default     = "America/New_York"
}

variable "collector_schedule" {
  description = "Cron for the collector tick. Fifteen seconds is the feed's own cadence, which cron cannot express, so Cloud Scheduler runs every minute and the service is also reachable directly."
  type        = string
  default     = "* * * * *"
}

# --- capacity --------------------------------------------------------------
variable "processor_max_instances" {
  description = <<-EOT
    Upper bound on processor instances.

    This is the one knob that can overwhelm the database: every instance opens
    its own connection pool, so the ceiling on concurrent Postgres connections
    is max_instances * TP_PG_POOL_MAX, and Cloud SQL enforces a hard
    max_connections. The default leaves headroom under the tier below.
  EOT
  type        = number
  default     = 10
}

variable "processor_pool_max" {
  description = "Per-instance Postgres pool size. Measured locally as having headroom at 10 (BENCHMARKS.md)."
  type        = number
  default     = 10
}

variable "sql_tier" {
  description = "Cloud SQL machine type."
  type        = string
  default     = "db-custom-2-7680"
}

variable "sql_max_connections" {
  description = "Hard ceiling in Postgres. Must exceed processor_max_instances * processor_pool_max plus room for the API and the jobs."
  type        = number
  default     = 200
}

# --- data retention --------------------------------------------------------
variable "partition_ahead_days" {
  description = "Days of future vehicle_events partitions to guarantee. An insert with no matching partition fails outright, so this is the margin between a missed maintenance run and refused traffic."
  type        = number
  default     = 14
}

variable "partition_retention_days" {
  description = "Partitions older than this are detached and dropped."
  type        = number
  default     = 90
}

variable "snapshot_retention_days" {
  description = "Lifecycle age for archived raw feed responses in GCS."
  type        = number
  default     = 30
}

# --- release ---------------------------------------------------------------
variable "canary_percent" {
  description = <<-EOT
    Share of processor traffic sent to the newest revision.

    Zero means the new revision is deployed, healthy and receiving nothing --
    which is the useful default, because it makes deploying and releasing two
    separate decisions. See canary.tf.
  EOT
  type        = number
  default     = 0

  validation {
    condition     = var.canary_percent >= 0 && var.canary_percent <= 100
    error_message = "canary_percent must be between 0 and 100."
  }
}

variable "github_repository" {
  description = "owner/name allowed to impersonate the deployer service account through Workload Identity Federation."
  type        = string
  default     = "AryanKokabankar/transitpulse"

  validation {
    condition     = can(regex("^[^/]+/[^/]+$", var.github_repository))
    error_message = "github_repository must be in owner/name form."
  }
}

variable "alert_email" {
  description = "Address for alert notifications. Empty disables the notification channel."
  type        = string
  default     = ""
}

locals {
  suffix = var.environment
  labels = {
    app         = "transitpulse"
    environment = var.environment
    managed_by  = "terraform"
  }

  # Shared by every service and job, so a setting cannot drift between the
  # thing that writes a row and the job that maintains the table it goes in.
  common_env = {
    TP_ENV                      = var.environment == "prod" ? "prod" : "dev"
    TP_GCP_PROJECT              = var.project_id
    TP_FEED_URL                 = var.feed_url
    TP_FEED_AGENCY              = var.feed_agency
    TP_AGENCY_TIMEZONE          = var.agency_timezone
    TP_TOPIC_VEHICLE_EVENTS     = local.topic_name
    TP_TOPIC_DLQ                = local.dlq_topic_name
    TP_SUBSCRIPTION_PROCESSOR   = local.processor_subscription
    TP_SUBSCRIPTION_DLQ         = local.dlq_subscription
    TP_BQ_ENABLED               = "true"
    TP_BQ_DATASET               = google_bigquery_dataset.history.dataset_id
    TP_BQ_TABLE                 = google_bigquery_table.vehicle_events.table_id
    TP_SNAPSHOT_BACKEND         = "gcs"
    TP_SNAPSHOT_BUCKET          = google_storage_bucket.snapshots.name
    TP_PARTITION_AHEAD_DAYS     = tostring(var.partition_ahead_days)
    TP_PARTITION_RETENTION_DAYS = tostring(var.partition_retention_days)
    OTEL_EXPORTER_OTLP_ENDPOINT = "http://localhost:4317"
    TP_OTEL_ENABLED             = "true"
  }
}
