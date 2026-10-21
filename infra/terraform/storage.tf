# The two stores whose shapes encode the project's central storage decision
# (ADR-002), plus the snapshot archive the load harness replays from.

# --- Cloud SQL: the serving layer ------------------------------------------
resource "google_sql_database_instance" "main" {
  name             = "transitpulse-${local.suffix}"
  database_version = "POSTGRES_16"
  region           = var.region
  project          = var.project_id

  # Guards against `terraform destroy` taking the serving database with it.
  deletion_protection = var.environment == "prod"

  settings {
    tier              = var.sql_tier
    availability_type = var.environment == "prod" ? "REGIONAL" : "ZONAL"
    disk_type         = "PD_SSD"
    disk_autoresize   = true

    # NOTE FOR ANYONE READING BENCHMARKS.md: the local stack runs with
    # fsync=off and synchronous_commit=off, which is why every write-path
    # figure there carries a caveat. Nothing of the sort appears here. Cloud
    # SQL does not expose fsync at all, and synchronous_commit is left at its
    # default of on.
    database_flags {
      name  = "max_connections"
      value = tostring(var.sql_max_connections)
    }

    database_flags {
      # The same extension the local stack preloads. It is what makes "which
      # query is slow" answerable during an incident, and it has to be set at
      # instance level because it is a shared-preload library.
      name  = "shared_preload_libraries"
      value = "pg_stat_statements"
    }

    backup_configuration {
      enabled                        = true
      start_time                     = "08:00" # UTC, outside the agency's peak
      point_in_time_recovery_enabled = var.environment == "prod"
      transaction_log_retention_days = 7
    }

    ip_configuration {
      # No public IP. Cloud Run reaches it over the Cloud SQL connector, which
      # means the database is not addressable from the internet at all rather
      # than being addressable and firewalled.
      ipv4_enabled    = false
      private_network = google_compute_network.main.id
      ssl_mode        = "ENCRYPTED_ONLY"
    }

    insights_config {
      query_insights_enabled  = true
      record_application_tags = true
    }

    user_labels = local.labels
  }

  depends_on = [google_service_networking_connection.private_vpc]
}

resource "google_sql_database" "main" {
  name     = "transitpulse"
  instance = google_sql_database_instance.main.name
  project  = var.project_id
}

resource "google_sql_user" "app" {
  name     = "transitpulse"
  instance = google_sql_database_instance.main.name
  project  = var.project_id
  password = random_password.sql.result
}

resource "random_password" "sql" {
  length  = 32
  special = false
}

resource "google_secret_manager_secret" "sql_dsn" {
  secret_id = "transitpulse-pg-dsn-${local.suffix}"
  project   = var.project_id
  labels    = local.labels

  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "sql_dsn" {
  secret = google_secret_manager_secret.sql_dsn.id

  # Through the Cloud SQL connector's unix socket rather than a host and port:
  # there is no IP to connect to, by design.
  secret_data = format(
    "postgresql://%s:%s@/%s?host=/cloudsql/%s",
    google_sql_user.app.name,
    random_password.sql.result,
    google_sql_database.main.name,
    google_sql_database_instance.main.connection_name,
  )
}

# --- BigQuery: the analytical history --------------------------------------
resource "google_bigquery_dataset" "history" {
  dataset_id                 = "transitpulse_${local.suffix}"
  location                   = "US"
  project                    = var.project_id
  labels                     = local.labels
  description                = "Append-only vehicle event history. Deduplicate on idempotency_key (ADR-005)."
  delete_contents_on_destroy = var.environment != "prod"
}

resource "google_bigquery_table" "vehicle_events" {
  dataset_id          = google_bigquery_dataset.history.dataset_id
  table_id            = "vehicle_events"
  project             = var.project_id
  labels              = local.labels
  deletion_protection = var.environment == "prod"

  # Partitioned by event day and clustered by route: the questions this table
  # answers are "what happened on route X over the last N days", and that
  # combination turns a full scan into a partition prune plus a cluster seek.
  # The serving table is shaped for the opposite access pattern (ADR-002).
  time_partitioning {
    type          = "DAY"
    field         = "event_time"
    expiration_ms = var.partition_retention_days * 24 * 60 * 60 * 1000
  }

  clustering = ["route_id", "agency"]

  # Generated from the same contract the sink writes, so a column cannot drift
  # from the model that fills it.
  schema = file("${path.module}/schemas/vehicle_events.json")
}

# --- GCS: the replay corpus -------------------------------------------------
resource "google_storage_bucket" "snapshots" {
  name                        = "${var.project_id}-transitpulse-snapshots-${local.suffix}"
  location                    = "US"
  project                     = var.project_id
  labels                      = local.labels
  uniform_bucket_level_access = true
  force_destroy               = var.environment != "prod"

  lifecycle_rule {
    condition {
      age = var.snapshot_retention_days
    }
    action {
      type = "Delete"
    }
  }

  versioning {
    # Off deliberately. Snapshots are immutable and content-addressed by
    # sha256 in their object name, so a second write of the same path is the
    # same bytes and a version history of it is pure cost.
    enabled = false
  }
}
