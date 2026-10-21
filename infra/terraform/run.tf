# The four services and three jobs, all from one image, selected by TP_SERVICE.

locals {
  # Rendered into every container. Secrets are not here -- they arrive as
  # secret references below, so the DSN never appears in a revision's
  # description, in `terraform show`, or in the state file as plaintext.
  env_pairs = local.common_env

  otel_sidecar_args = [
    "--config=/etc/otel/config.yaml",
  ]
}

# --- collector --------------------------------------------------------------
resource "google_cloud_run_v2_service" "collector" {
  name     = "tp-collector-${local.suffix}"
  location = var.region
  project  = var.project_id
  labels   = local.labels

  # Private. The only thing that should reach it is Cloud Scheduler.
  ingress = "INGRESS_TRAFFIC_INTERNAL_ONLY"

  template {
    service_account = google_service_account.collector.email
    labels          = local.labels

    scaling {
      # Scales to zero between ticks: it does nothing for most of each minute
      # and this is the bulk of why the bill is near nothing.
      min_instance_count = 0
      max_instance_count = 3
    }

    vpc_access {
      connector = google_vpc_access_connector.main.id
      egress    = "PRIVATE_RANGES_ONLY"
    }

    containers {
      image = var.image

      env {
        name  = "TP_SERVICE"
        value = "collector"
      }

      dynamic "env" {
        for_each = local.env_pairs
        content {
          name  = env.key
          value = env.value
        }
      }

      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
        # The decode is CPU-bound and happens in a burst per tick; throttling
        # between requests would make each tick pay a ramp-up.
        cpu_idle = true
      }

      startup_probe {
        http_get {
          path = "/readyz"
        }
        initial_delay_seconds = 2
        period_seconds        = 3
        failure_threshold     = 10
      }

      liveness_probe {
        http_get {
          path = "/healthz"
        }
        period_seconds = 30
      }
    }
  }
}

# --- processor --------------------------------------------------------------
resource "google_cloud_run_v2_service" "processor" {
  name     = "tp-processor-${local.suffix}"
  location = var.region
  project  = var.project_id
  labels   = local.labels

  ingress = "INGRESS_TRAFFIC_INTERNAL_ONLY"

  template {
    service_account = google_service_account.processor.email
    labels          = local.labels

    # Named from the image digest rather than generated. Two reasons: a
    # canary has to be able to name the revision that keeps the other 99% of
    # traffic, and re-applying an unchanged image must be a no-op rather than
    # a new revision with the same contents.
    revision = "tp-processor-${local.suffix}-${local.image_tag}"

    scaling {
      # Never zero. A push subscription delivering to a cold service pays a
      # cold start per burst, and the measured handler cost is already the
      # pipeline's binding constraint (BENCHMARKS.md).
      min_instance_count = 1
      max_instance_count = var.processor_max_instances
    }

    # One event per request, so concurrency is what sets how many events an
    # instance handles at once -- and with it how many pool connections are in
    # use. Kept at the pool size rather than above it: more concurrent
    # requests than connections means requests queueing on the pool, which
    # shows up as latency rather than as throughput.
    max_instance_request_concurrency = var.processor_pool_max

    vpc_access {
      connector = google_vpc_access_connector.main.id
      egress    = "PRIVATE_RANGES_ONLY"
    }

    volumes {
      name = "cloudsql"
      cloud_sql_instance {
        instances = [google_sql_database_instance.main.connection_name]
      }
    }

    containers {
      image = var.image

      env {
        name  = "TP_SERVICE"
        value = "processor"
      }

      env {
        name  = "TP_PG_POOL_MAX"
        value = tostring(var.processor_pool_max)
      }

      env {
        name = "TP_PG_DSN"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.sql_dsn.secret_id
            version = "latest"
          }
        }
      }

      dynamic "env" {
        for_each = local.env_pairs
        content {
          name  = env.key
          value = env.value
        }
      }

      volume_mounts {
        name       = "cloudsql"
        mount_path = "/cloudsql"
      }

      resources {
        limits = {
          cpu    = "1"
          memory = "1Gi"
        }
        # CPU always allocated: the handler does work between requests (the
        # BigQuery sink's batch flush) and a throttled instance would hold
        # acknowledged rows unflushed.
        cpu_idle = false
      }

      startup_probe {
        http_get {
          path = "/readyz"
        }
        initial_delay_seconds = 3
        period_seconds        = 3
        failure_threshold     = 10
      }
    }
  }

  # Traffic is split here rather than left to default. See canary.tf for the
  # shape and for why the newest revision gets nothing until someone says so.
  dynamic "traffic" {
    for_each = local.processor_traffic
    content {
      type     = traffic.value.type
      revision = traffic.value.revision
      percent  = traffic.value.percent
    }
  }
}

# --- serving API ------------------------------------------------------------
resource "google_cloud_run_v2_service" "api" {
  name     = "tp-api-${local.suffix}"
  location = var.region
  project  = var.project_id
  labels   = local.labels

  # The only public surface in the deployment.
  ingress = "INGRESS_TRAFFIC_ALL"

  template {
    service_account = google_service_account.api.email
    labels          = local.labels

    scaling {
      min_instance_count = var.environment == "prod" ? 1 : 0
      max_instance_count = 5
    }

    vpc_access {
      connector = google_vpc_access_connector.main.id
      egress    = "PRIVATE_RANGES_ONLY"
    }

    volumes {
      name = "cloudsql"
      cloud_sql_instance {
        instances = [google_sql_database_instance.main.connection_name]
      }
    }

    containers {
      image = var.image

      env {
        name  = "TP_SERVICE"
        value = "api"
      }

      env {
        name = "TP_PG_DSN"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.sql_dsn.secret_id
            version = "latest"
          }
        }
      }

      dynamic "env" {
        for_each = local.env_pairs
        content {
          name  = env.key
          value = env.value
        }
      }

      volume_mounts {
        name       = "cloudsql"
        mount_path = "/cloudsql"
      }

      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
      }
    }
  }
}

# --- jobs -------------------------------------------------------------------
# One Cloud Run Job per entrypoint, all from the same image. TP_JOB selects
# the module; a typo fails loudly in the entrypoint rather than starting the
# wrong thing.
resource "google_cloud_run_v2_job" "jobs" {
  for_each = {
    partition-maintenance = "partition_maintenance"
    gtfs-static-loader    = "gtfs_static_loader"
    dlq-replayer          = "dlq_replayer"
  }

  name     = "tp-${each.key}-${local.suffix}"
  location = var.region
  project  = var.project_id
  labels   = local.labels

  template {
    template {
      service_account = google_service_account.jobs.email

      # A job that hangs should fail, not run until someone notices the bill.
      timeout     = each.key == "gtfs-static-loader" ? "1800s" : "600s"
      max_retries = 1

      vpc_access {
        connector = google_vpc_access_connector.main.id
        egress    = "PRIVATE_RANGES_ONLY"
      }

      volumes {
        name = "cloudsql"
        cloud_sql_instance {
          instances = [google_sql_database_instance.main.connection_name]
        }
      }

      containers {
        image = var.image

        env {
          name  = "TP_SERVICE"
          value = "job"
        }

        env {
          name  = "TP_JOB"
          value = each.value
        }

        env {
          name = "TP_PG_DSN"
          value_source {
            secret_key_ref {
              secret  = google_secret_manager_secret.sql_dsn.secret_id
              version = "latest"
            }
          }
        }

        dynamic "env" {
          for_each = local.env_pairs
          content {
            name  = env.key
            value = env.value
          }
        }

        volume_mounts {
          name       = "cloudsql"
          mount_path = "/cloudsql"
        }

        resources {
          limits = {
            cpu = "1"
            # The static GTFS loader reads a whole agency archive into memory.
            memory = each.key == "gtfs-static-loader" ? "2Gi" : "512Mi"
          }
        }
      }
    }
  }
}
