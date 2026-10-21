# The schedules. These are the reason the collector has no loop of its own:
# the cadence is infrastructure state -- visible, auditable, changeable
# without a deploy -- rather than a sleep buried in application code.

resource "google_cloud_scheduler_job" "collect" {
  name      = "tp-collect-${local.suffix}"
  project   = var.project_id
  region    = var.region
  schedule  = var.collector_schedule
  time_zone = "UTC"

  # The collector already retries transient upstream failures internally and
  # opens a breaker on persistent ones, so a scheduler retry on top is mostly
  # redundant. One is kept for the case the retry budget does not cover: the
  # instance itself disappearing mid-tick.
  retry_config {
    retry_count = 1
  }

  # Shorter than the schedule interval. A tick that has not finished by the
  # time the next one is due is a tick that has gone wrong, and overlapping
  # ticks would publish the same feed snapshot twice -- harmless, thanks to
  # the idempotency key, but it muddies every rate on the dashboard.
  attempt_deadline = "45s"

  http_target {
    http_method = "POST"
    uri         = "${google_cloud_run_v2_service.collector.uri}/collect"

    oidc_token {
      service_account_email = google_service_account.scheduler.email
      audience              = google_cloud_run_v2_service.collector.uri
    }
  }

  depends_on = [google_cloud_run_v2_service_iam_member.scheduler_invokes_collector]
}

# --- the jobs that keep the schema usable ----------------------------------
resource "google_cloud_scheduler_job" "partition_maintenance" {
  name    = "tp-partitions-${local.suffix}"
  project = var.project_id
  region  = var.region
  # Daily, well before the window could run out. The job guarantees
  # partition_ahead_days of runway, so a fortnight of broken cron goes
  # unnoticed without costing data -- which is the margin that exists because
  # partitions really did run out mid-development and every insert was
  # refused with no warning signal at all.
  schedule  = "15 3 * * *"
  time_zone = "UTC"

  retry_config {
    retry_count = 3
  }

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${google_cloud_run_v2_job.jobs["partition-maintenance"].name}:run"

    oauth_token {
      service_account_email = google_service_account.scheduler.email
    }
  }
}

resource "google_cloud_scheduler_job" "gtfs_static" {
  name    = "tp-gtfs-static-${local.suffix}"
  project = var.project_id
  region  = var.region
  # Weekly. Agencies publish a new static feed on their own cadence and the
  # loader is idempotent, so this is about staying within a few days of the
  # published timetable rather than catching a change the hour it lands.
  schedule  = "0 4 * * 1"
  time_zone = "UTC"

  retry_config {
    retry_count = 2
  }

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${google_cloud_run_v2_job.jobs["gtfs-static-loader"].name}:run"

    oauth_token {
      service_account_email = google_service_account.scheduler.email
    }
  }
}

# The dead-letter replayer is deliberately NOT scheduled.
#
# It is an incident-recovery tool: it re-validates messages the broker gave up
# on and republishes the ones that now pass. A cron that ran it hourly would
# replay a genuinely poison message for ever, and would do it unattended --
# turning a bounded, visible failure into an invisible loop. It is run by hand
# once the fault is fixed:
#
#   gcloud run jobs execute tp-dlq-replayer-${local.suffix} --region ${var.region}
#
# `--dry-run` first, which reports what would be republished and changes
# nothing.

resource "google_project_iam_member" "scheduler_runs_jobs" {
  project = var.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.scheduler.email}"
}
