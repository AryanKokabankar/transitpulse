# The SLO, restated in Cloud Monitoring.
#
# It is already defined once as Prometheus recording rules
# (observability/prometheus/rules/), and this is the same objective expressed
# a second time against a different system -- which is a duplication worth
# being explicit about. ADR-007 covers it: the local rules are what a
# developer iterates against, and these are what pages someone at 3am.
#
# They can drift. The thing that stops them drifting silently is that both
# derive from the same two numbers -- a 2s threshold and a 99% target -- and
# both are stated here in one place.

locals {
  # 99% of events ingested within 2s, over a rolling 30 days. The 2s is
  # derived: Cloud Scheduler ticks the collector every minute and the agency
  # publishes every 15s, so a 2s ingest budget means ingestion is never the
  # reason a position is stale. The 99% is a chosen target, not a measurement
  # -- there is no production traffic to derive an achievable percentile
  # from, and saying otherwise would be an unearned number.
  slo_goal           = 0.99
  slo_threshold_secs = 2.0
}

resource "google_monitoring_service" "pipeline" {
  service_id   = "transitpulse-${local.suffix}"
  display_name = "TransitPulse pipeline (${var.environment})"
  project      = var.project_id

  basic_service {
    service_type = "CLOUD_RUN"
    service_labels = {
      service_name = google_cloud_run_v2_service.processor.name
      location     = var.region
    }
  }
}

resource "google_monitoring_slo" "ingest_lag" {
  service      = google_monitoring_service.pipeline.service_id
  slo_id       = "ingest-lag-${local.suffix}"
  display_name = "99% of events ingested within 2s"
  project      = var.project_id

  goal                = local.slo_goal
  rolling_period_days = 30

  request_based_sli {
    distribution_cut {
      # The OTel histogram, as Cloud Monitoring names it once the exporter
      # has been through it. The same renaming hazard the Prometheus side has
      # -- a unit change silently renames the series and empties the panel --
      # applies here, which is why the local dashboard contract test derives
      # names from the registry rather than trusting them.
      distribution_filter = join(" AND ", [
        "metric.type = \"workload.googleapis.com/transitpulse.ingest.lag\"",
        "resource.type = \"generic_task\"",
      ])

      range {
        # Lower bound omitted rather than set to zero: lag is
        # processed_time - event_time and a clock skew can make it
        # negative, which a zero floor would silently count as a breach.
        max = local.slo_threshold_secs
      }
    }
  }
}

# --- burn-rate alerting -----------------------------------------------------
# The same two-window pairing the Prometheus rules use. A long window alone
# keeps paging after recovery; a short one alone pages for every blip.
resource "google_monitoring_alert_policy" "burn_fast" {
  display_name = "TransitPulse SLO burn 14.4x (${var.environment})"
  project      = var.project_id
  combiner     = "AND"
  severity     = "CRITICAL"

  conditions {
    display_name = "1h burn rate above 14.4x"
    condition_threshold {
      filter          = "select_slo_burn_rate(\"${google_monitoring_slo.ingest_lag.name}\", \"3600s\")"
      comparison      = "COMPARISON_GT"
      threshold_value = 14.4
      duration        = "0s"

      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_MEAN"
      }
    }
  }

  conditions {
    display_name = "5m burn rate above 14.4x"
    condition_threshold {
      filter          = "select_slo_burn_rate(\"${google_monitoring_slo.ingest_lag.name}\", \"300s\")"
      comparison      = "COMPARISON_GT"
      threshold_value = 14.4
      duration        = "0s"

      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_MEAN"
      }
    }
  }

  documentation {
    content   = "Burning the 30-day error budget 14.4x faster than sustainable: the whole budget in about two days. Runbook: check ingest lag p95 by revision first, then sink write latency -- the measured ceiling is the consumer, so the processor or Cloud SQL is the usual cause (BENCHMARKS.md)."
    mime_type = "text/markdown"
  }

  notification_channels = var.alert_email == "" ? [] : [google_monitoring_notification_channel.email[0].id]
}

resource "google_monitoring_alert_policy" "dead_lettered" {
  display_name = "TransitPulse events dead-lettered (${var.environment})"
  project      = var.project_id
  combiner     = "OR"
  severity     = "CRITICAL"

  conditions {
    display_name = "anything reaching the dead-letter topic"
    condition_threshold {
      filter = join(" AND ", [
        "metric.type = \"pubsub.googleapis.com/topic/send_message_operation_count\"",
        "resource.type = \"pubsub_topic\"",
        "resource.labels.topic_id = \"${local.dlq_topic_name}\"",
      ])
      comparison = "COMPARISON_GT"
      # Any at all. A dead letter means five delivery attempts failed, which
      # is never routine.
      threshold_value = 0
      duration        = "60s"

      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_SUM"
      }
    }
  }

  documentation {
    content   = "Messages have exhausted their delivery budget. Nothing is lost yet -- they are on ${local.dlq_topic_name} and the dlq-replayer job recovers them once the fault is fixed. Run it with --dry-run first."
    mime_type = "text/markdown"
  }

  notification_channels = var.alert_email == "" ? [] : [google_monitoring_notification_channel.email[0].id]
}

resource "google_monitoring_notification_channel" "email" {
  count = var.alert_email == "" ? 0 : 1

  display_name = "TransitPulse alerts (${var.environment})"
  project      = var.project_id
  type         = "email"

  labels = {
    email_address = var.alert_email
  }
}
