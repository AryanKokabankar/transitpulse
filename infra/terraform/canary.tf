# Canary releases for the processor.
#
# **Deploying and releasing are separated.** `canary_percent` defaults to 0,
# so an apply with a new image creates the revision, starts it, runs its
# startup probe and sends it no traffic at all. Promoting is a second, cheap,
# reversible decision -- and a rollback is setting the number back, not
# rebuilding anything.
#
# The processor is the only service that gets this treatment, and that is a
# judgement about blast radius rather than an oversight:
#
#   * the **processor** is the one service that writes. A bad revision
#     corrupts or loses data, and the damage outlives the rollback.
#   * the **collector** is driven by a scheduler every minute and is
#     idempotent per tick -- a bad revision means some ticks fail, the
#     breaker opens, and the next good revision picks up. Nothing is lost,
#     because the feed is re-fetched rather than replayed.
#   * the **API** is read-only. A bad revision serves errors, which is
#     visible immediately and harmful only while it lasts.
#
# The thing this cannot do is decide whether the canary is healthy. That
# needs a comparison between revisions over a window, and the signal to
# compare already exists -- `transitpulse.events.processed` and
# `transitpulse.ingest.lag` both carry the Cloud Run revision as a resource
# label, so an error-rate or lag comparison per revision is a query rather
# than new instrumentation. The alert below watches the canary's own error
# rate, which catches a revision that is failing outright; it does not catch
# one that is merely worse than its predecessor.

variable "stable_revision" {
  description = <<-EOT
    Revision that keeps the traffic the canary is not taking.

    Empty on a first deploy, when there is no previous revision and all
    traffic goes to the latest one. Afterwards this is set to the revision
    currently serving, which is what makes a canary a split rather than a
    replacement.
  EOT
  type        = string
  default     = ""
}

locals {
  # The digest is what identifies a build, so the revision name is derived
  # from it. Short enough to keep the name under Cloud Run's limit, long
  # enough that a collision would be remarkable.
  image_tag = substr(regex("sha256:([0-9a-f]+)$", var.image)[0], 0, 10)

  # A canary needs both a share to give it and somewhere to put the rest.
  # Asking for 10% with no stable revision named would send 90% of traffic
  # nowhere, so the split only engages when both are present.
  canary_active = var.canary_percent > 0 && var.stable_revision != ""

  processor_traffic = local.canary_active ? [
    {
      type     = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
      revision = null
      percent  = var.canary_percent
    },
    {
      type     = "TRAFFIC_TARGET_ALLOCATION_TYPE_REVISION"
      revision = var.stable_revision
      percent  = 100 - var.canary_percent
    },
    ] : [
    {
      type     = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
      revision = null
      percent  = 100
    },
  ]
}

# Fires on the canary failing on its own terms, which is the half that can be
# judged without a baseline. A revision that is merely *worse* than the one it
# is replacing needs a comparison this does not attempt.
resource "google_monitoring_alert_policy" "canary_errors" {
  count = local.canary_active ? 1 : 0

  display_name = "TransitPulse canary is erroring (${var.environment})"
  project      = var.project_id
  combiner     = "OR"

  conditions {
    display_name = "5xx rate on the latest processor revision"

    condition_threshold {
      filter = join(" AND ", [
        "resource.type = \"cloud_run_revision\"",
        "resource.labels.service_name = \"${google_cloud_run_v2_service.processor.name}\"",
        "resource.labels.revision_name = \"tp-processor-${local.suffix}-${local.image_tag}\"",
        "metric.type = \"run.googleapis.com/request_count\"",
        "metric.labels.response_code_class = \"5xx\"",
      ])

      comparison      = "COMPARISON_GT"
      threshold_value = 0.05
      duration        = "300s"

      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_RATE"
      }
    }
  }

  documentation {
    content = join("\n", [
      "The canary revision is returning 5xx.",
      "",
      "Roll back by setting canary_percent to 0 and applying; traffic returns",
      "to ${var.stable_revision} without a rebuild. The canary revision stays",
      "deployed and receiving nothing, which is what makes it diagnosable.",
      "",
      "A processor 5xx is a nack, so nothing is lost while this is firing --",
      "messages are redelivered, and the dead-letter policy holds whatever",
      "exhausts its five attempts. Check the DLQ depth before assuming the",
      "rollback was clean, and run the dlq-replayer job if it is not empty.",
    ])
    mime_type = "text/markdown"
  }

  notification_channels = var.alert_email == "" ? [] : [google_monitoring_notification_channel.email[0].id]
}
