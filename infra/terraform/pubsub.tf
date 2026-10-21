# The broker, and the two IAM grants without which dead-lettering silently
# does nothing.

resource "google_pubsub_topic" "vehicle_events" {
  name    = local.topic_name
  labels  = local.labels
  project = var.project_id

  # Schema is enforced by the publisher (pydantic) rather than by Pub/Sub.
  # A broker-side schema would reject a malformed message at publish time,
  # which sounds better and is worse here: the collector's job is to
  # *partition* a feed into valid pings and quarantine records, and a publish
  # that raises would lose the record explaining why something was refused.
  message_retention_duration = "604800s" # 7 days
}

resource "google_pubsub_topic" "dlq" {
  name                       = local.dlq_topic_name
  labels                     = local.labels
  project                    = var.project_id
  message_retention_duration = "604800s"
}

resource "google_pubsub_subscription" "processor" {
  name    = local.processor_subscription
  topic   = google_pubsub_topic.vehicle_events.id
  labels  = local.labels
  project = var.project_id

  # Per-key ordering, not global. Pub/Sub then guarantees that one vehicle's
  # pings cannot overtake each other while remaining fully parallel across
  # vehicles; global ordering would cap throughput at a single consumer.
  enable_message_ordering = true

  # Longer than the handler's worst case by a wide margin. Too short and a
  # slow write turns into a redelivery of work that is still in flight, which
  # is how a backlog becomes a stampede.
  ack_deadline_seconds = 60

  expiration_policy {
    # Never expire. The default deletes a subscription after 31 days of
    # inactivity, which for a pipeline that is paused over a quiet period
    # means coming back to a topic nobody is listening to.
    ttl = ""
  }

  retry_policy {
    # Exponential backoff between redeliveries. Without it a transient
    # database outage is met with an immediate retry storm that keeps the
    # database down and burns the delivery budget in seconds -- measured
    # locally: the chaos harness, which retries with no backoff at all,
    # exhausts five attempts in under a second per message.
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  dead_letter_policy {
    dead_letter_topic = google_pubsub_topic.dlq.id
    # Must agree with TP_MAX_DELIVERY_ATTEMPTS in the processor. If the
    # handler gave up later than the broker dead-letters, the quarantine row
    # would never be written; earlier, and the DLQ would never see anything.
    max_delivery_attempts = 5
  }

  push_config {
    push_endpoint = google_cloud_run_v2_service.processor.uri

    # The processor is private, so Pub/Sub authenticates as a service account
    # that holds run.invoker on it. Without this the push subscription gets a
    # 403 on every delivery and every message dead-letters -- a failure that
    # looks exactly like a broken handler.
    oidc_token {
      service_account_email = google_service_account.pubsub_push.email
      audience              = google_cloud_run_v2_service.processor.uri
    }
  }

  # Only the topic grant can be ordered before the subscription: the
  # subscriber grant is *on* this subscription, so requiring it here would be
  # a cycle. It is applied immediately afterwards, which leaves a brief window
  # where a message that fails five times would be redelivered rather than
  # moved -- harmless, because the redelivery continues until the grant lands.
  depends_on = [google_pubsub_topic_iam_member.dlq_publisher]
}

# The replayer's own subscription on the dead-letter topic.
#
# A topic with no subscription discards what is published to it, so without
# this the broker would move a failed message to the DLQ and it would vanish
# -- "nothing is dropped silently" failing at the one hop that exists to
# prevent it.
resource "google_pubsub_subscription" "dlq_replayer" {
  name    = local.dlq_subscription
  topic   = google_pubsub_topic.dlq.id
  labels  = local.labels
  project = var.project_id

  # No ordering and no dead-letter policy of its own. Ordered delivery would
  # serialise the replayer per vehicle for no benefit, since order is
  # re-established by the ordering key when it republishes to the main topic;
  # and a dead-letter policy on a dead-letter subscription is a loop with
  # nowhere left to send a message that keeps failing.
  ack_deadline_seconds = 60

  expiration_policy {
    ttl = ""
  }
}

# ---------------------------------------------------------------------------
# The grants dead-lettering actually needs.
#
# Pub/Sub performs the move itself, as its own service agent, and that agent
# needs two permissions that are not granted by default: publish to the
# dead-letter topic, and acknowledge on the source subscription. Without them
# the move fails silently -- the message is simply redelivered for ever, the
# DLQ stays empty, and nothing anywhere reports a permissions problem. This is
# the single most common way a dead-letter policy is configured and does
# nothing.
# ---------------------------------------------------------------------------
data "google_project" "current" {
  project_id = var.project_id
}

locals {
  pubsub_agent = "serviceAccount:service-${data.google_project.current.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

resource "google_pubsub_topic_iam_member" "dlq_publisher" {
  topic   = google_pubsub_topic.dlq.id
  role    = "roles/pubsub.publisher"
  member  = local.pubsub_agent
  project = var.project_id
}

resource "google_pubsub_subscription_iam_member" "dlq_subscriber" {
  subscription = google_pubsub_subscription.processor.id
  role         = "roles/pubsub.subscriber"
  member       = local.pubsub_agent
  project      = var.project_id
}
