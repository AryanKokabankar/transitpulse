# Resource names, computed rather than read back off the resources.
#
# This exists because of a cycle Terraform refused to plan: the push
# subscription's endpoint is the processor's URI, so the subscription depends
# on the service; and the processor's environment named the subscription it
# consumes, so the service depended on the subscription. Neither actually
# needs the other to exist -- both need a name, and a name is deterministic.
#
# Keeping them here rather than inline also means the local stack's defaults
# in .env.example and the deployed names differ only by the environment
# suffix, which is checkable by reading two files.

locals {
  topic_name             = "vehicle-events-${local.suffix}"
  dlq_topic_name         = "vehicle-events-dlq-${local.suffix}"
  processor_subscription = "vehicle-events-processor-${local.suffix}"
  dlq_subscription       = "vehicle-events-dlq-replayer-${local.suffix}"
}
