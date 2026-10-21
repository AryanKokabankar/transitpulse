# A private network, so the database has no public address at all.
#
# The alternative -- a public IP with authorised networks -- is one
# misconfiguration away from exposing the serving database, and Cloud Run's
# egress IPs are not stable enough to allowlist anyway.

resource "google_compute_network" "main" {
  name                    = "transitpulse-${local.suffix}"
  project                 = var.project_id
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "main" {
  name                     = "transitpulse-${local.suffix}"
  project                  = var.project_id
  region                   = var.region
  network                  = google_compute_network.main.id
  ip_cidr_range            = "10.8.0.0/20"
  private_ip_google_access = true
}

# The range Cloud SQL's managed instances are allocated from. Peered rather
# than routed: the instance lives in Google's project, not this one.
resource "google_compute_global_address" "private_ip" {
  name          = "transitpulse-sql-${local.suffix}"
  project       = var.project_id
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 16
  network       = google_compute_network.main.id
}

resource "google_service_networking_connection" "private_vpc" {
  network                 = google_compute_network.main.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.private_ip.name]
}

# Cloud Run is serverless and has no VPC of its own, so outbound traffic to
# the private network goes through a connector.
resource "google_vpc_access_connector" "main" {
  name    = "tp-${local.suffix}"
  project = var.project_id
  region  = var.region
  subnet {
    name = google_compute_subnetwork.connector.name
  }
  min_instances = 2
  max_instances = 3
}

resource "google_compute_subnetwork" "connector" {
  name          = "transitpulse-connector-${local.suffix}"
  project       = var.project_id
  region        = var.region
  network       = google_compute_network.main.id
  ip_cidr_range = "10.8.16.0/28"
}
