# Provider and backend pinning.
#
# The state lives in GCS rather than locally because more than one thing will
# eventually apply this -- a laptop and the deploy workflow -- and two
# uncoordinated local states silently diverge until one of them destroys the
# other's resources. The bucket is NOT created here: a backend cannot depend
# on a resource in the configuration it stores the state for, so it is made
# once by hand and recorded in the README.

terraform {
  required_version = ">= 1.9.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 6.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  backend "gcs" {
    # Configured with `-backend-config` per environment rather than hardcoded,
    # so dev and prod cannot share a state file by accident.
    prefix = "transitpulse"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# The beta provider exists solely for resources the GA one does not yet carry.
# Kept as a separate explicit alias rather than switching everything to beta:
# a beta provider silently changes the API surface of every resource it owns.
provider "google-beta" {
  project = var.project_id
  region  = var.region
}
