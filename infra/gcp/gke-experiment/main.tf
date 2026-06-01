locals {
  required_services = [
    "artifactregistry.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "cloudbuild.googleapis.com",
    "compute.googleapis.com",
    "container.googleapis.com",
    "iam.googleapis.com",
  ]

  labels = merge(
    {
      project    = "rec-store"
      purpose    = "gke-experiment"
      managed_by = "terraform"
    },
    var.labels,
  )

  node_project_roles = [
    "roles/container.defaultNodeServiceAccount",
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
    "roles/stackdriver.resourceMetadata.writer",
  ]

  cloud_build_service_accounts = toset([
    "${data.google_project.current.number}@cloudbuild.gserviceaccount.com",
    "${data.google_project.current.number}-compute@developer.gserviceaccount.com",
  ])

  system_machine_type  = var.system_machine_type == "" ? var.machine_type : var.system_machine_type
  storage_machine_type = var.storage_machine_type == "" ? var.machine_type : var.storage_machine_type
}

data "google_project" "current" {
  project_id = var.project_id
}

resource "google_project_service" "required" {
  for_each = toset(var.enable_required_apis ? local.required_services : [])

  project            = var.project_id
  service            = each.key
  disable_on_destroy = false
}

resource "google_artifact_registry_repository" "images" {
  count = var.create_artifact_registry_repository ? 1 : 0

  project       = var.project_id
  location      = var.region
  repository_id = var.artifact_registry_repository_id
  description   = "REC object store experiment images"
  format        = "DOCKER"
  labels        = local.labels

  depends_on = [google_project_service.required]
}

resource "google_service_account" "gke_nodes" {
  project      = var.project_id
  account_id   = var.node_service_account_id
  display_name = "REC Store GKE experiment nodes"

  depends_on = [google_project_service.required]
}

resource "google_project_iam_member" "node_project_roles" {
  for_each = toset(local.node_project_roles)

  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.gke_nodes.email}"
}

resource "google_artifact_registry_repository_iam_member" "node_reader" {
  project    = var.project_id
  location   = var.region
  repository = var.artifact_registry_repository_id
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${google_service_account.gke_nodes.email}"

  depends_on = [
    google_artifact_registry_repository.images,
    google_project_service.required,
  ]
}

resource "google_project_iam_member" "cloud_build_source_reader" {
  for_each = local.cloud_build_service_accounts

  project = var.project_id
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:${each.key}"

  depends_on = [google_project_service.required]
}

resource "google_artifact_registry_repository_iam_member" "cloud_build_writer" {
  for_each = local.cloud_build_service_accounts

  project    = var.project_id
  location   = var.region
  repository = var.artifact_registry_repository_id
  role       = "roles/artifactregistry.writer"
  member     = "serviceAccount:${each.key}"

  depends_on = [
    google_artifact_registry_repository.images,
    google_project_service.required,
  ]
}

resource "google_compute_network" "experiment" {
  project                 = var.project_id
  name                    = "${var.cluster_name}-vpc"
  auto_create_subnetworks = false
  routing_mode            = "REGIONAL"

  depends_on = [google_project_service.required]
}

resource "google_compute_subnetwork" "experiment" {
  project                  = var.project_id
  name                     = "${var.cluster_name}-subnet"
  region                   = var.region
  network                  = google_compute_network.experiment.id
  ip_cidr_range            = var.subnet_cidr
  private_ip_google_access = true

  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = var.pods_cidr
  }

  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = var.services_cidr
  }
}

resource "google_container_cluster" "experiment" {
  project  = var.project_id
  name     = var.cluster_name
  location = var.zone

  network    = google_compute_network.experiment.id
  subnetwork = google_compute_subnetwork.experiment.id

  remove_default_node_pool = true
  initial_node_count       = 1
  deletion_protection      = false
  networking_mode          = "VPC_NATIVE"
  logging_service          = "logging.googleapis.com/kubernetes"
  monitoring_service       = "monitoring.googleapis.com/kubernetes"
  resource_labels          = local.labels

  ip_allocation_policy {
    cluster_secondary_range_name  = "pods"
    services_secondary_range_name = "services"
  }

  release_channel {
    channel = var.release_channel
  }

  timeouts {
    create = "45m"
    update = "45m"
    delete = "30m"
  }
}

resource "google_container_node_pool" "system" {
  project  = var.project_id
  name     = var.system_node_pool_name
  location = var.zone
  cluster  = google_container_cluster.experiment.name

  node_count = var.system_node_count
  version    = var.node_version == "" ? null : var.node_version

  node_config {
    machine_type    = local.system_machine_type
    disk_type       = var.node_disk_type
    disk_size_gb    = var.node_disk_size_gb
    image_type      = "COS_CONTAINERD"
    service_account = google_service_account.gke_nodes.email
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]

    labels = merge(
      local.labels,
      {
        "rec-store-role" = "system"
      },
    )

    tags = [
      "rec-store-gke-experiment",
    ]

    metadata = {
      disable-legacy-endpoints = "true"
    }
  }

  management {
    auto_repair  = true
    auto_upgrade = var.node_auto_upgrade
  }

  upgrade_settings {
    max_surge       = 1
    max_unavailable = 0
  }

  depends_on = [
    google_artifact_registry_repository_iam_member.node_reader,
    google_project_iam_member.node_project_roles,
  ]

  timeouts {
    create = "45m"
    update = "45m"
    delete = "30m"
  }
}

resource "google_container_node_pool" "storage" {
  project  = var.project_id
  name     = var.storage_node_pool_name
  location = var.zone
  cluster  = google_container_cluster.experiment.name

  node_count = var.storage_node_count
  version    = var.node_version == "" ? null : var.node_version

  node_config {
    machine_type    = local.storage_machine_type
    disk_type       = var.node_disk_type
    disk_size_gb    = var.node_disk_size_gb
    image_type      = "COS_CONTAINERD"
    service_account = google_service_account.gke_nodes.email
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]

    labels = merge(
      local.labels,
      {
        "rec-store-role" = "storage"
      },
    )

    tags = [
      "rec-store-gke-experiment",
    ]

    metadata = {
      disable-legacy-endpoints = "true"
    }
  }

  management {
    auto_repair  = true
    auto_upgrade = var.node_auto_upgrade
  }

  upgrade_settings {
    max_surge       = 1
    max_unavailable = 0
  }

  depends_on = [
    google_artifact_registry_repository_iam_member.node_reader,
    google_project_iam_member.node_project_roles,
  ]

  timeouts {
    create = "45m"
    update = "45m"
    delete = "30m"
  }
}
