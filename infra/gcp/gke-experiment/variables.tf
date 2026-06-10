variable "project_id" {
  description = "GCP project ID that owns the experiment resources."
  type        = string
}

variable "region" {
  description = "GCP region for regional resources such as Artifact Registry and VPC subnet."
  type        = string
  default     = "asia-east1"
}

variable "zone" {
  description = "GCP zone for the single-zone GKE Standard experiment cluster."
  type        = string
  default     = "asia-east1-a"
}

variable "cluster_name" {
  description = "GKE cluster name."
  type        = string
  default     = "rec-store-exp"
}

variable "node_pool_name" {
  description = "Backward-compatible storage node pool name. Prefer storage_node_pool_name for new configs."
  type        = string
  default     = "rec-store-storage"
}

variable "node_count" {
  description = "Backward-compatible storage node count. Prefer storage_node_count for new configs."
  type        = number
  default     = 6
}

variable "machine_type" {
  description = "Default machine type for both system and storage node pools."
  type        = string
  default     = "n2-standard-4"
}

variable "system_node_pool_name" {
  description = "Node pool name for API, metadata, TiKV/PD, tiering worker, and benchmark workload pods."
  type        = string
  default     = "rec-store-system"
}

variable "storage_node_pool_name" {
  description = "Node pool name for storage-node pods and storage pressure jobs."
  type        = string
  default     = "rec-store-storage"
}

variable "system_node_count" {
  description = "Number of system nodes reserved for non-storage components."
  type        = number
  default     = 2
}

variable "storage_node_count" {
  description = "Number of storage nodes. Keep at 6 to match the six storage-node StatefulSet replicas."
  type        = number
  default     = 6
}

variable "system_machine_type" {
  description = "Machine type for system nodes. Leave empty to use machine_type."
  type        = string
  default     = ""
}

variable "storage_machine_type" {
  description = "Machine type for storage nodes. Leave empty to use machine_type."
  type        = string
  default     = ""
}

variable "node_disk_type" {
  description = "Boot disk type for GKE worker nodes."
  type        = string
  default     = "pd-balanced"
}

variable "node_disk_size_gb" {
  description = "Boot disk size for each worker node."
  type        = number
  default     = 30
}

variable "release_channel" {
  description = "GKE release channel."
  type        = string
  default     = "REGULAR"
}

variable "node_version" {
  description = "Optional exact GKE node version. Leave empty to use the cluster/release-channel default."
  type        = string
  default     = ""
}

variable "node_auto_upgrade" {
  description = "Whether GKE may auto-upgrade worker nodes. Keep true unless the experiment window needs a frozen version."
  type        = bool
  default     = true
}

variable "node_service_account_id" {
  description = "Service account ID for GKE nodes."
  type        = string
  default     = "rec-store-gke-nodes"
}

variable "artifact_registry_repository_id" {
  description = "Artifact Registry Docker repository ID that stores rec-store images."
  type        = string
  default     = "rec-store"
}

variable "create_artifact_registry_repository" {
  description = "Create the Artifact Registry repository when this is a fresh project. Set false if the repository already exists."
  type        = bool
  default     = false
}

variable "enable_required_apis" {
  description = "Enable Compute Engine, GKE, and Artifact Registry APIs. Services are not disabled on destroy."
  type        = bool
  default     = true
}

variable "subnet_cidr" {
  description = "Primary subnet CIDR for GKE nodes."
  type        = string
  default     = "10.20.0.0/20"
}

variable "pods_cidr" {
  description = "Secondary CIDR for GKE Pod IPs."
  type        = string
  default     = "10.24.0.0/16"
}

variable "services_cidr" {
  description = "Secondary CIDR for Kubernetes Service IPs."
  type        = string
  default     = "10.25.0.0/20"
}

variable "labels" {
  description = "Additional GCP labels applied to managed resources."
  type        = map(string)
  default     = {}
}
