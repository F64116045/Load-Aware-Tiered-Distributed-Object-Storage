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
  description = "Fixed-size worker node pool name."
  type        = string
  default     = "rec-store-workers"
}

variable "node_count" {
  description = "Number of worker nodes. Keep fixed for comparable experiments."
  type        = number
  default     = 6
}

variable "machine_type" {
  description = "Worker node machine type. Use n2-standard-4 for pilot runs; consider n2/n4-standard-8 for final runs."
  type        = string
  default     = "n2-standard-4"
}

variable "node_disk_type" {
  description = "Boot disk type for GKE worker nodes."
  type        = string
  default     = "pd-balanced"
}

variable "node_disk_size_gb" {
  description = "Boot disk size for each worker node."
  type        = number
  default     = 50
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
