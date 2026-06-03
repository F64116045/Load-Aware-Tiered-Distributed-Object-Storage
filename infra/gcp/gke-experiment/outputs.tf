output "project_id" {
  value = var.project_id
}

output "region" {
  value = var.region
}

output "zone" {
  value = var.zone
}

output "cluster_name" {
  value = google_container_cluster.experiment.name
}

output "node_pool_name" {
  value = google_container_node_pool.workers.name
}

output "node_service_account" {
  value = google_service_account.gke_nodes.email
}

output "artifact_registry_repository_id" {
  value = var.artifact_registry_repository_id
}

output "artifact_registry_image_prefix" {
  value = "${var.region}-docker.pkg.dev/${var.project_id}/${var.artifact_registry_repository_id}"
}

output "image_example" {
  value = "${var.region}-docker.pkg.dev/${var.project_id}/${var.artifact_registry_repository_id}/rec-store:gke-exp-001"
}

output "get_credentials_command" {
  value = "gcloud container clusters get-credentials ${google_container_cluster.experiment.name} --zone ${var.zone} --project ${var.project_id}"
}

output "configure_docker_command" {
  value = "gcloud auth configure-docker ${var.region}-docker.pkg.dev"
}

output "deploy_command" {
  value = "IMAGE=${var.region}-docker.pkg.dev/${var.project_id}/${var.artifact_registry_repository_id}/rec-store:gke-exp-001 RESET_NAMESPACE=true ./deploy/gke/scripts/deploy.sh"
}

output "destroy_command" {
  value = "terraform -chdir=infra/gcp/gke-experiment destroy"
}
