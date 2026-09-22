variable "confluent_cloud_api_key" {
  type        = string
  description = "Confluent Cloud API Key for the Terraform Provider"
  sensitive   = true
}

variable "confluent_cloud_api_secret" {
  type        = string
  description = "Confluent Cloud API Secret for the Terraform Provider"
  sensitive   = true
}

variable "region" {
  type        = string
  default     = "us-central1"
  description = "GCP region where the cluster will be deployed"
}
