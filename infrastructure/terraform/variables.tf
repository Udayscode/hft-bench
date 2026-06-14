variable "aws_region" {
  description = "AWS region to deploy infrastructure"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "hackathon-prod"
}

variable "database_url" {
  description = "Connection string for the centralized telemetry database"
  type        = string
  sensitive   = true
}

variable "redpanda_brokers" {
  description = "Comma separated list of Redpanda/Kafka brokers"
  type        = string
}

variable "metal_instance_type" {
  description = "Bare metal instance type for Firecracker microVMs"
  type        = string
  default     = "c5n.metal"
}
