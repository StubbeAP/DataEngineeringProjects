variable "snowflake_account" {
  type        = string
  description = "Snowflake Account Identifier"
  default     = "dummy_account"
}

variable "snowflake_user" {
  type        = string
  description = "Admin or deployment user for executing Terraform"
  default     = "ACCOUNTADMIN"
}

variable "snowflake_role" {
  type        = string
  description = "Role for executing Terraform"
  default     = "ACCOUNTADMIN"
}

variable "snowflake_password" {
  type        = string
  description = "Password for the Snowflake admin user"
  sensitive   = true
  default     = null
}

variable "snowflake_private_key_path" {
  type        = string
  description = "Path to RSA private key (if using key-pair authentication)"
  default     = null
}

variable "snowflake_region" {
  type        = string
  description = "Snowflake cloud region"
  default     = "us-east-1"
}

variable "database_name" {
  type        = string
  description = "Name of the analytics database"
  default     = "CRYPTO_DB"
}

variable "warehouse_name" {
  type        = string
  description = "Name of the compute warehouse"
  default     = "CRYPTO_WH"
}

variable "kafka_role_name" {
  type        = string
  description = "Role for Airflow and Kafka Connector"
  default     = "KAFKA_CONNECTOR_ROLE"
}

variable "kafka_user_name" {
  type        = string
  description = "Service user for Airflow / Kafka"
  default     = "KAFKA_CONNECTOR_USER"
}
