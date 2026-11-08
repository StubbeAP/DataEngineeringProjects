terraform {
  required_version = ">= 1.5.0"
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.87.0"
    }
  }
}

provider "snowflake" {
  account     = var.snowflake_account
  user        = var.snowflake_user
  role        = var.snowflake_role
  private_key = file(var.snowflake_private_key_path)
}

# 1. Virtual Compute Warehouse
resource "snowflake_warehouse" "crypto_wh" {
  name           = var.warehouse_name
  warehouse_size = "x-small"
  auto_suspend   = 60
  auto_resume    = true
  initially_suspended = true
  comment        = "Compute warehouse for Crypto streaming transformations"
}

# 2. Database and Schemas
resource "snowflake_database" "crypto_db" {
  name    = var.database_name
  comment = "Analytics database for real-time crypto trades"
}

resource "snowflake_schema" "public" {
  database = snowflake_database.crypto_db.name
  name     = "PUBLIC"
  comment  = "Default schema for ingested Kafka tables"
}

# 3. RBAC Role
resource "snowflake_role" "kafka_connector_role" {
  name    = var.kafka_role_name
  comment = "Dedicated service role for Airflow orchestration and Kafka Connect"
}

# 4. Role Grants
resource "snowflake_grant_privileges_to_role" "warehouse_usage" {
  privileges = ["USAGE"]
  role_name  = snowflake_role.kafka_connector_role.name
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.crypto_wh.name
  }
}

resource "snowflake_grant_privileges_to_role" "database_usage" {
  privileges = ["USAGE"]
  role_name  = snowflake_role.kafka_connector_role.name
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.crypto_db.name
  }
}

resource "snowflake_grant_privileges_to_role" "schema_all" {
  privileges = ["USAGE", "CREATE TABLE", "CREATE STAGE", "CREATE PIPE"]
  role_name  = snowflake_role.kafka_connector_role.name
  on_schema {
    schema_name = "\"${snowflake_database.crypto_db.name}\".\"${snowflake_schema.public.name}\""
  }
}
