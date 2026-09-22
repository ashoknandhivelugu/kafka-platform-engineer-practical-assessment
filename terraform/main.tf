terraform {
  required_version = ">= 1.5.0"
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 1.60.0" # Constraint: Explicitly pinning the provider version
    }
  }
}

provider "confluent" {
  cloud_api_key    = var.confluent_cloud_api_key
  cloud_api_secret = var.confluent_cloud_api_secret
}

# 1. Environment
resource "confluent_environment" "staging" {
  display_name = "assessment-environment"
}

# 2. Schema Registry (Essentials Tier)
resource "confluent_schema_registry_cluster" "essentials" {
  package = "ESSENTIALS"
  environment {
    id = confluent_environment.staging.id
  }
  region {
    id = "sg-1" 
  }
}

# 3. Kafka Cluster (Standard Tier)
resource "confluent_kafka_cluster" "standard" {
  display_name = "orders-cluster"
  availability = "SINGLE_ZONE"
  cloud        = "GCP"
  region       = var.region
  standard     = {} 

  environment {
    id = confluent_environment.staging.id
  }
}

# Admin Service Account required to execute topic management commands
resource "confluent_service_account" "admin" {
  display_name = "cluster-admin-sa"
  description  = "Service account used for pipeline management tasks"
}

resource "confluent_role_binding" "admin_rb" {
  principal   = "User:${confluent_service_account.admin.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.standard.rbac_crn
}

resource "confluent_api_key" "admin_keys" {
  display_name = "admin-api-key"
  owner {
    id          = confluent_service_account.admin.id
    api_version = confluent_service_account.admin.api_version
    kind        = confluent_service_account.admin.kind
  }
  managed_resource {
    id          = confluent_kafka_cluster.standard.id
    api_version = confluent_kafka_cluster.standard.api_version
    kind        = confluent_kafka_cluster.standard.kind
    environment {
      id = confluent_environment.staging.id
    }
  }
  depends_on = [confluent_role_binding.admin_rb]
}

# 4. Kafka Topic orders.v1 with explicit constraints and comments
resource "confluent_kafka_topic" "orders" {
  kafka_cluster {
    id = confluent_kafka_cluster.standard.id
  }
  
  topic_name       = "orders.v1"
  partitions_count = 6 # Explict partition count to manage scaling and dynamic consumer group parallelisms.
  
  config = {
    "replication.factor"  = "3" # Configures three physical copies across nodes to guarantee fault tolerance if brokers drop.
    "min.insync.replicas" = "2" # Requires at least two replicas to commit writes, maintaining availability during 1 node outage.
  }

  credentials {
    key    = confluent_api_key.admin_keys.id
    secret = confluent_api_key.admin_keys.secret
  }
}

# 5. Application Service Account (Least-Privilege Producer)
resource "confluent_service_account" "producer" {
  display_name = "orders-producer-sa"
  description  = "Least-privilege account restricted strictly to publishing onto the orders topic"
}

# Grant WRITE permissions on the orders.v1 topic only
resource "confluent_kafka_acl" "producer_write" {
  kafka_cluster {
    id = confluent_kafka_cluster.standard.id
  }
  principal     = "User:${confluent_service_account.producer.id}"
  host          = "*"
  operation     = "WRITE"
  permission    = "ALLOW"
  pattern_type  = "LITERAL"
  resource_name = confluent_kafka_topic.orders.topic_name

  credentials {
    key    = confluent_api_key.admin_keys.id
    secret = confluent_api_key.admin_keys.secret
  }
}

# Grant DESCRIBE permissions (required for metadata lookups before writing)
resource "confluent_kafka_acl" "producer_describe" {
  kafka_cluster {
    id = confluent_kafka_cluster.standard.id
  }
  principal     = "User:${confluent_service_account.producer.id}"
  host          = "*"
  operation     = "DESCRIBE"
  permission    = "ALLOW"
  pattern_type  = "LITERAL"
  resource_name = confluent_kafka_topic.orders.topic_name

  credentials {
    key    = confluent_api_key.admin_keys.id
    secret = confluent_api_key.admin_keys.secret
  }
}

