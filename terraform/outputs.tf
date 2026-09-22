output "kafka_bootstrap_endpoint" {
  value       = confluent_kafka_cluster.standard.bootstrap_endpoint
  description = "The Kafka cluster bootstrap URL needed by producers/consumers to establish connection."
}

