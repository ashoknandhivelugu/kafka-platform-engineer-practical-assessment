#!/usr/bin/env python3
import sys
import json
import time
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient

def fetch_lag_report():
    # Input verification constraints
    if len(sys.argv) != 4:
        sys.stderr.write("Usage: ./lag_report.py <bootstrap_servers> <api_key:api_secret> <consumer_group>\n")
        sys.exit(2)

    bootstrap_servers = sys.argv[1]
    credentials = sys.argv[2]
    target_group = sys.argv[3]

    if ":" not in credentials:
        sys.stderr.write("Error: Credentials input must follow the format 'API_KEY:API_SECRET'\n")
        sys.exit(2)
        
    api_key, api_secret = credentials.split(":", 1)

    # Confluent Cloud standard client configuration dictionary
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': api_key,
        'sasl.password': api_secret
    }

    # Constraint Check: Retry connection twice with a short delay if broker is unreachable
    admin_client = None
    for attempt in range(3): # Attempt 0, Attempt 1 (Retry 1), Attempt 2 (Retry 2)
        try:
            admin_client = AdminClient(conf)
            admin_client.list_topics(timeout=4.0) # Light-weight cluster accessibility test call
            break
        except Exception as e:
            if attempt == 2:
                sys.stderr.write(f"Error: Connection to broker failed after 2 retries. {e}\n")
                sys.exit(1) # Constraint exit code 1
            time.sleep(2)

    # Constraint Check: Validate if the group exists; if not, throw error to stderr and exit code 2
    try:
        group_meta = admin_client.list_groups(group=target_group, timeout=5.0)
        matched = [g for g in group_meta if g.id == target_group]
        if not matched or matched[0].state == "Dead":
            sys.stderr.write(f"Error: Consumer group '{target_group}' does not exist.\n")
            sys.exit(2) # Constraint exit code 2
    except Exception as e:
        sys.stderr.write(f"Error checking group configuration metadata: {e}\n")
        sys.exit(2)

    # Initialize low-overhead consumer mapping to fetch active watermark boundary positions
    conf['group.id'] = target_group
    conf['enable.auto.commit'] = 'false'
    consumer = Consumer(conf)

    try:
        # Discover all partition assignments tracking within the consumer group logs
        cluster_topics = consumer.list_topics(timeout=5.0).topics.values()
        committed_partitions = consumer.committed(cluster_topics, timeout=5.0)
        
        report = []
        for part in committed_partitions:
            # Handle states where partition hasn't registered initialization offset commits yet
            if part.offset == KafkaError._NO_OFFSET or part.offset < 0:
                current_offset = 0
            else:
                current_offset = part.offset

            try:
                # Query broker directly for partition logs boundaries (High bound = Log End Offset)
                _, high_watermark = consumer.get_watermark_offsets(part, timeout=4.0)
                log_end_offset = high_watermark
            except Exception:
                log_end_offset = current_offset

            calculated_lag = max(0, log_end_offset - current_offset)

            report.append({
                "topic": part.topic,
                "partition": part.partition,
                "current_offset": current_offset,
                "log_end_offset": log_end_offset,
                "lag": calculated_lag
            })

        # Constraint requirement: Emit clean structural format string directly to stdout
        print(json.dumps(report, indent=2))

    finally:
        consumer.close()

if __name__ == "__main__":
    fetch_lag_report()

