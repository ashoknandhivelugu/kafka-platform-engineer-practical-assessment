# Kafka Broker Safe Rolling Upgrade Automation

This playbook automates safe, zero-downtime rolling configuration updates across your Kafka cluster.

### Operational Characteristics
* **Idempotency Gate:** The execution logic tracks structural file changes. If zero modifications are detected on a target host, the daemon restart block is skipped entirely.
* **Under-Replicated Partitions Safety Guard:** Prior to dropping any broker, an automated cluster evaluation check runs. If active under-replicated partitions are caught anywhere on the fabric, the playbook halts execution immediately.

### Safe Dry-Run Evaluation Syntax
To validate formatting, verify template variables, and preview target configuration diff structural states without altering files or dropping daemons, use:
```bash
ansible-playbook -i inventory.ini site.yml --check --diff
```
