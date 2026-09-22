## 1.1 Durability Basics

**What does acks=all actually wait for?**
acks=all means the producer waits for 
the leader AND all in-sync replicas (ISR)
to acknowledge the write before 
considering it successful.
With min.insync.replicas=2, at least 
2 brokers (leader + 1 follower) must 
acknowledge before the producer 
gets success response.

**What happens when one broker 
holding a replica goes offline?**
With RF=3, ISR shrinks from 3 to 2.
Since min.insync.replicas=2, we still
have 2 in-sync replicas available.
Production continues normally.
No data loss. No producer error.

**Why is min.insync.replicas=2 
(not 3) a common choice?**
Setting min.insync.replicas=3 means
ALL replicas must be in sync.
Any single broker loss = full outage.
Setting it to 2 gives a balance:
- Tolerates 1 broker failure
- Still guarantees 2 copies of data
- Production continues during 
  rolling restarts and maintenance 
This is the standard production 
pattern for high availability.
