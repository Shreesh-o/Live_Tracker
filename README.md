# Apache Kafka — Learning Journey 🚀

This repository documents my hands-on journey learning **Apache Kafka** by progressively building a real-time data pipeline.

Instead of learning Kafka only through theory, I built each concept step-by-step, committed the changes to GitHub, and gradually evolved a simple API producer into a more reliable, scalable Kafka-based streaming system.

---

## 🎯 Goal

The goal of this project is to understand how Apache Kafka works internally and practically by building a real-time streaming pipeline.

The learning process focuses on:

- Kafka Producers
- Kafka Consumers
- Topics and Partitions
- Message Keys
- Consumer Groups
- Parallel Processing
- Offsets
- Consumer Recovery
- Manual Offset Commits
- At-least-once delivery
- Message Replay
- Producer Reliability
- Idempotence

---

## 🏗️ Learning Architecture

The project gradually evolved into the following architecture:

```text
             External API
                 │
                 ▼
        ┌─────────────────┐
        │  Kafka Producer │
        └────────┬────────┘
                 │
                 │ JSON Events
                 ▼
        ┌─────────────────┐
        │  Kafka Topic    │
        │                 │
        │ P0 │ P1 │ P2 │ P3│
        └──┬──┬──┬──┬────┘
           │  │  │  │
           ▼  ▼  ▼  ▼
        Consumer Group
        ┌────┬────┬────┐
        │ C1 │ C2 │ C3 │
        └────┴────┴────┘
                 │
                 ▼
          Message Processing
                 │
                 ▼
          Manual Offset Commit
```

The architecture was intentionally developed incrementally so that each Git commit represents a new Kafka concept.

---

# 📚 Learning Progression

## V1 — Kafka Fundamentals

The first stage focused on understanding the basic Kafka ecosystem.

### Concepts learned

- Kafka brokers
- Topics
- Partitions
- Producers
- Consumers
- Messages
- Kafka CLI/tools
- Basic producer → consumer communication

The objective was to understand the basic flow:

```text
Producer → Topic → Consumer
```

---

# V2 — API Producer

### Branch

```text
v2-API_Producer
```

The project was extended to consume real data from an external API and publish it to Kafka.

### Concepts learned

- Calling an external API
- Processing JSON responses
- Converting API data into Kafka messages
- Kafka Producer
- Message keys
- Structured event data

The producer was initially designed around cryptocurrency market data such as:

- BTC
- ETH
- SOL

The system was later expanded to support additional assets.

### Basic flow

```text
API
 │
 ▼
Python Producer
 │
 ▼
JSON Event
 │
 ▼
Kafka Topic
```

Example event:

```json
{
  "symbol": "BTC",
  "price": 105000,
  "timestamp": "2026-09-22T10:00:00"
}
```

---

# V3 — Kafka Consumer Groups

### Branch

```text
v3-Kafka_Consumer_Groups
```

The next stage introduced **Consumer Groups**.

Instead of having a single consumer process every message, multiple consumers were placed inside the same consumer group.

### Concepts learned

- Consumer groups
- Consumer group IDs
- Partition assignment
- Load distribution
- Multiple consumers
- Kafka's parallel consumption model

Example:

```text
Kafka Topic
│
├── Partition 0 ──► Consumer 1
├── Partition 1 ──► Consumer 2
├── Partition 2 ──► Consumer 3
└── Partition 3 ──► Consumer 4
```

This demonstrated how Kafka distributes partitions among consumers belonging to the same consumer group.

---

# V4 — Partitioning & Parallelism

### Branch

```text
v4-Kafka_Partition_Parallelism
```

The project was then extended to explore Kafka's partitioning model in more detail.

The topic was configured with multiple partitions, eventually reaching:

```text
3 → 4 partitions
```

### Concepts learned

- Why Kafka uses partitions
- Partition distribution
- Parallel processing
- Consumer-to-partition assignment
- Message keys
- Ordering within partitions
- Scalability through partitions

### Important concept

Kafka guarantees ordering **within a partition**, rather than across the entire topic.

Using a key allows related messages to consistently map to the same partition.

For example:

```text
BTC → Partition 1
BTC → Partition 1
BTC → Partition 1

ETH → Partition 2
ETH → Partition 2

SOL → Partition 3
SOL → Partition 3
```

This helps maintain ordering for messages belonging to the same key.

---

# V5 — Kafka Reliability

### Branch

```text
v5-Kafka_Reliability
```

The latest stage focused on making the pipeline more reliable.

### Concepts learned

- Kafka offsets
- Automatic offset commits
- Manual offset commits
- Consumer recovery
- Message replay
- At-least-once delivery
- Producer acknowledgements
- Producer retries
- Idempotent producers

---

## Producer Reliability

The producer was configured with stronger delivery guarantees.

Important settings included:

```python
acks="all"
retries=5
enable.idempotence=True
```

### `acks="all"`

The producer waits for acknowledgement from all required in-sync replicas before considering the message successfully written.

### `retries=5`

The producer retries failed message deliveries.

### `enable.idempotence=True`

Idempotence helps prevent duplicate records caused by producer retries.

Together, these settings provide a more reliable producer configuration than a basic fire-and-forget producer.

---

# Consumer Reliability

The consumer was changed from automatic offset commits to manual commits.

Instead of:

```text
Receive message
     ↓
Automatically commit offset
     ↓
Process message
```

the pipeline uses:

```text
Receive message
     ↓
Process message
     ↓
Processing successful?
     ↓
Commit offset
```

The consumer uses:

```python
enable.auto.commit=False
```

and commits the offset after successful processing.

---

# 🔄 At-Least-Once Processing

The reliability implementation demonstrates the concept of **at-least-once delivery**.

The basic idea is:

```text
Message received
       ↓
Process message
       ↓
      ┌──────────────┐
      │ Processing OK│
      └──────┬───────┘
             │
             ▼
       Commit offset
```

If the consumer crashes before committing the offset:

```text
Message
   ↓
Processing
   ↓
Consumer crashes
   X
Offset NOT committed
```

When the consumer restarts, Kafka can deliver that message again.

This means the application may process a message more than once, but it avoids losing the message before its processing has been successfully acknowledged.

---

# 🔁 Message Replay

Offsets also make it possible to replay messages.

Conceptually:

```text
Partition

0   1   2   3   4   5   6
            ▲
          Offset
```

A consumer can resume from a previously committed offset rather than requiring the entire stream to start over.

This is one of the major differences between Kafka and traditional message queues.


# 🗂️ Git Learning Progression

The Git history is intentionally structured around the Kafka concepts learned.

---

```text
V1
│
├── Kafka Fundamentals
│
▼
V2
│
├── API Producer
├── JSON Events
├── Message Keys
│
▼
V3
│
├── Consumer Groups
├── Multiple Consumers
│
▼
V4
│
├── Partitions
├── Partition Parallelism
│
▼
V5
│
├── Offsets
├── Manual Commits
├── Consumer Recovery
├── At-Least-Once Processing
├── Producer Reliability
└── Idempotence
