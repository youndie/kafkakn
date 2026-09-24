---
id: B-34
title: "A minimal admin client: create, delete and describe topics, describe the cluster"
status: done
priority: P2
size: M
stage: stage-7-admin
---

# B-34 — a minimal admin client

Every mainstream Kafka client ships an admin API, and so do both of ours — `Admin` on the JVM, the
`rd_kafka_CreateTopics` family on native ([research §1.8](../research/research-architecture.md)). A
kafkakn service that needs a topic to exist has to reach for a second tool today.

- **The decision and its reason.** A separate `kafkaAdmin(config)` with four calls: create topics
  (partitions, replication, configuration), delete topics, describe topics, describe the cluster. The
  native admin API answers through event queues, which is the same seam as the delivery report and is
  bridged the same way.
- **The suite does not start using it to build its own fixtures.** A producer checked by a topic its
  own library created, described by the same library, is a library agreeing with itself — the rule in
  CLAUDE.md against verifying a produce with our own consumer. `broker.sh` stays the fixture; admin is
  checked against `kafka-topics.sh`.
- The rejected alternative is putting these calls on the producer. They are a different lifecycle and
  a different set of permissions, and both clients keep them apart.
- Not covered: ACLs, configuration changes on existing topics, consumer-group administration.

- AC: a topic created through each arm is seen by `kafka-topics.sh --describe` with the partitions,
  replication and configuration asked for; a deleted one is gone.
- AC: describing an existing topic agrees with `kafka-topics.sh`; creating one that exists fails with
  one kafkakn exception on both arms.
- AC: no call holds the caller's dispatcher.
- Anchors: `ci/harness/broker.sh`, `docs/api/producer-contract.md`.

## Findings (2026-09-24)

**Measured, `ci/b-34/run.sh`, both arms.**
- A topic created through kafkakn is described by `kafka-topics.sh` with 5 partitions and replication
  1, and by `kafka-configs.sh` with `retention.ms=123456789` and `cleanup.policy=compact`.
- A topic created and deleted is not in `kafka-topics.sh --list`.
- Describing the fixture's 7-partition topic agrees with `kafka-topics.sh --describe`.
- The cluster id agrees with `kafka-cluster.sh cluster-id`; the node is `1@127.0.0.1:9092` and both
  arms report controller 1.
- Creating an existing topic is `TopicExistsException` on both arms.

**Mutants, each watched red.**
- On the JVM, `get()` in place of awaiting the future held the single-lane dispatcher for 5.0 s. It
  also broke the exists mapping, because `get()` wraps the failure in `ExecutionException`.
- On native, dropping the `TOPIC_ALREADY_EXISTS` mapping turned the exists case into
  `KafkaAdminException (36)`.

**On the way.** librdkafka's `rd_kafka_admin_op_t` comes through cinterop as constants, not an enum
class as `rd_kafka_type_t` does; the first native build failed on that alone. Its default request
timeout is already `socket.timeout.ms`, and create and delete wait up to 60 s on the controller by
default — read in `rdkafka.h`, so the native arm sets neither.

**Left out, as the item says.** ACLs, configuration changes on existing topics, consumer-group
administration. The suite still builds its fixtures with `broker.sh`.
