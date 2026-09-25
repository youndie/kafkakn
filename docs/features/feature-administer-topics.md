---
id: feature-administer-topics
title: Administer topics and describe the cluster
type: feature
status: active
owner: unassigned
involved_services:
  - kafkakn-core
  - test-broker
client_entries: []
api:
  - producer-contract
tags: [admin]
---

# Administer topics and describe the cluster

## 1. Overview

A service creates the topics it writes to, deletes the ones it no longer needs, describes them, and
describes the cluster. On both arms, and checked with the broker's own tools
([B-34](../backlog/B-34-a-minimal-admin.md); the admin section of the
[producer contract](../api/producer-contract.md)).

**Built and measured on both arms.**

## 2. Business rules

- A topic is created with a partition count, a replication factor and a configuration map, and the
  broker holds exactly those.
- Creating a topic that exists fails with one kafkakn exception on both arms, `TopicExistsException`.
- No call holds the caller's dispatcher while the cluster answers.

## 3. Code anchors

| Service | Code |
|---|---|
| kafkakn-core | `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/KafkaAdmin.kt`: the surface |
| kafkakn-core | `kafkakn-core/src/jvmMain/kotlin/io/github/youndie/kafkakn/KafkaAdmin.jvm.kt` |
| kafkakn-core | `kafkakn-core/src/nativeMain/kotlin/io/github/youndie/kafkakn/KafkaAdmin.native.kt` |

## 4. Scenarios (BDD / test cases)

### Scenario: A created topic is what was asked for
* **Given:** a name nobody uses.
* **When:** each arm creates it with 5 partitions, replication 1, `retention.ms=123456789` and
  `cleanup.policy=compact`.
* **Then:** `kafka-topics.sh --describe` and `kafka-configs.sh --describe` show exactly those.
* **Automated:** `AdminTest.a_created_topic_has_the_partitions_replication_and_configuration_asked_for`,
  checked by `ci/b-34/run.sh`.

### Scenario: A deleted topic is gone
* **When:** each arm deletes a topic it created.
* **Then:** `kafka-topics.sh --list` no longer lists it.
* **Automated:** `AdminTest.a_deleted_topic_is_gone`, checked by `ci/b-34/run.sh`.

### Scenario: A topic and the cluster are described as the broker describes them
* **When:** each arm describes a topic, and the cluster.
* **Then:** every partition's leader, replicas and in-sync replicas match `kafka-topics.sh --describe`
  of the same topic, and the cluster id matches `kafka-cluster.sh cluster-id`. The nodes and the
  controller each arm reports are printed next to it, not compared.
* **Automated:** `AdminTest.describing_a_topic_agrees_with_the_broker` and
  `AdminTest.describing_the_cluster_names_its_id_and_its_broker`, checked by `ci/b-34/run.sh`.

### Scenario: Creating a topic that exists fails the same way on both arms
* **When:** each arm creates a topic that already exists.
* **Then:** it fails with `TopicExistsException`.
* **Automated:** `AdminTest.creating_a_topic_that_exists_fails_with_one_kafkakn_exception_on_both_arms`.

## 5. Out of scope

Consumer-group administration, topic configuration on an existing topic, adding partitions and deleting
records are stage 13 ([B-58](../backlog/B-58-list-and-describe-consumer-groups.md) to
[B-63](../backlog/B-63-delete-records.md)). ACLs are not planned.
