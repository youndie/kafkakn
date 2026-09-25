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
- A group's lag is the caller's subtraction, the partition's latest offset minus the group's commit: the
  library reads both and computes neither.

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

### Scenario: Consumer groups are listed and described as the broker's tool describes them
* **Given:** a group with a live member, the same group once its member left, a group that never existed,
  and a group whose member is the distribution's console consumer.
* **When:** each arm lists and describes them.
* **Then:** the live group is `STABLE` with its one member and assignment, the left one `EMPTY`, and the
  missing one `DEAD` with no members, on both arms. The console consumer's member id, host, client id and
  assignment are what `kafka-consumer-groups.sh --describe --members --verbose` prints.
* **Automated:** `AdminGroupsTest` on both arms, held against the broker's tool by `ci/b-58/run.sh`.

### Scenario: A group's commits and a partition's offsets are what the broker's tools print
* **Given:** a topic of three partitions holding five, three and no records, a second apart, and a group
  that committed offsets 4 and 3 on the first two without ever joining.
* **When:** each arm lists the group's offsets, and lists the topic's offsets as earliest, latest and at four
  timestamps.
* **Then:** the commits are `0:4 1:3`, as `kafka-consumer-groups.sh --describe` prints them; every offset is
  what `kafka-get-offsets.sh --time` prints, and a partition with no record that late is null where the tool
  prints nothing. A group that does not exist has no offsets. Both arms answer alike.
* **Automated:** `AdminOffsetsTest` on both arms, held against the broker's tools by `ci/b-59/run.sh`.

## 5. Out of scope

Resetting and deleting group offsets ([B-60](../backlog/B-60-reset-and-delete-group-offsets.md)), topic configuration on an existing topic, adding partitions and deleting
records are stage 13 ([B-58](../backlog/B-58-list-and-describe-consumer-groups.md) to
[B-63](../backlog/B-63-delete-records.md)). ACLs are not planned.
