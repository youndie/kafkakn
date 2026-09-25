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
- A topic's partition count only grows, and growing it moves keyed records written afterwards.
- A topic's configuration is changed incrementally only: a key not named is never reset.
- A group with an active member is never moved or deleted: the broker refuses, and both arms throw
  `GroupNotEmptyException`.
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

### Scenario: An empty group's offsets are moved, and a member that joins reads from there
* **Given:** a group that committed offset 2 of partition 0 and has no member left.
* **When:** its offsets are moved to 7, and a member joins afterwards.
* **Then:** the group's commit is 7, as `kafka-consumer-groups.sh --describe` prints it. The first record the
  member reads is offset 7, both for a kafkakn member and for the distribution's console consumer.
* **Automated:** `AdminGroupOffsetsTest.an_empty_groups_offsets_are_moved_and_a_member_that_joins_reads_from_there`,
  and `ci/b-60/run.sh`.

### Scenario: A group with an active member is refused
* **Given:** a group with one assigned member.
* **When:** its offsets are altered or deleted, or the group is deleted.
* **Then:** each call throws `GroupNotEmptyException` on both arms, and the member's commit is untouched.
* **Automated:** `AdminGroupOffsetsTest.a_group_with_an_active_member_is_refused_with_one_exception_on_both_arms`.

### Scenario: An empty group's offsets, and then the group, are deleted
* **Given:** an empty group with commits on two partitions.
* **When:** the commit of one partition is deleted, and then the group is deleted.
* **Then:** only the other partition's commit is left. Once the group is deleted it is neither listed nor
  has any offsets, and `kafka-consumer-groups.sh --list` does not show it.
* **Automated:** `AdminGroupOffsetsTest.an_empty_groups_offsets_and_then_the_group_are_deleted`, and
  `ci/b-60/run.sh`.

### Scenario: A topic's configuration is described, changed incrementally, and returned to its default
* **Given:** a topic created with `retention.ms` set.
* **When:** `max.message.bytes` is set on it, and then `retention.ms` is deleted.
* **Then:** the topic's configuration changes as follows:
  - the set key reads its new value with source `TOPIC`;
  - the key set at creation is left alone by the first change;
  - the deleted key returns to its default with source `DEFAULT`;
  - every key, value and source is what `kafka-configs.sh --describe --all` reports.

  An unknown key, or a value the broker cannot read, is refused with `IllegalArgumentException` on both
  arms. The valid half of such a call is not applied.
* **Automated:** `AdminConfigsTest` on both arms, held against the broker's tool by `ci/b-61/run.sh`.

### Scenario: A topic grows, and the same keys written after it land elsewhere
* **Given:** a one-partition topic holding eight keyed records.
* **When:** it is grown to four partitions, and the same eight keys are written again.
* **Then:** `kafka-topics.sh --describe` reports four partitions, and the keys spread over them the same way on
  both arms, as the broker's end offsets count. A count that does not grow the topic is refused with
  `IllegalArgumentException` on both arms, and the topic keeps its partitions.
* **Automated:** `AdminPartitionsTest` on both arms, held against the broker's tools by `ci/b-62/run.sh`.

### Scenario: Records before an offset are deleted, and the answer is the broker's low watermark
* **Given:** a topic with ten records in partition 0 and five in partition 1.
* **When:** each arm deletes partition 0 before 4, then before 2, then up to its end.
* **Then:**
  - the calls return 4, then 4 again (not the 2 that was asked for), then 10;
  - partition 1, asked before 0, stays at 0;
  - `kafka-get-offsets.sh --time -2` reports the same watermarks.

  An offset past the end is refused with `IllegalArgumentException` on both arms, and nothing is deleted.
* **Automated:** `AdminDeleteRecordsTest` on both arms, held against the broker's tool by `ci/b-63/run.sh`.

## 5. Out of scope

Broker configuration, which is an operator's tool, not a service's. ACLs are not planned.
