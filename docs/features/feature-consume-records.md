---
id: feature-consume-records
title: Consume records — assign, seek, poll, groups, commits
type: feature
status: active
owner: unassigned
involved_services:
  - kafkakn-core
  - test-broker
client_entries: []
api:
  - consumer-contract
tags: [consumer]
---

# Consume records — assign, seek, poll, groups, commits

## 1. Overview

A Kotlin service reads records from Kafka, on the JVM or in a single native binary, with one API on both.
It reads the partitions it names, or joins a group and shares a topic's partitions with the group's
other members. It commits how far it got, and is told when partitions move.

**Built and measured on both arms.** Every scenario below is checked against a third party: records
written by the Kafka distribution's own client, and read back or counted by the broker's own tools.
The contract is [consumer-contract](../api/consumer-contract.md).

## 2. Business rules

- Records are bytes, with key and value nullable (a null value is a tombstone), and headers ordered with
  duplicates kept.
- `poll` returns what is available, up to 500, or an empty list when its timeout passes. It suspends
  without holding the caller's dispatcher, and a cancelled `poll` leaves the consumer usable.
- Calls on one consumer never overlap: they queue.
- Auto-commit is off on both arms. A group resumes from what was committed, so records returned and
  not committed are delivered again (at-least-once).
- `subscribe` and `commit` need a `group.id` the caller named.
- A rebalance listener runs inside `poll`, and commits only through the scope it is given.

## 3. Code anchors

| Service | Code |
|---|---|
| kafkakn-core | `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/KafkaConsumer.kt`: the surface |
| kafkakn-core | `kafkakn-core/src/jvmMain/kotlin/io/github/youndie/kafkakn/KafkaConsumer.jvm.kt` |
| kafkakn-core | `kafkakn-core/src/nativeMain/kotlin/io/github/youndie/kafkakn/KafkaConsumer.native.kt` |
| test-broker | `ci/harness/Records.java`: the third party that writes and dumps records |

## 4. Scenarios (BDD / test cases)

### Scenario: An assigned partition is read byte for byte and in order
* **Given:** the consumer fixture, twenty records written by the distribution's own client, among them
  null keys, a tombstone, bytes that are not UTF-8, and duplicate header names.
* **When:** each arm assigns the partition, seeks to the beginning and polls.
* **Then:** each arm reads all twenty in offset order, and its rendering of every record equals the
  third party's dump, byte for byte.
* **Automated:** `ConsumerTest.both_arms_read_every_record_of_an_assigned_partition_in_order`, compared
  by `ci/b-36/run.sh`.

### Scenario: A seek lands where the broker says
* **Given:** the same fixture.
* **When:** each arm seeks to the beginning, to offset 7, to a timestamp, and to the end.
* **Then:** the first record after each seek is the one `kafka-get-offsets.sh` names for that position,
  and a seek to the end reads nothing.
* **Automated:** `ConsumerTest.seeking_lands_where_the_broker_says`, checked by `ci/b-36/run.sh`.

### Scenario: Calls on one consumer queue rather than interleave
* **Given:** one consumer.
* **When:** eight coroutines poll it at once, twenty times each.
* **Then:** every record is read exactly once and nothing throws, on both arms.
* **Automated:** `ConsumerTest.overlapping_calls_on_one_consumer_are_queued_not_interleaved`.

### Scenario: A cancelled poll returns promptly
* **Given:** a `poll` waiting on a partition with nothing to read.
* **When:** its caller is cancelled.
* **Then:** it returns promptly, and the consumer's next call is served at once.
* **Automated:** `ConsumerTest.cancelling_a_waiting_poll_returns_promptly_and_the_consumer_stays_usable`,
  and `ConsumerTest.a_waiting_poll_does_not_hold_the_callers_dispatcher`.

### Scenario: A group shares partitions and a crash loses nothing
* **Given:** a group of two members on a four-partition topic that is being written to.
* **When:** one member stops without committing its last batch, the way a crash would.
* **Then:** the other member takes its partitions, and every record the broker holds was processed. The
  abandoned batch is delivered again, and the group's commits reach every partition's end.
* **Automated:** `GroupTest.two_members_share_the_partitions_hand_them_over_and_lose_nothing` on each arm,
  and `GroupTest.a_member_of_a_group_whose_other_member_is_the_other_arm` with one member per arm,
  counted by `ci/b-37/run.sh`.

### Scenario: A group needs a group id the caller named
* **Given:** a consumer with no `group.id`.
* **When:** it subscribes, or commits.
* **Then:** both are refused, on both arms, with a message naming `group.id`. Assigning and polling still
  work.
* **Automated:** `GroupTest.subscribe_and_commit_need_a_group_the_caller_named_on_both_arms`.

### Scenario: A named offset is what the group resumes from
* **Given:** a consumer that has read the whole fixture.
* **When:** it commits offset 7.
* **Then:** `kafka-consumer-groups.sh` shows 7, and a new member of the group reads offsets 7 to 19.
* **Automated:** `CommitOffsetsTest.a_named_offset_is_what_the_group_resumes_from`, read back by
  `ci/b-48/run.sh`.

### Scenario: A consumer knows where it is, before it has read anything too
* **Given:** the consumer fixture, twenty records from offset 0.
* **When:** each arm seeks to the beginning, the end and offset 7, reads everything, and seeks back.
  Then a new member of a group that committed 7, and a new group, each ask before their first poll.
* **Then:** the positions are 0, 20, 7, 20 and 7, then 7 for the member and 0 for the new group. The two
  arms agree on every one, and `committed` is what `kafka-consumer-groups.sh` shows.
* **Automated:** `PositionTest` on both arms, compared by `ci/b-49/run.sh`.

### Scenario: A member seeks within what its group gave it
* **Given:** a lone member of a new group, given the consumer fixture's one partition.
* **When:** it reads everything and seeks to 7; or its listener seeks to 12 as the partition arrives; or it
  seeks a partition the group did not give it.
* **Then:** the next record is 7 in the first case, and the first record it is given is 12 in the second.
  The third is refused with `IllegalStateException`. The two arms agree.
* **Automated:** `GroupSeekTest` on both arms, compared by `ci/b-51/run.sh`.

### Scenario: A paused partition returns nothing, and the member stays
* **Given:** a topic with 2000 records on partition 0 and a member polling it, with `max.poll.interval.ms`
  at 6 s.
* **When:** partition 0 is paused after its first batch, and the member polls for 10 s while partition 1
  receives records. Then partition 0 is resumed.
* **Then:** partition 0 returns nothing while paused, partition 1 returns its records, and the member still
  holds both and can commit. After `resume`, partition 0's 2000 records arrive exactly once. A seek does not
  undo a pause.
* **Automated:** `PauseTest` on both arms, counted against the broker by `ci/b-52/run.sh`.

### Scenario: A consumer reports how far behind it is
* **Given:** a partition of 2000 records.
* **When:** a consumer reads to 500 and pauses it, before and after committing, and then reads everything.
* **Then:** its lag is 1500 on both arms, as `kafka-consumer-groups.sh` computes it, and 0 once
  everything is read.
* **Automated:** `ConsumerLagTest` on both arms, against the broker's tool in `ci/b-53/run.sh`.

### Scenario: Records as a Flow, cancelled promptly
* **Given:** a consumer on the fixture.
* **When:** a caller collects `records()`, or cancels a collector waiting at the end of the partition.
* **Then:** the twenty records arrive in order, a cancelled collector stops within milliseconds, and the
  consumer reads again afterwards, on both arms.
* **Automated:** `RecordsFlowTest` on both arms, by `ci/b-54/run.sh`. A collector slower than
  `max.poll.interval.ms` is reported to the listener as lost, rejoins at the next poll and reads again from
  the committed offset, the same way on both arms (B-64).

### Scenario: A cooperative group moves only what changes owner
* **Given:** a group on a six-partition topic with `partition.assignment.strategy=cooperative-sticky`: a
  JVM member, a native member, and a third, native, joining 30 s later, while records are being written.
* **When:** each member arrives.
* **Then:** earlier members give up only the partitions that move and keep reading the others; nobody
  gives up everything mid-stream. Nothing is lost or processed twice, counted against the broker. A
  strategy only one arm understands is refused at construction.
* **Automated:** `CooperativeTest` on both arms, the group run by `ci/b-55/run.sh`.

### Scenario: A member that commits on revocation hands over without loss or duplicates
* **Given:** a group with one member on each arm, each committing only in its revocation callback.
* **When:** one member processes 50 records and leaves, in both directions.
* **Then:** no record the broker holds is lost, none is processed twice, and the leaver's last listener
  event is its revocation.
* **Automated:** `RebalanceListenerTest.a_member_that_commits_only_on_revocation_hands_over_without_loss_or_duplicates`,
  counted by `ci/b-50/run.sh`.

### Scenario: A callback that calls the consumer is refused, not deadlocked
* **Given:** a listener whose `onAssigned` calls the consumer.
* **When:** the consumer is assigned a partition.
* **Then:** the call throws `IllegalStateException`, on both arms.
* **Automated:** `RebalanceListenerTest.calling_the_consumer_from_inside_a_callback_throws_instead_of_deadlocking`.

### Scenario: A static member restarts without a rebalance
* **Given:** a group of two members on a two-partition topic, each with a `group.instance.id`.
* **When:** one member is closed and reopened within `session.timeout.ms`, on each arm and in a mixed group
  both ways.
* **Then:** it holds the same partition as before. The other member's listener hears nothing while it is
  away and back, and the broker's log shows no rebalance in that window. Without the key, the same restart
  is a rebalance the other member hears (the control).
* **Automated:** `StaticMembershipTest` on both arms; the broker's log and the mixed group are read by
  `ci/b-56/run.sh`.

### Scenario: A second member with the same instance id fences the first
* **Given:** a member with a `group.instance.id`.
* **When:** a second member joins with the same id.
* **Then:** the second holds the partitions, and the first's next `poll` throws `ConsumerFencedException`,
  on both arms. So do its commits, explicit and positional, and the group keeps the second member's offsets,
  as `kafka-consumer-groups.sh --describe` reads them.
* **Automated:** `StaticMembershipTest.a_second_member_with_the_same_instance_id_fences_the_first`, and
  `StaticMembershipTest.a_fenced_members_commit_is_refused_and_changes_nothing`, read by `ci/b-66/run.sh`.

### Scenario: A mixed group under the KIP-848 protocol reads every record once
* **Given:** members with `group.protocol=consumer`: a JVM member, a native member, and a native third
  joining mid-stream, each committing only on revocation, while records are being written.
* **When:** each member arrives, and each leaves.
* **Then:** nothing is lost or processed twice against the broker, commits reach every end, and nobody gives
  up everything mid-stream. The broker lists the group as a consumer-protocol group. A lone member on each
  arm reads every record once, its listener hears `+[0] -[0]`, and explicit commits read back. The classic
  protocol's own keys are refused at construction with `IllegalArgumentException`. A member that names the
  broker's `range` assignor with `group.remote.assignor` runs it, as the broker's tool reads a live group, and
  an assignor the broker does not offer is refused at the first `poll` with `IllegalArgumentException`.
* **Automated:** `ConsumerProtocolTest` on both arms, and `CooperativeTest`'s member under the new protocol;
  run and counted by `ci/b-57/run.sh`; the assignor read by `ci/b-67/run.sh`.

### Scenario: A member whose session expires hears onLost and comes back from the group's commit
* **Given:** a group of one member on each arm, each committing only on revocation, while records are written.
* **When:** one member's process is frozen with `SIGSTOP` for longer than `session.timeout.ms`, and resumed,
  in both directions.
* **Then:** the broker removes it on heartbeat expiration, and the other member takes its partitions. On
  resuming, it hears `onLost` and then `onAssigned` on both arms, and reads from exactly the group's commit.
  Nothing is lost against the broker's end offsets, and the group's commits reach every end.
* **Automated:** `SessionExpiryTest`'s member, run, frozen and counted by `ci/b-65/run.sh`.

## 5. Out of scope

Making the `consumer` protocol the default: it stays the clients' own `classic`. `onLost` is mapped on both arms but not exercised.
