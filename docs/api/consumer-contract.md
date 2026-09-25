---
id: consumer-contract
title: The consumer contract — designed before it is built
type: api_endpoints
status: active
services:
  - kafkakn-core
contract_source:
  - kafkakn:kafkakn-core io.github.youndie.kafkakn.KafkaConsumer
---

# The consumer contract

**Assign, seek and poll are built and measured** ([B-36](../backlog/B-36-assign-and-poll.md),
2026-09-24), and so are groups ([B-37](../backlog/B-37-consumer-groups.md), 2026-09-25). This document began as [B-35](../backlog/B-35-the-consumer-designed-first.md):
the consumer designed before any of it is written, because the questions that decide its shape are
questions about the two clients underneath, and none of them is answered by writing a `poll` loop.
Every promise below is ***target*** until the item named beside it measures it; the facts it rests
on are marked **read**, with the address they were read at.

It is written in the shape of [producer-contract](producer-contract.md), and it inherits that
document's rules wholesale: bytes and not strings, a key neither client honours fails at
construction, TLS and SASL are spelled and translated as there, and a platform key is refused by
the other arm.

## 1. Threading: what each client demands of the thread that calls it

This is the decision every later consumer item inherits, and the producer's seam had to learn its
half of it twice ([research §2.3, §2.13](../research/research-architecture.md)).

| | kafka-clients 4.3.1 | librdkafka 2.13.0 |
|---|---|---|
| thread safety | **not thread-safe** — *"Un-synchronized access will result in `ConcurrentModificationException`"* | *"completely thread-safe … may call any of the API functions from any of its own threads at any time"* |
| what is actually checked | a light lock held **for the duration of a call**: `acquire()` records the calling thread's id, `release()` clears it when the call returns. **Sequential calls from different threads pass; overlapping calls throw.** | nothing — no per-thread state |
| interrupting a waiting call | `wakeup()`, the one method safe from another thread; the waiting call throws `WakeupException`. Thread interrupts work but are *discouraged* — they *"may cause a clean shutdown of the consumer to be aborted"* | a zero-timeout poll never waits, so there is nothing to interrupt |
| rebalance callbacks run | on the thread calling `poll` — *"rebalances will only occur during an active call to `poll`, so callbacks will also only be invoked during that time"* | queued callbacks *"will also be triggered by … `rd_kafka_consumer_poll()`"*, on its caller's thread; that `rebalance_cb` is among them is the documented example's usage, not a sentence — [B-37](../backlog/B-37-consumer-groups.md) confirms it |
| must come back within | `max.poll.interval.ms` (300 000) between polls, or the member leaves the group | the same key, the same default, the same consequence |

**Read at:** `apache/kafka@4.3.1!/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java`
(the "Multi-threaded Processing" section), `…/consumer/internals/ClassicKafkaConsumer.java` and
`…/consumer/internals/AsyncKafkaConsumer.java` (`acquire`, `release` — the same check in both);
`librdkafka-2.13.0.tar.gz!/INTRODUCTION.md` ("Threads and callbacks", and the offset store: *"updated by
`consumer_poll()` … to store the offset of the last message passed to the application"*).

**The decision — built and measured in [B-36](../backlog/B-36-assign-and-poll.md).**

- **JVM arm:** every call to the Java consumer runs on a lane of its own —
  `Dispatchers.IO.limitedParallelism(1)` per consumer. One lane means no two calls ever overlap, which
  is exactly what `acquire()` checks, and `IO` means a `poll` that waits holds a thread that exists
  for waiting rather than the caller's. It is **not** a single thread, and it does not need to be: the
  lock is per call, not per thread — read, not assumed.
- **Cancelling a waiting `poll` calls `wakeup()`**, from the cancelling side, and the `WakeupException`
  that follows is the cancellation, not a failure. Interrupting the lane's thread
  (`runInterruptible`) is the rejected alternative: the Java client's own documentation discourages
  it because it can abort a clean shutdown.
- **Native arm:** `rd_kafka_consumer_poll(rk, 0)` in a loop with `delay` between empty polls — the
  producer's delivery-report pump, again. Nothing waits inside C, so cancellation is immediate and no
  thread is held.
- **Measured, both arms**: eight coroutines polling one consumer at once read every record exactly
  once and nothing threw; with the JVM lane replaced by plain `Dispatchers.IO`, the Java client threw
  *"KafkaConsumer is not safe for multi-threaded access"* — the check read above, seen. A cancelled
  `poll` returned in 16 ms (JVM) and 0 ms (native), and the next call was served at once; with the
  `wakeup()` removed the caller was still released at once — and the next call waited 55 s behind the
  abandoned `poll`, which is why the test times the next call and not the cancellation. A waiting
  `poll` held a single-lane caller for under 500 ms on both arms; the native arm made to block in
  `rd_kafka_consumer_poll(timeout)` held it for 3.0 s.
- **Neither arm hides `max.poll.interval.ms`.** A caller who takes longer than that between polls is
  removed from the group on both arms. That is Kafka's rule rather than a platform's, so the contract
  states it instead of working around it — and it is the reason for §2.

## 2. Shape: an explicit `poll` first, a `Flow` built on it later

```kotlin
interface KafkaConsumer {                                   // B-36, B-37
    suspend fun assign(partitions: List<TopicPartition>)   // B-36
    suspend fun subscribe(topics: List<String>)             // B-37
    suspend fun commit()                                    // B-37
    suspend fun commit(offsets: Map<TopicPartition, Long>)  // B-48: each value is the next offset to read
    suspend fun assignment(): List<TopicPartition>          // B-37
    suspend fun position(partition: TopicPartition): Long   // B-49: the next offset poll returns
    suspend fun committed(partitions: List<TopicPartition>): Map<TopicPartition, Long?>   // B-49
    suspend fun pause(partitions: List<TopicPartition>)     // B-52
    suspend fun resume(partitions: List<TopicPartition>)    // B-52
    suspend fun paused(): List<TopicPartition>              // B-52
    suspend fun metrics(): ConsumerMetrics                  // B-53: lag per held partition
    suspend fun groupMetadata(): ConsumerGroupMetadata      // B-38
    suspend fun seek(partition: TopicPartition, to: SeekTo) // B-36: beginning, end, offset, timestamp
    suspend fun poll(timeout: Duration): List<ConsumerRecord>
    suspend fun close()
}

class ConsumerRecord(                                       // B-36
    val topic: String,
    val partition: Int,
    val offset: Long,
    val timestamp: Long,
    val key: ByteArray?,
    val value: ByteArray?,          // null: a tombstone written by somebody else
    val headers: List<Header>,      // ordered, duplicates kept — as the producer sends them
)
```

- **`poll` returns what is available, up to a bound, or an empty list when `timeout` passes.** The
  JVM bounds a batch by `max.poll.records` (500); librdkafka returns one message per call, so the
  native arm drains up to the same 500 per `poll`. `max.poll.records` itself is a JVM key, refused on
  native by the configuration rule.
- **`value` is nullable**: a null value is a tombstone, whoever wrote it. `ProducerRecord.value` has
  been nullable too since [B-47](../backlog/B-47-a-producer-can-write-a-tombstone.md); until then this
  library could read a tombstone and not write one.
- **The `Flow` is built, as the extension promised here** ([B-54](../backlog/B-54-a-flow-over-poll.md)):
  `KafkaConsumer.records(pollTimeout): Flow<ConsumerRecord>`, in common code, cold, one collector at a
  time, and it never closes the consumer. *Measured* (`ci/b-54/run.sh`):
  - the twenty fixture records are read in order on both arms;
  - a cancelled collector stops within milliseconds, and the consumer is usable afterwards.

  **A slow collector's eviction, measured.** A collector that stalls longer than `max.poll.interval.ms`
  (10 s against 6 s):
  - it keeps receiving the rest of the batch already in hand, from a partition it no longer holds;
  - both arms report the partition to the listener as lost (`onLost`);
  - the next `poll` rejoins: the listener sees the partition assigned again, and with nothing committed
    the records come again from the start.

  Until [B-64](../backlog/B-64-native-poll-throws-where-the-jvm-rejoins.md) the arms parted at that `poll`:
  native threw `KafkaConsumeException: … Maximum application poll interval (max.poll.interval.ms)
  exceeded`, because it treated librdkafka's `__MAX_POLL_EXCEEDED` event as a failure. It now skips the
  event, as it skips end-of-partition, and rejoins as the reference does. Measured: `+[0] ![0] +[0] -[0]`
  and the same offsets on both arms.
- **The `Flow` came later, as an extension over `poll`, not instead of it.** A cold `Flow` makes the
  collector's pace the poll's pace, and a collector slower than `max.poll.interval.ms` is evicted from
  its group — on both arms, with nothing in the `Flow`'s signature to say so. With an explicit
  `poll`, the time between calls is visible in the caller's code. The rejected alternative is the
  `Flow` as the first and only shape.
- **Groups** ([B-37](../backlog/B-37-consumer-groups.md)): `subscribe(topics)`, `commit()` and
  `assignment()`. No rebalance callback is offered and none is installed: with auto-commit off, each
  client's own handling is the promise — a partition handed over resumes from its last commit.
  `commit()` is synchronous and commits the position after everything `poll` returned, for every
  partition held: `commitSync()` on the JVM, `rd_kafka_commit(rk, NULL, sync)` on native, whose stored
  offsets are exactly that.
- **`commit(offsets)` commits exactly the offsets named** ([B-48](../backlog/B-48-commit-explicit-offsets.md)),
  each the next offset to read, as `sendOffsetsToTransaction` takes it: `commitSync(Map)` on the JVM,
  `rd_kafka_commit(rk, list, sync)` on native, whose per-partition errors are read as well as the call's.
  An empty map commits nothing, and a negative offset is refused in common code before either client
  sees it. *Measured* (`ci/b-48/run.sh`): each arm commits offset 7 in the middle of a batch,
  `kafka-consumer-groups.sh` shows 7, and a new member of the group reads from 7 on.

  A commit of a partition the consumer does not hold is **accepted by both arms**, and the broker stores
  it (measured with `assign`, so the group has no generation to check against). The two clients agree,
  so kafkakn passes it through rather than refusing it. Under a subscription, the broker may judge such a
  commit against the member's generation. That is not measured yet; it belongs with the rebalance
  listener ([B-50](../backlog/B-50-a-rebalance-listener.md)).
- **`position` is a number on both arms, before any record too** ([B-49](../backlog/B-49-committed-and-position.md)).
  The Java client answers `position` from wherever it must. librdkafka does not: `rdkafka.h` says
  `rd_kafka_position` gives *"the offset of the last consumed message + 1, or RD_KAFKA_OFFSET_INVALID in
  case there was no previous message"*. So the native arm answers the way the Java client does:
  - with `assign`: from its own map, which consumption and seeks both write, with a seek to the beginning
    or the end resolved through `rd_kafka_query_watermark_offsets`;
  - in a group: from librdkafka's position once a record was consumed;
  - otherwise from the group's commit (`rd_kafka_committed`), and otherwise from `auto.offset.reset`.

  *Measured* (`ci/b-49/run.sh`): the arms agree on all eight readings. After seeks to the beginning, the
  end and offset 7 they read 0, 20 and 7. After reading everything, 20; after seeking back, 7. A fresh
  member of a group that committed 7 reads 7, and a new group with `earliest` reads 0. A partition not
  assigned is refused with `IllegalStateException` on both arms.
- **`pause` is the consumer's backpressure** ([B-52](../backlog/B-52-pause-and-resume.md)): keep calling
  `poll`, which returns the other partitions' records and keeps the member in its group, while a paused
  partition returns nothing. The item's trap was records a client had already fetched for a partition
  when it was paused. **Neither arm returns them.** *Measured* (`ci/b-52/run.sh`), on a topic of the
  test's own:
  - partition 0 paused with more than one batch of it still to come, and polled past a 6 s
    `max.poll.interval.ms`: 0 records from it, records from partition 1, and the member still holds both
    and can commit;
  - after `resume`, all 2000 of partition 0's records exactly once, the number the broker holds.

  The native arm keeps the paused set itself, because librdkafka has no call that lists it. A rebalance
  that takes a partition away forgets its pause, as the Java client does. A seek keeps it, as the Java
  client does, although the native arm's `assign`-mode seek re-assigns and librdkafka starts an
  assignment unpaused. Measured: `PauseTest.a_seek_does_not_undo_a_pause`.
- **Lag is the end of the log minus the position** ([B-53](../backlog/B-53-consumer-lag-in-metrics.md)): what
  this consumer has not read yet, per held partition, null until the client knows it.
  - JVM: `currentLag`.
  - Native: `consumer_lag_stored` from librdkafka's statistics, which kafkakn turns on once a second. Not
    `consumer_lag`, which `STATISTICS.md` measures from the *committed* offset: a different quantity
    from the Java client's.
  - `kafka-consumer-groups.sh` measures from the commit, so the three agree once the position is
    committed.

  The arms sample at different moments: on each fetch, and once per statistics interval, delivered
  inside `poll`. So the contract promises agreement for a lag that is not moving. *Measured*
  (`ci/b-53/run.sh`):
  - a 2000-record partition, read to 500 and paused: 1500 on both arms before the commit and after it,
    the broker's LAG 1500 too;
  - after reading everything: 0 on both arms.

  No success count is exposed, and `scripts/no_delivery_counters.py` still passes with its self-test.
- **`committed` returns null where nothing is committed**, never −1: both clients use −1 internally, and a
  caller could read it as an offset. What each arm returns is what `kafka-consumer-groups.sh` shows.
- **`subscribe` and `commit` need a `group.id` the caller named**, on both arms. The native arm's
  private `kafkakn-assign-*` id exists only so librdkafka will `assign`; a subscription joining it, or a
  commit landing in it, would be a group nobody asked for.
- **A seek in a group is allowed for the partitions the group gave this member**, and refused with
  `IllegalStateException` for any other, on both arms ([B-51](../backlog/B-51-seek-under-a-subscription.md)).
  Until B-51 every seek under a subscription was refused, because the native arm seeks by re-assigning,
  and in a group the assignment is not the member's to make. In a group the native arm now seeks with
  `rd_kafka_seek_partitions`, and `position` answers the seek until a record is read. To seek a partition
  as the group hands it over, use the listener's scope (§2a). *Measured* (`ci/b-51/run.sh`), with the arms
  agreeing:
  - a member that read the whole fixture and sought to 7 reads position 7, and its next record is 7;
  - a seek from `onAssigned` to 12 makes 12 the first record the member is given;
  - a seek before the group has assigned anything is still refused.

### At-least-once, measured for loss

**Measured 2026-09-25, `ci/b-37/run.sh`**, with records written by a third party *while* the group
formed:

- a group of two members on each arm split four partitions ([0,1] and [2,3]), the leaving member
  left the way a crash would — one batch read and never committed — and the staying one ended with all
  four: 800 of 800 records seen, the abandoned record delivered again, commits at the log end for
  every partition, read by `kafka-consumer-groups.sh --describe`;
- **one group with a member on each arm**, in two processes at once: it formed, split ([0,1] to the
  JVM member, [2,3] to the native one), and when the native member left without committing its last
  batch the JVM member was given that batch and all four partitions: 600 of 600, commits at the log end;
- the check is for loss, and it is not blind: the abandoned batch is **not** counted as seen by the
  member that dropped it, so only redelivery can cover it. With the native `close()` made to commit
  first — a plausible "commit on close" — one record was lost in the native group and one in the mixed
  one, and the run went red.

The two arms' default assignment strategies differ (§3) and share `range`; the split observed is
range's shape. That is a reading of the shape, not a reading of the broker's own record of the
strategy.

### Two things librdkafka needs that the design did not know

Found by B-36, and each is the native arm doing more so that both arms mean the same.

- **`rd_kafka_assign` needs a `group.id`.** Without one it answers *"Local: Unknown group"*; the Java
  client assigns without. A caller who names no group gets a private one on the native arm,
  `kafkakn-assign-<random>`, which joins nothing and commits nothing — `kafka-consumer-groups.sh --list`
  shows no such group after the run.
- **A seek before fetching has started is refused**: *"Local: Erroneous state"*, as `rdkafka.h`
  warns — a seek is for partitions *"already assigned/consumed"*. The Java client seeks at any moment
  after `assign`. So the native arm seeks by **re-assigning**, with the partition moved and every other
  one at the next offset this side has handed out.

## 2a. Rebalances: a listener that runs inside `poll`

**Built and measured**, [B-50](../backlog/B-50-a-rebalance-listener.md). This section was written, and put
to the owner, before any of the code; the owner chose this shape over suspending callbacks and events
returned from `poll`. What was measured is at the end of the section.

**What the two clients do, read rather than assumed.**

| | kafka-clients 4.3.1 | librdkafka 2.13.0 |
|---|---|---|
| the hook | `subscribe(topics, ConsumerRebalanceListener)`: `onPartitionsRevoked`, `onPartitionsAssigned`, and `onPartitionsLost`, whose default calls `onPartitionsRevoked` (`javap -c`) | `rd_kafka_conf_set_rebalance_cb`: one callback, told `ASSIGN` or `REVOKE`, and it **must** apply the change itself (`rd_kafka_assign`, or `rd_kafka_incremental_assign` under cooperative) |
| where it runs | inside `poll`, on the thread calling it (§1) | inside `rd_kafka_consumer_poll`, on its caller's thread (§1). **That is our polling coroutine's thread, not one of librdkafka's.** |
| lost versus revoked | a separate callback, `onPartitionsLost` | `rd_kafka_assignment_lost(rk)`, read inside a `REVOKE` |
| may commit inside | yes: `commitSync` on the polling thread, which the listener is already on | yes: `rd_kafka_commit`, synchronous |

**The constraint that decides the shape.** The callback fires while `poll` is running, so the consumer
is held: by the lane on the JVM, by the lock on native. A listener that calls the kafkakn consumer's own
suspending methods (`commit`, `seek`) would wait for the `poll` that is waiting for it: a deadlock on
both arms. Whatever the listener may do inside the callback has to be done directly on the client, by
the call that is already running.

**The design.**

```kotlin
suspend fun subscribe(topics: List<String>, listener: RebalanceListener)   // B-50

interface RebalanceListener {
    /** Called inside poll, before these partitions leave this member. Commit what was processed here. */
    fun onRevoked(partitions: List<TopicPartition>, scope: RebalanceScope) {}
    /** Called inside poll, after these partitions arrived, before any of their records is returned. */
    fun onAssigned(partitions: List<TopicPartition>) {}
    /** The same moment, with the scope: seek what arrived. Both arms call this one; it defaults to the above. B-51 */
    fun onAssigned(partitions: List<TopicPartition>, scope: RebalanceScope) { onAssigned(partitions) }
    /** Called instead of onRevoked when the member was removed from the group: a commit would be refused. */
    fun onLost(partitions: List<TopicPartition>) {}
}

interface RebalanceScope {
    /** Commits synchronously, directly on the client, inside the callback. B-48's meaning. */
    fun commit(offsets: Map<TopicPartition, Long>)
    /** From onAssigned only: where an arriving partition is read from. Refused from onRevoked. B-51 */
    fun seek(partition: TopicPartition, to: SeekTo)
}
```

**Amended by B-51: the scope seeks as well as commits, and `onAssigned` is given it.** The rule is the same
one: the scope is the only way back into the client from a callback. A seek right after an assignment is
refused by librdkafka (*"Erroneous state"*, measured in B-36). So on native, `onAssigned` runs just
**before** `rd_kafka_assign`, and a seek made in it becomes that partition's starting offset in the list
assigned. No record can be fetched in between, so what the contract promises is unchanged: the seek takes
effect before the partition's first record. On the JVM it is `seek` on the lane's thread, inside `poll`.

- **Plain functions, not `suspend`.** They run inside `poll`, on the thread that polls, and they must
  return before the rebalance can finish, within the group's rebalance timeout. A suspending callback
  would have to be driven by `runBlocking` inside the client's callback. That hides the same wait
  behind a signature that promises it does not block.
- **`RebalanceScope` is the only door back into the client.** It commits on the JVM with `commitSync`
  on the lane's current thread, and on native with `rd_kafka_commit`, both inside the running call. The
  consumer itself must not be called from a callback. Doing so throws, on both arms, rather than
  deadlocking.
- **Lost is not revoked.** `onLost` has no scope, because a member that was removed can no longer
  commit. On native it is the `REVOKE` for which `rd_kafka_assignment_lost` is true. On the JVM it is
  `onPartitionsLost`, overridden so that it is not routed to `onRevoked`.
- **Order, under the eager protocol both arms use by default:** every partition held is revoked, then
  the new set is assigned. `onRevoked` sees what is about to go, `onAssigned` what arrived.
- **Under cooperative rebalancing ([B-55](../backlog/B-55-cooperative-rebalancing.md)), `onRevoked` sees
  only what moves.** A member keeps reading the partitions it keeps, and `onAssigned` sees only what it
  gained. Native's callback switches to `rd_kafka_incremental_assign`/`_unassign` when
  `rd_kafka_rebalance_protocol` says `COOPERATIVE`; librdkafka refuses a plain `assign` under that protocol
  ("must be made using incremental_assign()", measured). *Measured* in a group with a member on each arm
  and a third joining 30 s later (`ci/b-55/run.sh`):
  - B joins: the JVM member gives up `[3,4,5]`, keeps `[0,1,2]`, and the native member B receives
    `[3,4,5]`;
  - C joins: the JVM member gives up `[2]` and B gives up `[5]`, and the third member C receives `[2,5]`;
  - over 1200 records, nothing is lost or processed twice.
- **Native applies the assignment itself.** Once a `rebalance_cb` is set, librdkafka no longer assigns
  on its own: the callback calls `rd_kafka_assign` with the new list, or with `NULL` on a revoke, after
  the listener has returned. Forgetting it leaves the member holding nothing, silently.

**Measured (`ci/b-50/run.sh`, and `RebalanceListenerTest` on both arms).**
- **The handover.** A group with one member on each arm, in both directions. Each member commits
  **only** in `onRevoked`, and the leaver leaves after processing 50 records: 1200 records, 0 lost,
  0 processed twice. The group's commits reach every partition's end. The eager sequence is visible in
  the stayer's events: `+[0,1,2,3] -[0,1,2,3] +[0,1] -[0,1] +[0,1,2,3] -[0,1,2,3]`.
- **The scope is what makes it hold.** With either arm's scope made to commit nothing, the same run
  finds 50 to 93 records processed twice. When the mutated arm is the leaver, exactly its 50.
- **Re-entry is refused, not deadlocked.** On both arms, a call to the consumer from `onAssigned` throws
  `IllegalStateException` with this section's reasoning. With the guard removed, the call deadlocks by
  suspending, on the JVM's lane and on native's lock. An unbounded test then hung the run, measured
  twice, so the test bounds the call and fails by name.
- **No empty lists.** A lone member of a one-partition topic sees exactly `+[0]` and, on `close`,
  `-[0]`, on both arms.
- **Not measured:** `onLost`. It needs a member removed for missing `session.timeout.ms`, which no run
  here arranges yet. The mapping (`onPartitionsLost` overridden; `rd_kafka_assignment_lost` inside
  `REVOKE`) is read from each client, not observed.

### Static membership ([B-56](../backlog/B-56-static-membership.md))

`group.instance.id` travels to both clients unchanged, and what it promises is now measured.
- **A static member closed and reopened within `session.timeout.ms` gets its partitions back, and the group
  does not rebalance.** The broker hands the returning instance its old assignment under a new member id.
  The other member's listener hears nothing in the window, and neither does the broker's log: no
  "Preparing to rebalance", and no new generation. The restarted member's listener is told `onAssigned` with
  the same partitions. This was measured on each arm and in a mixed group both ways, with a JVM stayer and
  a native restarter and the reverse. The positive control is the same restart without the key, which the
  other member hears as a leave and a join.
- **The trap: `close` on a static member does not leave the group.** That is by design, in both clients. Its
  partitions stay with the absent instance until it returns or `session.timeout.ms` passes. Nobody reads
  them in between. The member's own listener still hears `onRevoked` for what it held when it closes, on
  both arms, even though the group heard nothing.
- **A second member with the same `group.instance.id` fences the first.** The broker keeps the newer member,
  and the older one's next `poll` throws **`ConsumerFencedException`, on both arms**. Nothing is left to do
  with it but `close`, which both arms then complete without an error.
  - The Java client throws `FencedInstanceIdException`, kept as the cause.
  - librdkafka hands `poll` a fatal error, `_FATAL`, whose reason `rd_kafka_fatal_error` gives as
    `FENCED_INSTANCE_ID`. It used to surface as *"Local: Fatal error"*, which named nothing. The native arm
    now asks for the reason and keeps librdkafka's sentence in the message.
  - Measured for `poll` only. A commit made by a fenced member is not.
- **The generation is read from the broker's log, not its tool.** `kafka-consumer-groups.sh --describe`
  prints `GROUP-EPOCH` as `-` for a classic group, so it cannot show a generation that did or did not change.
  The broker's "Stabilized group G generation N" lines can.

### The KIP-848 protocol, `group.protocol=consumer` ([B-57](../backlog/B-57-the-kip-848-consumer-protocol.md))

A supported value on both arms, and not the default: the default stays the clients' own, `classic`, until
both clients change it. The test broker, `apache/kafka:4.3.1`, accepts it with no configuration change.
- **The broker assigns.** `kafka-groups.sh --list` names such a group TYPE `Consumer`, PROTOCOL `consumer`, and
  its assignor is the broker's (`uniform` on the fixture). The runner checks the type, so a member that
  quietly fell back to `classic` would not pass.
- **Three classic keys are refused at construction with `IllegalArgumentException`, on both arms:**
  `partition.assignment.strategy`, `session.timeout.ms` and `heartbeat.interval.ms`. Under the new
  protocol the broker owns them: `group.remote.assignor` names the assignor, and the broker's
  `group.consumer.*` settings are the timeouts. Both clients refuse them anyway, but in different types
  (the Java client's `ConfigException`, a failed `rd_kafka_new`), so kafkakn refuses first, in one type.
  Under `classic` the same keys are still the client's to judge.
- **The promises of §2 and §2a hold, measured on both arms and in a mixed group:**
  - a lone member reads every record once;
  - its listener hears `+[0]` and, on `close`, `-[0]`;
  - an explicit commit reads back;
  - a commit made in `onRevoked` at `close` is what the broker then holds.

  In a group of a JVM member and a native member, with a native third joining mid-stream, each committing
  only on revocation: nothing is lost or processed twice across 1 200 records, the commits reach every end,
  and nobody gives up everything mid-stream. Assignments arrive in pieces: the late member heard `+[2, 5]`
  and then `+[0, 1, 3, 4]` as the others left. The listener must not assume one `onAssigned` per rebalance.
- **Native: why it works.** librdkafka reports the new protocol as `COOPERATIVE`, and the rebalance callback
  already switches to incremental assign for that (B-55). With that switch forced off, the lone member fails.

**Rejected alternatives.**
- *Suspending callbacks.* See above: a blocking wait wearing a `suspend` signature, and a deadlock the
  moment the listener calls the consumer.
- *Rebalances as events returned from `poll`.* It is the cleanest API, but it cannot support the reason
  for the item: by the time a caller reads "revoked", the partitions are gone, and a commit made then
  lands after another member took over.

## 3. Defaults: every key this contract names, read from both artefacts

**Read at:** `ConsumerConfig.configDef().defaultValues()` executed against `kafka-clients-4.3.1.jar`
on 2026-09-24, and `librdkafka-2.13.0.tar.gz!/CONFIGURATION.md`. Five of these rows were in
[research §1.8](../research/research-architecture.md); the rest are new here.

| Key | kafka-clients 4.3.1 | librdkafka 2.13.0 | kafkakn (*target*) | Why |
|---|---|---|---|---|
| `isolation.level` | `read_uncommitted` | `read_committed` | **`read_committed`, both arms** | see below |
| `enable.auto.commit` | `true` | `true` | **`false`, both arms** | same value, different meaning — see below |
| `allow.auto.create.topics` | `true` | `false` | **`false`, both arms** | reading a topic should not create it; the fixture refuses auto-creation for the same reason |
| `check.crcs` | `true` | `false` | **`true`, both arms** | corruption surfaces as an error instead of as bytes |
| `auto.offset.reset` | `latest` | `largest` | `latest`, travels | the same meaning; librdkafka accepts `earliest` and `latest` too, so those two spellings travel and the rest (`none`, `error`, `smallest`) are platform values |
| `partition.assignment.strategy` | `RangeAssignor`, `CooperativeStickyAssignor` | `range,roundrobin` | unset: each arm's own; set: **portable since [B-55](../backlog/B-55-cooperative-rebalancing.md)** | librdkafka's words, `range`, `roundrobin` and `cooperative-sticky`, which the JVM arm translates into class names. A Java class name, `sticky`, or cooperative next to an eager assignor is refused at construction on both arms. Unset, the two defaults share `range`, which is what a mixed group settles on ([B-37](../backlog/B-37-consumer-groups.md)) |
| `group.protocol` | `classic` | `classic` | `classic`, travels | `consumer` (KIP-848) is supported and measured since [B-57](../backlog/B-57-the-kip-848-consumer-protocol.md): see §2a. Not the default until both clients make it theirs |
| `group.id` | none | none | travels; required by `subscribe` and `commit` | |
| `group.instance.id` | none | none | travels | static membership, measured in [B-56](../backlog/B-56-static-membership.md): see §2a |
| `max.poll.interval.ms` | 300 000 | 300 000 | travels | §1 |
| `session.timeout.ms` | 45 000 | 45 000 | travels | |
| `heartbeat.interval.ms` | 3 000 | 3 000 | travels | |
| `fetch.min.bytes` | 1 | 1 | travels | |
| `max.partition.fetch.bytes` | 1 048 576 | 1 048 576 (alias of `fetch.message.max.bytes`) | travels | |
| `fetch.max.bytes` | 52 428 800 | 52 428 800 | travels | |
| `fetch.max.wait.ms` | 500 | *absent* — librdkafka calls it `fetch.wait.max.ms`, also 500 | **platform key on each side** | two names, one meaning; not translated until a caller needs it |
| `max.poll.records` | 500 | *absent* | JVM platform key; the native arm's per-`poll` bound is the same 500 | §2 |
| `enable.auto.offset.store` | *absent* | `true` | native platform key, left at `true` | with auto-commit off it only feeds `commit()`, which then commits what `poll` returned — the JVM's `commitSync()` semantics |
| `enable.partition.eof` | *absent* | `false` | native platform key, left at `false` | |
| `client.id` | `""` | `rdkafka` | each arm's own | cosmetic |

**`isolation.level`: the safe default, not the reference arm's.** B-25 gave both arms the Java client's
idempotence default because it was the one that writes nothing twice. The same rule here picks
librdkafka's: under `read_uncommitted` a consumer returns records from transactions that were
aborted — records that, as far as the producer's caller is concerned, never happened — and since
[B-30](../backlog/B-30-transactions.md) this library's own producer writes them. So the rule is
"the default that shows a caller nothing they did not ask for", and it lands on whichever arm has it.
The JVM arm sets `read_committed` unless the caller wrote the key.

**`enable.auto.commit`: one value, two meanings, so neither travels.** The Java consumer commits inside
`poll` and `close`, and the offsets it commits are those of the batch the previous `poll` returned —
at-least-once, *"but the requirement is that you must consume all data returned from each call to
`poll`"* before the next one. librdkafka commits in the background every `auto.commit.interval.ms`,
and what it commits is the offset store, which `enable.auto.offset.store` fills **as each message is
handed to the application** — before it is processed. A crash between the two commits a record that
was never processed on one arm and not on the other. The first consumer therefore commits only when
told to ([B-37](../backlog/B-37-consumer-groups.md)); a caller who turns auto-commit on gets each
client's own semantics, and the contract says which.

## 4. The oracle

- **Records are written by a third party, not by this library**, and include what a text-shaped path
  would damage: non-UTF-8 values, null keys, null values, duplicate header names, empty next to null.
  **Not `kafka-console-producer`**, which this section first named: the console tools carry text, and a
  value that is not UTF-8 comes back from them as U+FFFD. The third party is the Kafka distribution's
  own client, run as a program of its own (`ci/harness/Records.java`) that writes the records and reads
  them back as hex; kafkakn is not on its classpath. Measured, B-36: both arms read all twenty byte for
  byte as it does, and as each other.
  A consumer checked against our own producer can be wrong in the same way as it and agree with
  itself — the reverse of the rule the producer lives by.
- **Both arms read the same partitions, and their readings are compared with each other and with
  `kafka-console-consumer`**, byte for byte and in order. A difference between the arms is a failure,
  not a warning.
- **Positions and committed offsets are read by `kafka-get-offsets.sh` and
  `kafka-consumer-groups.sh --describe`**, never by the consumer that moved or committed them.
- **Isolation is named in every read** — the lesson of [B-30](../backlog/B-30-transactions.md), where
  an unnamed level counted aborted records.

## 5. What the first consumer will not do

- **Rebalance callbacks**, and a seek under a subscription (§2). Both have since been built: B-50 and B-51.
- **Auto-commit by default**, on either arm (§3).
- **A `Flow` as the primary shape** (§2).
- **The `consumer` group protocol** (KIP-848), static membership, cooperative rebalancing chosen on
  the caller's behalf. Cooperative rebalancing is now the caller's choice (B-55); it is still not
  chosen for them. Static membership has since been measured (B-56); it works through the key the caller
  sets. The `consumer` protocol is supported since B-57, and not chosen for the caller either.
- **Exactly-once as a built-in loop.** `groupMetadata()` and the producer's `sendOffsetsToTransaction` are
  in since [B-38](../backlog/B-38-exactly-once-read-process-write.md); the read-process-write loop
  around them is the caller's.
- **Deserializers.** Bytes in, bytes out, as for the producer.
- **Pause and resume, per-partition flow control, incremental fetch tuning.** Pause and resume have since
  been built: B-52. Fetch tuning has not.
- **Metrics** — [B-41](../backlog/B-41-metrics-an-operator-can-read.md), for both clients at once. The
  consumer's lag has since been built: B-53.
- **More than one call in flight on one consumer.** Calls are serialised by the lane (§1); a caller
  who wants parallelism runs more consumers.

**Amended 2026-09-25.** At the owner's request, most of this list is now planned, each as its own item
with a differential test:
- the rebalance listener ([B-50](../backlog/B-50-a-rebalance-listener.md)), designed in this contract
  before it is built;
- a seek under a subscription ([B-51](../backlog/B-51-seek-under-a-subscription.md));
- pause and resume ([B-52](../backlog/B-52-pause-and-resume.md));
- consumer lag ([B-53](../backlog/B-53-consumer-lag-in-metrics.md));
- the `Flow` over `poll` ([B-54](../backlog/B-54-a-flow-over-poll.md));
- cooperative rebalancing, static membership and KIP-848 ([B-55](../backlog/B-55-cooperative-rebalancing.md)
  to [B-57](../backlog/B-57-the-kip-848-consumer-protocol.md));
- explicit commits and reading positions back ([B-48](../backlog/B-48-commit-explicit-offsets.md),
  [B-49](../backlog/B-49-committed-and-position.md)).

What stays out: auto-commit by default, exactly-once as a built-in loop, deserializers, and more than
one call in flight on one consumer. The list above is kept as it was written, because the reason each
entry was first left out is what its item has to answer.

## 6. Code anchors

Nothing of the consumer exists yet, so the anchors are what it will be built from: the artefacts the
facts above were read in, and the producer code whose rules it inherits.

| What | Where |
|---|---|
| the Java consumer's thread check | `apache/kafka@4.3.1!/clients/src/main/java/org/apache/kafka/clients/consumer/internals/ClassicKafkaConsumer.java` |
| the same check in the KIP-848 consumer | `apache/kafka@4.3.1!/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AsyncKafkaConsumer.java` |
| librdkafka's threading and consumer keys | `librdkafka-2.13.0.tar.gz!/INTRODUCTION.md`, `librdkafka-2.13.0.tar.gz!/CONFIGURATION.md` |
| the configuration rules a consumer inherits | `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/TlsKeys.kt`, `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/SaslKeys.kt` |
| the TLS and SASL translations on the JVM arm | `kafkakn-core/src/jvmMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.jvm.kt` |
| the pump the native `poll` repeats | `kafkakn-core/src/nativeMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.native.kt` |
| the fixture and its third-party tools | `ci/harness/broker.sh` |
