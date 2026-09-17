---
id: B-23
title: "The publish that returns before the acknowledgement — the shape RQ-A could not reach"
status: done
priority: P3
size: M
stage: stage-4-a-real-user
---

# B-23 — the publish that returns before the acknowledgement

[B-19](B-19-close-under-a-real-shutdown.md) produced twenty clean rounds and, in producing them,
showed what they do not cover. The publisher it used awaits the broker's acknowledgement **inside the
request**, so by the time `close` runs there is nothing outstanding for it to flush. What those
rounds exercised is the **order** — the drain finishes before the producer is closed, so a request
mid-`send` is not cut — and not the sentence's harder half, which is `close` answering for records
that are already in flight when the signal lands.

The contract sentence covers both, and a reader will assume the harder one
([research §2.14](../research/research-architecture.md)).

- **The decision and its reason.** Measure the other shape: a sink that hands the record to a bounded
  queue and returns, with a publisher coroutine calling `send`. That is what most people write on an
  ingress path, because nobody wants a webhook's `200` to wait for Kafka, and it is the only shape in
  which `close`'s flush has anything to do.
- **It is not simply "the same harness with a different sink".** A queue in front of `send` makes the
  gap between the row and the send wide enough to matter, which turns part of any loss into an
  **outbox** question — whether a service should record its intent and reconcile later. That question
  is real and it is not about this library; an item that measures the queued sink has to say, before
  it runs, which side of that line each loss falls on. Deciding it afterwards decides the result.
- The rejected alternative is widening B-19 to cover both. It was rejected while B-19 was being
  measured: the two shapes fail differently, and one harness reporting a single number for both would
  have made the failure it found unattributable.
- Not covered: changing the gateway's shipping sink. Its synchronous publish is a deliberate choice
  with its cost written down, and this item measures a second arm rather than replacing the first.

- AC: the same twenty rounds against a publisher whose `publish` returns before the acknowledgement,
  with every loss classified, before the run, as either "the producer was asked and lost it" or "the
  process stopped before the producer was asked".
- AC: a positive control that loses records, as in B-19 — and for this shape it has to be able to lose
  them **in the producer**, which the broker-stopped control of B-19 already does.
- AC: whichever colour comes out, it goes into `docs/research/` beside §2.14, including a red.
- **It also waits on a merge that is not this repository's to make.** The publisher whose sink this
  item adds a second arm to exists only on an open pull request — `youndie/xyk#5`, opened 2026-09-17
  for [B-19](B-19-close-under-a-real-shutdown.md), still unreviewed. Building the queued arm on that
  branch would stack unmerged work on unmerged work, and any number measured from it would be a
  number about code that review can still change. Recorded here after an iteration picked this item
  by the backlog's own rule and stopped: the rule sees `open` and no `blocked_by`, and neither of the
  two things actually holding it up is expressible in that field.
- **Out of M2's budget**, and filed rather than started: M2 bought three days and RQ-A spent them. It
  is written down because a limitation a measurement discovered about itself is exactly the thing that
  is otherwise remembered as a green.
- Anchors: `ci/b-23/run.sh`, `ci/b-19/run.sh`, `docs/research/research-architecture.md`,
  `docs/api/producer-contract.md`.

## What happened

**Green on the number the item is judged on, and the control is where the result is.**
`ci/b-23/run.sh`, 64-deep queue, 64 concurrent senders, on a machine restarted minutes earlier:

| | 20 rounds | the control, broker stopped before the signal |
|---|---|---|
| accepted | **17 644** (525–1 266 per round) | 1 259 |
| `producer` — asked of the producer and lost | **0** | **1** |
| `outbox` — accepted, queued, never asked | **0** | **127** |
| `refused` — the `send` threw | 0 | 1 |
| shutdown | 2.26 s, drain 12–40 ms | **15.07 s, DEADLINE_EXCEEDED** |

With the broker gone the drain stage ran to exactly **10.000 s** and the release stage to exactly
**3.000 s** — both their deadlines — kore cut them, and the 127 records still in the queue were never
handed to the producer. **Without the pre-registered split that control reads "129 records lost" and
the number lands on this library.** Two of them are about the producer; the other 127 are the outbox
question, which is not.

**The cost of the shape, visible without being asked.** The same harness at the same concurrency
accepted 17 644 events where [B-19](B-19-close-under-a-real-shutdown.md)'s synchronous arm accepted
130 681 — a single consumer in front of the producer serialises what was 64 concurrent `send`s, so
the service's own throughput falls about sevenfold. That is the trade a service makes when it decides
a webhook's `200` must not wait for Kafka, and it is a property of the shape rather than of the
library.

**The harness lied green twice before any of this was true, and neither time was caught by reading.**

1. **`rows=0` in every round, and therefore `missing=0` in every round.** `sqlite3` is not on `PATH`
   in a non-interactive shell on that machine, the per-round database read failed silently, and a
   reconciliation with an empty left-hand side comes out clean whatever happened. It was visible only
   because `extra=707` — records on the topic with no row behind them — is the one column an empty
   read cannot make zero. The harness now checks the tools it reads answers with **before** measuring
   anything, and a round whose database holds no accepted events is red with a sentence.
2. **The publisher's own drain test passed with the drain removed** (`youndie/xyk#7`): with an instant
   delegate, closing the channel let the queue finish before the assertion looked. Twenty records at
   twenty milliseconds cannot finish by accident; with the wait gone, two tests now fail.

The machine also dropped its ssh connections twice mid-run and was restarted; the run that produced
these numbers was detached from the session that started it, which is why it survived.

## Pre-registered, before any round ran

The item's first criterion asks for the classification to be fixed in advance, so it is written here
and in `ci/b-23/run.sh`'s header, and **the classifier is a line the service prints at the time**
rather than a reading taken afterwards. `QueuedEventSink` announces each event id immediately before
it hands the record to the producer; everything else follows from that line existing or not.

| column | what it means | whose question |
|---|---|---|
| `producer` | the process **asked** the producer for it — the log says so — and it never arrived, and nothing reported it refused | **kafkakn's.** The rounds are judged on this number |
| `outbox` | accepted, queued, and the process stopped before the producer was ever asked | **not this library's.** Whether a service should record its intent and reconcile later is a real question and a different one |
| `refused` | the `send` threw | neither: the contract says *acknowledged, or its `send` throws*, so this is a legitimate non-delivery in both arms |

**Why the split has to be pre-registered.** Both losses look identical from the outside — a row in
`events` with nothing on the topic behind it. A run that decided afterwards which pile each belonged
to could report an outbox defect as a producer defect, or the reverse, and either way the number
would be an opinion. The green condition is `producer = 0`; `outbox` is reported next to it and kept
out of the verdict on purpose.

**The publisher is the same one B-19 used**, with one arm added rather than replaced:
`XYK_KAFKA_QUEUE` is `0` in the shipping configuration, which is exactly the sink B-19 measured, and
above zero it is a measurement arm in the same sense as that service's `xyk.outbound=noop`. Its own
suite asserts the three properties a run through it depends on: `publish` returns while the delegate
is still working, `close` drains rather than discards, and every record is announced **before** the
delegate is asked — that last one is the classifier itself, and if it were announced afterwards a
record the process stopped in the middle of would look like one it had never reached.
