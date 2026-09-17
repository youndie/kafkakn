---
id: B-23
title: "The publish that returns before the acknowledgement — the shape RQ-A could not reach"
status: wip
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
