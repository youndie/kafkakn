---
id: B-17
title: "`ci/downstream` is the wrong word in a Kafka repository"
status: done
priority: P2
size: XS
stage: stage-3-usable-by-others
---

# B-17 — `ci/downstream` is the wrong word in a Kafka repository

To anyone arriving for Kafka, **consumer** means a Kafka consumer — the one thing this library
deliberately does not have ([D2](../research/research-architecture.md)). The directory it names is a
downstream build that resolves the published artefact and links a binary
([B-13](B-13-external-downstream-acceptance.md)).

The word is in the directory, in `ci/b-13/run.sh`, in the README, in three documents and in the
item that created it. A reader who opens `ci/downstream/` expecting the thing the README says does not
exist has been told something wrong by the layout itself.

- **The decision and its reason.** `ci/downstream`, because that is what it is: a build downstream of
  the publication. It also survives the day a real consumer exists, which `ci/downstream` would not.
- The rejected alternative is leaving it and explaining in the README. A name that needs a footnote
  in the README is a name that will be misread by everyone who does not reach the footnote.
- Not covered: renaming `ci/publish/downstream`, the smaller probe inside the publication proof — the
  same argument applies and it moves in the same change.

- AC: no path or document in the repository calls a downstream build a consumer, checked by grep.
- AC: `ci/b-13/run.sh` and `ci/publish/run.sh` both still pass after the rename — a rename that
  breaks the acceptance is how the acceptance stops being run.
- Anchors: `ci/downstream/`, `ci/publish/downstream/`, `ci/b-13/run.sh`, `README.md`.

## What happened

`ci/consumer` → `ci/downstream`, `ci/publish/consumer` → `ci/publish/downstream`, and with them the
project name (`kafkakn-downstream`), the Gradle home the scripts purge, and
`docs/backlog/B-13-external-consumer-acceptance.md`, whose own filename called the thing a consumer.

**The prose needed judgement rather than a sed**, because there are three different consumers in this
repository and only one of them was wrong:

* a **Kafka** consumer — what the library does not have, and what the word means to a reader arriving
  for Kafka. Left alone everywhere: `kafka-console-consumer`, "the offsets are the oracle, never this
  library's own consumer", the scope fence in D2.
* a **Gradle** consumer — a build that resolves an artefact, which is Gradle's own vocabulary. Left
  alone in the build files and in the contract's header notes.
* **this repository's downstream build** — the one the item is about. Renamed, including in the
  findings of items that are already closed (B-12, B-13, B-15, B-16), because a finding that says
  "the consumer in `ci/downstream`" has stopped agreeing with itself.

**Two deliberate exceptions**, so that a later reader does not "finish" the rename and make things
worse:

1. this file is still `B-17-consumer-is-the-wrong-word-here.md`. The word is its **subject**, in
   quotation marks; renaming it would leave the item's own title unable to say what it is about.
2. `logs/b-16/run-2026-09-17.log` still says `ci/consumer`, and must. It is the record of a run that
   happened when the directory had that name, and editing evidence to match a later rename is how a
   log stops being evidence. It was rewritten by the first pass of the rename and put back.

**Both acceptance scripts were run after the rename**, which is the other half of this item:
`ci/publish/run.sh` — three coordinates, the probe compiles against them and fails against an empty
repository; `ci/b-13/run.sh` — 50/50 on each arm from the published snapshot, headers intact. And
`ci/b-16/run.sh`, which the item did not name but which reads the same directory, still measures what
it did before.
