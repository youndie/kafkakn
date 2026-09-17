---
id: B-17
title: "`ci/consumer` is the wrong word in a Kafka repository"
status: open
priority: P2
size: XS
stage: stage-3-usable-by-others
---

# B-17 — `ci/consumer` is the wrong word in a Kafka repository

To anyone arriving for Kafka, **consumer** means a Kafka consumer — the one thing this library
deliberately does not have ([D2](../research/research-architecture.md)). The directory it names is a
downstream build that resolves the published artefact and links a binary
([B-13](B-13-external-consumer-acceptance.md)).

The word is in the directory, in `ci/b-13/run.sh`, in the README, in three documents and in the
item that created it. A reader who opens `ci/consumer/` expecting the thing the README says does not
exist has been told something wrong by the layout itself.

- **The decision and its reason.** `ci/downstream`, because that is what it is: a build downstream of
  the publication. It also survives the day a real consumer exists, which `ci/consumer` would not.
- The rejected alternative is leaving it and explaining in the README. A name that needs a footnote
  in the README is a name that will be misread by everyone who does not reach the footnote.
- Not covered: renaming `ci/publish/consumer`, the smaller probe inside the publication proof — the
  same argument applies and it moves in the same change.

- AC: no path or document in the repository calls a downstream build a consumer, checked by grep.
- AC: `ci/b-13/run.sh` and `ci/publish/run.sh` both still pass after the rename — a rename that
  breaks the acceptance is how the acceptance stops being run.
- Anchors: `ci/consumer/`, `ci/publish/consumer/`, `ci/b-13/run.sh`, `README.md`.
