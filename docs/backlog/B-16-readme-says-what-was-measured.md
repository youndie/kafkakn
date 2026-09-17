---
id: B-16
title: "The README says what was measured, not what sounded right"
status: open
priority: P1
size: S
stage: stage-3-usable-by-others
---

# B-16 — The README says what was measured, not what sounded right

Five corrections to one file, from a review of it. Four are about claims that are **false or
unfalsifiable as written**, and the fifth is the missing half of what a reader needs before they can
link the artefact at all.

- **The two defects are the strongest claim in the document and it names neither.** "The oracle
  found a real defect on the first day it ran, and an external consumer found another the library's
  own suite could not see" is the proof of this project's thesis, served as a teaser. One sentence
  each, with the item that holds the run: the partitioner disagreement
  ([B-07](B-07-native-actual.md), [research §2.2](../research/research-architecture.md)) and the
  klib that carried no C ([B-15](B-15-native-klib-carries-no-c.md), §2.12).
- **"No runtime dependency beyond libc" is contradicted by the spike's own `ldd`.** The measured set
  is `libc libcrypt libgcc_s libm libresolv` and the loader, and `libgcc_s` is not libc — a reader
  who runs `ldd` finds the sentence false in its first word that matters. The measured claim is
  **stronger**: the `ldd` set is *identical to a Kafka-free Kotlin/Native binary built the same
  way* ([research §1.2](../research/research-architecture.md)).
- **Nothing tells whoever links the binary what it needs.** A short block under *Getting it*: the
  minimum glibc is Kotlin/Native's own floor and kafkakn adds nothing above it, because the C bundle
  is built in `manylinux2014` (glibc 2.17) rather than on the host
  ([B-03](B-03-c-bundle-old-glibc.md), [D4](../research/research-architecture.md)); and the bundle
  carries a one-line local patch to `rdrand.c`.
- **No CI badge**, though both halves of the gate run there.
- **The README links D7 for "snapshots only" and never says when that ends.** One sentence naming a
  **condition and not a date**: a user arriving of their own accord. There is no announcement to
  bring one — [B-21](B-21-does-anyone-want-this.md) was dropped because nothing is posted anywhere —
  so the sentence says that plainly instead of implying a plan.

- **The decision and its reason.** The review that produced this list also asked for "the upstream
  PR status" for the patch. **There is no PR and there will not be one**: nothing from this project
  goes upstream ([B-03](B-03-c-bundle-old-glibc.md) says so twice, research §1.3 says "nothing is
  being reported upstream"). The README says the patch is maintained here, and says why, rather than
  pointing at a tracker that does not exist.
- The rejected alternative is leaving the `ldd` sentence as a simplification. It is not a
  simplification, it is a claim a reader can refute in one command — and the true version is the
  better advertisement.
- Not covered: the TLS-verification contradiction, which needs a decision rather than an edit —
  [B-18](B-18-verification-cannot-be-turned-off.md).

- AC: every claim in the README either names where it was measured or is visibly a decision.
- AC: the two defects are named in one sentence each, with their item.
- AC: `ldd` run against the current test binary and the Kafka-free one, and the README says what
  that run showed rather than what the spike showed.
- Anchors: `README.md`, `docs/research/research-architecture.md`.
