---
id: B-21
title: "RQ-B: does anyone outside this portfolio want it?"
status: dropped
priority: P1
size: M
stage: stage-4-a-real-user
---

# B-21 — RQ-B: does anyone outside this portfolio want it?

**Dropped 2026-09-17, before any of it ran: nothing is posted anywhere.** The rule that keeps this
project out of other people's repositories covers announcements too, and an announcement is the only
mechanism this item had.

What it was for is still true — every question answered so far was about correctness and every
answer was green, and none of them is evidence that the library should exist. The rest of this file
is kept as the design of a test that was not run, because the alternative is re-inventing it badly
later.

## What dropping it costs, and what it settles

- **The amber outcome becomes the standing state rather than a finding.** The pre-registered
  expectation was "correct and unwanted"; without a window, that is simply where the repository is,
  and nobody will be able to say afterwards whether a demand existed.
- **Maven Central stays gated**, and on nothing this project can perform: the condition is a user
  arriving of their own accord, which is now the only way one can.
- **`linuxArm64` stays closed** for the same reason it was closed — a second target is engineering
  with no signal behind it, and there will be no signal.
- The README's snapshot sentence ([B-16](B-16-readme-says-what-was-measured.md)) therefore names a
  **condition**, not a date, and this file is where it points for why there is no date.

---

*The design below was written before the item was dropped. It is preserved, not active.*

- **The decision and its reason.** Announced **once each**, one week apart: Kotlin Slack
  (`#kotlin-native`, `#server`), r/Kotlin, then a Show HN. The Show HN is built on **the two
  findings** — the glibc 2.19 sysroot, and `flush` returning success with a quarter of the input
  lost — rather than on the library: the findings are the part a stranger gains something from
  whether or not they ever use this.
- The rejected alternative is announcing first and deciding what counts later. The whole value of a
  demand test is that the bar was set while the answer was unknown.
- Not covered: paid promotion, cross-posting the same text, and announcing before
  [B-19](B-19-close-under-a-real-shutdown.md) is green — nothing is said about a library that can
  lose a record on shutdown.

**The window is six weeks from the first announcement**, and the verdicts are:

| | What it takes | What follows |
|---|---|---|
| **green** | one external issue or PR, **or** one stranger describing their own use case — "nice work" is not one | Maven Central is unlocked and `linuxArm64` re-opens as an item |
| **amber** | stars and comments only | snapshots stay, nothing new is built |
| **red** | nothing | the repository is frozen at the producer, the README says so, **and the freeze is the published result** |

- AC: three announcements, one week apart, in that order, each linked from the research note.
- AC: at week six the verdict is written into `docs/research/2026-10-xx-m2-publisher.md` **whichever
  colour it is**, with the links, and the README's snapshot sentence ([B-16](B-16-readme-says-what-was-measured.md))
  points at it.
- AC: no coordinate, target or release decision is taken before that date on the strength of early
  signal.

**Who would have done this.** The announcements are posts under a person's name in communities that
have opinions about being announced to — the owner's to make, never an agent's. That was true while
the item was open and it is the shape of the rule that closed it.
