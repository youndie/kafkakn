---
id: B-14
title: "CI: make check on every pull request"
status: open
priority: P0
size: XS
stage: stage-0-it-builds
---

# B-14 — CI: `make check` on every pull request

The documentation gate, run by the same target a contributor runs, on GitHub's standard runners.

- **The decision and its reason.** The repository is **public**, so standard runners are free and
  `ubuntu-latest` is the right label. This is not a detail: in a private repository on this account a
  job on `ubuntu-latest` does not start at all, and pointing at a self-hosted runner registered to an
  organisation rather than to this account would leave runs queued for ever — a run that never
  finishes looks exactly like one that passed.
- The rejected alternative is trusting the local `make check`. A check that only runs where somebody
  remembers to run it is not a gate.
- Not covered: building or testing the Kotlin code in CI. The C bundle needs a Docker build and the
  suite needs a broker; whether that belongs on a hosted runner is a separate decision, taken when
  there is code to run.

- AC: `.github/workflows/check.yaml` runs `make check` on pull requests and on the default branch,
  with **no path filters** — they buy seconds and cost red default branches.
- AC: the backlog-number check against the base branch runs **before** `make check`, so a collision
  is reported by the check that can say which branch took the number.
- AC: the workflow is shown **failing** once, on a deliberately broken document, so it is known to be
  able to.
- Anchors: `.github/workflows/check.yaml`, `Makefile`.
