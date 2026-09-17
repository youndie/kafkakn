---
id: B-14
title: "CI: make check on every pull request"
status: done
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

## Outcome — 2026-09-17

`.github/workflows/check.yaml` runs on `push` and `pull_request` with no path filters, the
base-branch number check ahead of `make check`, and `--on-main` only on the default branch.

**It was watched failing, which is the whole of this item.** Green since the first pull request is
not evidence that a gate can be red: a workflow whose `if:` never matches, a step that cannot find
its subject and exits zero, a job that is skipped rather than run — all three look exactly like a
pass from the outside.

| Run | Event | Broken on purpose | Failed at |
|---|---|---|---|
| [35229592559](https://github.com/youndie/kafkakn/actions/runs/35229592559) | push | a second `B-09` | `make check`: `duplicate ids: B-09` |
| [35229598512](https://github.com/youndie/kafkakn/actions/runs/35229598512) | pull_request | the same commit | the base-branch check, **before** `make check`: `B-09: here B-09-duplicate-demo.md, on origin/main already B-09-accounting.md` |
| [35229696244](https://github.com/youndie/kafkakn/actions/runs/35229696244) | pull_request | a feature pointing at an endpoint that does not exist | `make check`: `[broken-ref] ... api: producer-contract-that-does-not-exist - no such document` |

The first two rows are **one commit seen through two events**, which is what turns the ordering
criterion from a reading of the YAML into a measurement: the pull request gets the message naming
the file, the push gets the id alone.

**Also done here.** The file was a verbatim copy of a template and still opened with "Copy to
`.github/workflows/check.yaml` in the project being documented", followed by advice to a reader
deciding whether they want a step this repository has already decided about. The decisions are now
written as taken, with the measured messages in place of the illustrative ones.

**Not covered, deliberately.** Building or testing the Kotlin code in CI. The C bundle needs a
Docker build and the suite needs a broker; the suites run on the Linux box and each item's pull
request says so. Whether that belongs on a hosted runner is a separate decision with its own item
when it is taken.
