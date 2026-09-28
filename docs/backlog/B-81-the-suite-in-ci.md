---
id: B-81
title: "CI runs the whole suite on both arms against the fixture broker"
status: wip
priority: P1
size: M
stage: stage-19-the-suite-in-ci
blocked_by: [B-78]
---

# B-81 — CI runs the whole suite on both arms against the fixture broker

Until now the suite ran only by hand, on a build box, through each item's own runner. The `check` workflow ran the
documents and the formatter. So "the loop merges on green" read a green that had not run a single test. It was
backed by whatever the person or the loop happened to run on the box that day, on a shared machine whose state
(a fixture topic of the wrong shape, B-78) could decide the result.

- **The decision and its reason.** One script, `ci/suite/run.sh`, run by a person and by a new `suite` workflow,
  on every pull request and every push to `main`. It brings the fixture broker up with all its listeners and asks
  each listener to refuse what it must. Then it runs `jvmTest` and `linuxX64Test` whole, with `--continue`, and
  diffs the arms' observations. The repository is public, so the standard runners cost nothing. The C bundle, the
  Kotlin/Native toolchain and Gradle's caches are cached. Tests that pause or stop the broker stay behind their
  switches. Their items' runners are where they run.
- AC: the script fails when an arm ran fewer than `MIN_TESTS` tests, and when fewer than `MIN_OBSERVATIONS`
  observations were compared. A suite that compiled nothing, or skipped its way through, is not green.
- AC: the workflow is green on its own pull request, on the runner, with the counts in its log.
- AC: a positive control. A commit that breaks one assertion turns the workflow red and names that test. It is
  reverted before merge.
- AC: `CLAUDE.md`'s "Checks" section says what CI runs now.
- Anchors: `ci/suite/run.sh`, `.github/workflows/suite.yaml`.
