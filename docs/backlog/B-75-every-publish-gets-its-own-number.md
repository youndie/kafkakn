---
id: B-75
title: "Every publish gets a number of its own"
status: done
priority: P1
size: S
stage: stage-17-numbered-publishes
epic: feature-produce-a-record
blocked_by: []
---

# B-75 — every publish gets a number of its own

Every publish so far went out as `0.1.0-SNAPSHOT`, six builds under one name by 2026-09-26. A consumer
cannot name the build it depends on. [mostik](https://github.com/youndie/mostik), the HTTP bridge on keel built as this library's consumer, needed `enqueue` (B-74) and could only
say so by quoting a timestamped build, `0.1.0-20260926.224535-6`. The next publish from `main`
replaces what `0.1.0-SNAPSHOT` resolves to, and nothing downstream notices.

The owner asked for the portfolio's own scheme, the one `sborka` publishes under (`0.4.0.86`). The
head of the version is kept in `gradle.properties`. CI appends the workflow's run number and passes
the result as `-PVERSION`, which sborka's conventions read first
([SborkaSettings.version](https://github.com/youndie/sborka/blob/main/build-logic/conventions/src/main/kotlin/io/github/youndie/sborka/internal/SborkaSettings.kt)).
The number is computed by sborka's `determine-version` action, not copied here.

This revisits the owner's 2026-09-25 decision in part. Reposilite is still the only target. What
changes is that a published version never changes again.

## Acceptance

- `gradle.properties` holds the head, `version=0.1.0`, and no `-SNAPSHOT`.
- The publish workflow publishes `<head>.<run number>`. `ci/publish/run.sh`, the preflight and
  `verify-published.sh` all read that one number, through `ci/lib/coordinate.sh` (`KAFKAKN_VERSION`).
- **A published version is never overwritten.** The preflight asks for the version's POM under
  every coordinate and refuses the publish if any of them answers 200. A re-run of a workflow run
  keeps its run number, so without this check the re-run would write over what was already there.
- After the published version resolves back from the network, the workflow pushes a tag
  `v<version>` at the commit it built, so the number leads back to a commit. The workflow no
  longer runs on tag pushes: its own tag would start it again.
- The README's install snippet names a numbered version and says where to find the latest.

## Findings

- `-PVERSION` has been read by sborka's conventions since `ccf01b7` (2026-09-05), before the pinned
  `0.4.0.86`.
- No overwrite guard exists in sborka's `publish` convention on `main` as of 2026-09-27. Whatever
  refuses a second publish of one version has to be here.
- Verified on the Linux box, 2026-09-27. `KAFKAKN_VERSION=0.1.0.999 ci/publish/run.sh` published every
  file named `…-0.1.0.999…` under all three coordinates, and the downstream build compiled against them.
  The preflight's version check, against a local HTTP server holding that publication, answered 200 ×3
  and refused (exit 1). Against reposilite it answered 404 ×3 and passed. Against an unreachable host it
  answered 000 ×3 and refused. The positive control for the method: the real POM
  `kafkakn-core-0.1.0-20260926.224535-6.pom` answers 200 there.
- Mutant: `VERSION_ARG` emptied, so the build no longer gets `-PVERSION` while the scripts expect the
  full number. The build published under the head `0.1.0` instead, and `run.sh` failed with
  "3 coordinate(s) or file(s) missing".
- Not exercised here: the workflow itself (sborka's action, the tag push). Its first run is that
  check, and the pull request says so.
