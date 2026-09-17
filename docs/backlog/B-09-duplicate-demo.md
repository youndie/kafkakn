---
id: B-09
title: "A number that is already taken on main"
status: open
priority: P3
size: XS
stage: stage-1-produce
---

# B-09 — a number that is already taken on main

Deliberately broken, and removed before this branch merges. It exists so the ordering claim in
`.github/workflows/check.yaml` is a measurement rather than a reading of the file: the step that
compares against the base branch runs first and names *which branch took the number*, while the
gate would only say `duplicate ids: B-09`.

- AC: this file does not survive the branch.
- Anchors: `.github/workflows/check.yaml`.
