---
id: B-97
title: "The fixture broker shared a Compose project with mostik's, and a fresh broker failed its own topic check"
status: done
priority: P0
size: S
stage: stage-21-schema-registry
blocked_by: []
---

# B-97 — the fixture broker shared a Compose project with mostik's

Found by B-92's first run after the box rebooted: `kafkakn-broker` did not exist. Bringing it back printed
*"Container mostik-broker Recreated"*.

- **Why.** Compose names a project after the directory of its file when nothing names it. kafkakn and
  [mostik](https://github.com/youndie/mostik) keep their fixture at the same path, `ci/broker/docker-compose.yml`,
  each with a service called `broker`. So on the shared box they were one project, `broker`, and one service.
  Whichever repository brought its broker up last removed the other's container and created its own in its place.
- **And a fresh broker failed B-78's check.** On a broker that had just started, the describe right after
  `--create --if-not-exists` answered nothing. Every run then stopped on the next fixture topic with *"no
  partitions"*.
- **Done.** `name: kafkakn` in the Compose file. `broker.sh up` removes a `kafkakn-broker` left by the unnamed project
  `broker`, which is this repository's own container, and refuses one owned by any other project.
  `ensure_topic` asks for the description again for up to 15 s before it judges the shape.
- AC: on the box, `up` twice gives exit 0 both times. `kafkakn-broker` carries the project label `kafkakn`, and
  `mostik-broker` keeps its own and stays up beside it.
- **Not here:** mostik's Compose file still has no name of its own. That is mostik's repository, told separately.
- Anchors: `ci/broker/docker-compose.yml`, `ci/harness/broker.sh`.
