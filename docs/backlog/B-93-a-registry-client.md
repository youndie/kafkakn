---
id: B-93
title: "A Schema Registry client on both targets: register, look up, cache"
status: done
priority: P1
size: M
stage: stage-21-schema-registry
epic: feature-schema-registry
blocked_by: [B-92]
---

# B-93 — a Schema Registry client on both targets: register, look up, cache

The part every format needs. `SchemaRegistry(url, httpClient)` (B-92's decision: a Ktor `HttpClient`, CIO by
default) registers a schema under a subject and looks one up by id, and
it caches both, so one record costs no request once its schema is known. Subjects follow Confluent's default,
`TopicNameStrategy` (`<topic>-value`, `<topic>-key`).

- AC: against the fixture registry, both arms register a schema, get its id, and read the schema back by id. The
  registry's own REST answer (`curl`) agrees.
- AC: a second `encode` of the same type sends no request (counted at the fixture, not by the client).
- AC: a registry that answers an error is one kafkakn type with the registry's error code and message in it.
- Anchors: `kafkakn-schema-registry/`.

## Findings (2026-09-28)

- `kafkakn-schema-registry`: `SchemaRegistry(url, httpClient)` over Ktor 3.5.2, CIO by default. It registers
  (`POST /subjects/{subject}/versions`), reads by id (`GET /schemas/ids/{id}`), caches both, and names subjects by
  `TopicNameStrategy`. The fixture brings `cp-schema-registry:8.3.2` beside the broker, in the `kafkakn` Compose
  project.
- *Measured*, `ci/b-93/run.sh`, both arms: one schema registered twice was 1 request in the registry's own request
  log, and the same schema under two subjects was 2. The registry's REST answer (`curl`) holds id 1 as `JSON`. An
  unreadable schema is `SchemaRegistryException` 422/42201, and an unknown id 404/40403.
- **Found by the first control:** under `BACKWARD` a property added to an open JSON Schema is refused
  (`OPTIONAL_PROPERTY_ADDED_TO_OPEN_CONTENT_MODEL`). So B-94 generates closed schemas (`additionalProperties: false`),
  and B-96 measures evolution on them. The control became the same schema under two subjects.
- Mutant: `register` ignoring its cache was caught by the runner's count on both arms (*"the cached registration
  reached the registry 2 times"*).
- **CI's fresh runner found a harness defect:** for a container that does not exist, `docker inspect -f` prints an
  empty line and fails. The `|| echo absent` then read as a project named `\nabsent`, and `up` refused. The owner is
  now asked only of a container that exists, and on the box the absent case reads as absent.
