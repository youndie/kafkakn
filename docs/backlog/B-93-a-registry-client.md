---
id: B-93
title: "A Schema Registry client on both targets: register, look up, cache"
status: wip
priority: P1
size: M
stage: stage-21-schema-registry
epic: feature-produce-a-record
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
