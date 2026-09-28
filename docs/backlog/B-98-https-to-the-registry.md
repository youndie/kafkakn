---
id: B-98
title: "A registry served over HTTPS, reached from both arms (Curl on native)"
status: open
priority: P2
size: M
stage: stage-21-schema-registry
epic: feature-schema-registry
blocked_by: [B-93]
---

# B-98 — a registry served over HTTPS, reached from both arms

[B-92](B-92-schema-registry-what-is-real.md) measured that Ktor's CIO client has no TLS on Kotlin/Native, and the
owner chose that a native caller who needs HTTPS passes `HttpClient(Curl)`. This item proves that path. The fixture
registry gets an HTTPS listener with the fixture's own CA. Then the JVM arm reaches it through CIO and the native arm
through Curl, which is a test dependency only.

- AC: both arms register and read a schema over HTTPS, verifying the fixture's CA.
- AC: a registry certificate from the wrong CA is refused on both arms, and the failure names certificate
  verification.
- AC: the README and the module's KDoc say how a native caller gets HTTPS, and what libcurl adds to its binary's `ldd`.
- Anchors: `kafkakn-schema-registry/`, `ci/broker/`.
