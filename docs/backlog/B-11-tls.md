---
id: B-11
title: "TLS on both arms"
status: wip
priority: P1
size: M
stage: stage-2-real-use
blocked_by: [B-07]
---

# B-11 — TLS on both arms

A broker reachable over plaintext is not a normal deployment. On native this uses the OpenSSL
already linked into the binary — measured, and `ldd` does not change
([research §1.2](../research/research-architecture.md)).

- **The decision and its reason.** Certificate verification is on and cannot be turned off through
  this API. A library that offers the convenience gets it used in production.
- The rejected alternative is exposing `enable.ssl.certificate.verification` as a first-class
  setting. It stays available as a raw configuration key for whoever insists, without a named
  convenience pointing at it.
- Not covered: SASL, mTLS with client certificates, certificate rotation, and hostname-verification
  policy beyond the default.

- AC: the scenarios of [feature-secure-connection](../features/feature-secure-connection.md) pass on
  both arms.
- AC: the broker fixture grows an SSL listener **beside** the plaintext one, so the plaintext port
  can be shown refusing a TLS handshake — and the SSL port is **published**, not merely configured.
- AC: the wrong CA fails and the failure **names certificate verification**, not merely "brokers are
  down" — which is the same line a closed port produces.
- AC: offsets are verified over the plaintext listener, so the verification path is not the path
  under test.
- Anchors: `ci/broker/docker-compose.tls.yml`, `ci/broker/certs.sh`,
  `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/TlsTest.kt`.
