---
id: B-11
title: "TLS on both arms"
status: done
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

## Outcome — 2026-09-17

Closed by `ci/b-11/run.sh`. Suites: **18 tests on jvm, 20 on linuxX64, 0 failures**; the arms still
agree on all 14 shared observations.

| | jvm | linuxX64 |
|---|---|---|
| records over the SSL listener, counted over the plaintext one | 500/500 | 500/500 |
| a broker signed by an authority it was not given | refused | refused |
| TLS spoken at the plaintext port | fails | fails |

**What each arm says when it refuses**, which is the criterion that was not free:

* native — `SSL handshake failed: ... certificate verify failed: broker certificate could not be
  verified, verify that ssl.ca.location is correctly configured or root CA certificates are
  installed (install ca-certificates package)`
* jvm — `SSL handshake failed <- ... unable to find valid certification path to requested target`

**The fixture is checked before anything is believed of it.** `broker.sh tls-selftest` connects with
the right CA and fails with the wrong one, asked through the broker's own tools — a broker that
accepted every certificate, or a client that verified none, would make all three scenarios green.
`certs.sh` also verifies both authorities against the broker certificate before handing them over.

**Two defects found on the way, both in the library rather than the test.**

1. **Topic-level configuration never reached the topic** — the native arm built a fresh
   `rd_kafka_topic_conf_t` per topic, so `message.timeout.ms`, `acks` and the rest stayed on the
   global handle. It had been invisible for four items because librdkafka's default for
   `request.required.acks` is already `-1`. Found by a test asking to fail in 20 s and taking 60.
   [research §2.8](../research/research-architecture.md).
2. **The last error librdkafka reports is the least informative one** — `_ALL_BROKERS_DOWN` is a
   summary that arrives after the cause, so an unverifiable certificate and a closed port produced
   the same sentence. [research §2.9](../research/research-architecture.md).

**Fixture notes.** The broker refuses a PEM keystore (`SSL key store password cannot be specified
with PEM format`) and its image insists on supplying one, so the server uses PKCS12 while both
clients read the PEM CA. Certificates are generated outside the source tree and kept rather than
regenerated: new ones under a running broker leave it presenting a certificate no client trusts, and
that looks exactly like a misconfigured client.

**Not covered, as the item says.** SASL, mTLS with client certificates, certificate rotation, and
hostname-verification policy beyond the default.
