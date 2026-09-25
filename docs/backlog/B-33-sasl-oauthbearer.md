---
id: B-33
title: "SASL OAUTHBEARER with a token the caller supplies"
status: wip
priority: P3
size: M
stage: stage-6-real-deployments
blocked_by: [B-32]
---

# B-33 — SASL OAUTHBEARER

OAUTHBEARER is how a growing share of hosted Kafka authenticates. Our native bundle has the
OAUTHBEARER provider and **not** the OIDC token fetcher, because the bundle is built without curl
([research §1.8](../research/research-architecture.md): `rd_kafka_sasl_oauthbearer_provider` defined,
`rd_kafka_sasl_oauthbearer_oidc_token_refresh_cb` absent).

- **The decision and its reason.** The only shape both arms can honour is **a token the caller
  supplies**: a suspending provider the library calls when a token is needed or about to expire. On
  native that feeds `rd_kafka_oauthbearer_set_token`; on the JVM it has to become a login callback
  handler — which the Java client configures **by class name**, so bridging a Kotlin function into it
  is the design problem of this item, not a detail.
- The rejected alternative is adding curl to the bundle so OIDC works on native. It changes the
  `ldd` set that [B-16](B-16-readme-says-what-was-measured.md) measured identical, grows the binary,
  and is a decision about the bundle rather than about SASL — a separate item if it is wanted.
- Not covered: OIDC client-credentials fetching, cloud-specific signing schemes.

- AC: a producer with a token provider connects to an OAUTHBEARER listener on both arms.
- AC: a provider that throws fails the connection with the provider's exception visible, not a generic
  authentication error.
- AC: a token that expires is refreshed through the provider without the caller doing anything, and a
  test watches it happen.
- Anchors: `kafkakn-core/src/nativeInterop/cinterop/rdkafka.def`, `ci/librdkafka/build.sh`.
