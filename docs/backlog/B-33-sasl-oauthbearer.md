---
id: B-33
title: "SASL OAUTHBEARER with a token the caller supplies"
status: done
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

## Findings (2026-09-25)

**Measured, `ci/b-33/run.sh`, both arms.**
- 50/50 records with the caller's tokens, counted over plaintext.
- Twelve-second tokens, thirty seconds of sending against a broker that re-authenticates every ten:
  four tokens issued on each arm and every send succeeded.
- A provider that throws is visible in its own words: at construction on the JVM, and at the first
  send on native, through the error callback.

**The JVM bridge, and what it cost to read.** The Java client instantiates its login handler by
class name, so the provider is registered under an id carried in the JAAS options. Throwing out of
the handler lost the provider's words: *"An internal error occurred while retrieving token from
callback handler"*, measured, then read in `OAuthBearerLoginModule.identifyToken`. They come through
`OAuthBearerTokenCallback.error` instead.

**The broker fixture.** OAUTHBEARER in the shared `KafkaServer` JAAS section stopped the broker from
starting (*"Must supply exactly 1 non-null JAAS mechanism configuration"*). It is now configured on
the plaintext SASL listener alone. The JAAS file is written on every run, because a file only the full
certificate generation wrote would never change on a box whose certificates are still valid.

**Watched red.** The native bridge was made to overstate each token's life by an hour. Nothing was
refreshed, the broker refused the expired token at re-authentication, and the sends stalled until the
test's own timeout (`logs/b-33/`). On the JVM, the exception path above was the red for the
"visible" criterion.

**Scope.** Only the producer takes a provider. OAUTHBEARER on a consumer or an admin client is refused
at construction. Wiring either is a new item if it is wanted.
