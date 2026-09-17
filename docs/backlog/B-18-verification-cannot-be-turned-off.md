---
id: B-18
title: "Certificate verification: the README and the contract disagree"
status: done
priority: P1
size: S
stage: stage-2-real-use
---

# B-18 — Certificate verification: the README and the contract disagree

The README says TLS verification is on **with no way to turn it off from this API**. The contract
and [feature-secure-connection](../features/feature-secure-connection.md) say librdkafka's
`enable.ssl.certificate.verification` **remains reachable as a raw key for whoever insists**. One of
them is wrong, and a reader who takes either at face value is misled about the security property
that matters most here.

**And the asymmetry makes it worse than a wording slip.** `kafka-clients` has no equivalent key —
`ssl.endpoint.identification.algorithm=` disables hostname verification only, never trust — so under
this contract's own configuration rule the key is a **platform** key and the JVM arm refuses it at
construction. Verification can therefore be disabled only on the arm that **has no oracle**: the one
whose correctness this whole project exists to check. A caller who turns it off is alone with the
implementation that was never meant to be trusted alone.

- **The decision to take.** Refuse `enable.ssl.certificate.verification` on **both** arms at
  construction, with a message naming the key, and make the README sentence true. The escape hatch
  it currently promises has no honest form: on the JVM it does not exist, and on native it exists
  only where nothing is watching.
- The rejected alternative is keeping the hatch and correcting the README instead. It fails the
  project's own rule about configuration — a key that works on one arm and not the other is a
  platform key, and this one's platform is the one without a witness. The library would be shipping
  a way to be wrong that its own suite cannot see.
- The second rejected alternative is a named convenience (`verifyCertificates = false`). Worse than
  the raw key: a library that offers one gets it used in production
  ([B-11](B-11-tls.md)).
- **The second key of the same family is decided here too, and differently.**
  `ssl.endpoint.identification.algorithm` exists on **both** arms with the same meaning — empty or
  `none` turns off **hostname** verification, `https` is the default — so it is a portable key by
  this contract's own rule, it carries Kafka's own name, and whatever it does happens where the
  oracle can see it. It passes. What it must not do is pass *quietly*: the contract names it as
  **the one remaining way to weaken TLS through this API**, so that the README's "verification is on
  and cannot be turned off" is read exactly as far as it is true — trust cannot be turned off,
  hostname checking can.
- Not covered: mTLS and certificate rotation — unchanged from B-11.

- AC: constructing a producer with that key throws on **both** arms, and the message names the key.
- AC: the test is shown failing against the arm as it is today — on native the key is accepted now,
  so the guard has to be watched catching it.
- AC: the README sentence and the contract's *Configuration* section say the same thing, and the
  contract records this as a decision with the asymmetry as its reason.
- AC: `ssl.endpoint.identification.algorithm` is accepted on both arms, named in the contract as the
  only remaining weakening knob, and the README sentence is worded so that it does not claim
  otherwise.
- Anchors: `kafkakn-core/src/nativeMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.native.kt`,
  `kafkakn-core/src/jvmMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.jvm.kt`,
  `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/TlsKeys.kt`,
  `docs/api/producer-contract.md`, `README.md`.

## What happened

**The decision stands and is implemented in common code.** `checkTlsKeys` lives in `commonMain` and
both actuals call it before they touch anything: a rule enforced on one arm is a rule the caller
meets for the first time on the platform they do not run locally. `enable.ssl.certificate.
verification` is refused whatever it is set to, and the message names the key.

**Watched failing first, on the arm that had the hole.** Before the rule existed, the native arm
accepted the key and constructed a producer — the two refusal tests failed there and passed on the
JVM, where `kafka-clients` had never heard of the key anyway. That asymmetry is the item's argument,
and it was visible in the suite for one run.

**The second key was not portable, which this item assumed it was.** Measured 2026-09-17: handed the
empty string `kafka-clients` documents as "off", librdkafka answers *"Configuration property
`ssl.endpoint.identification.algorithm` cannot be set to empty value"*. The **key** is shared; the
**value** that disables is not. So the contract spells off as `none` — librdkafka's spelling, and
the one an environment variable that expanded to nothing cannot produce by accident — the JVM arm
translates it, and the empty string is refused on both arms.

`TranslateForJavaTest` asserts the translation directly rather than through a producer that
constructs, because `kafka-clients` accepts `none` too: JSSE does not know that algorithm and
quietly enforces nothing, so a test that only watched construction succeed would have passed on the
untranslated map.

**A guard caught the first version of the code.** `common_is_platform_free.py` refused the refusal
message for naming librdkafka outside a comment — common code that knows which platform is which has
stopped being common. The message says "only one of the two clients has it" instead, which is what
the caller needs anyway.

**Found on the way, not fixed here:** `AccountingTest` appeared to time out after `runTest`'s default
minute on the jvm arm, on `main` as well as here, and was filed as
[B-24](B-24-the-central-guard-times-out.md). **That diagnosis was wrong** and B-24 says so: the test
was being run directly through Gradle rather than through `ci/b-09/run.sh`, which creates its topic,
so the Java client was waiting for metadata that would never arrive. Through its own runner the guard
passes on both arms.
