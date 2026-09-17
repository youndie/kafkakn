---
id: B-18
title: "Certificate verification: the README and the contract disagree"
status: wip
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
  `docs/api/producer-contract.md`, `README.md`.
