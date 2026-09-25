---
id: B-42
title: "A PKCS#1 client key: read in the source as working on one arm only"
status: done
priority: P2
size: S
stage: stage-6-real-deployments
blocked_by: [B-31]
---

# B-42 — a PKCS#1 client key on the two arms

Found while doing [B-31](B-31-client-certificates.md), by reading, not by measuring. `kafka-clients`
4.3.1 parses a PEM key store's private key through `PKCS8EncodedKeySpec` or
`EncryptedPrivateKeyInfo` (`DefaultSslEngineFactory.PemStore.privateKey`); its PEM pattern matches
`-----BEGIN RSA PRIVATE KEY-----` as well, so a PKCS#1 key is found and then fails to decode —
*"Private key could not be loaded"*. librdkafka hands `ssl.key.location` to OpenSSL
(`SSL_CTX_use_PrivateKey_file`), which reads PKCS#1 too. If that holds, one key file constructs a
producer on native and refuses on the JVM — at construction on both, so loudly, but a key the caller
meets for the first time on the arm they do not run locally.

- **The decision this item makes, not the one it assumes.** Two candidates: refuse a PKCS#1 key on
  both arms with a message saying how to convert it (`openssl pkcs8 -topk8`), or convert it on the
  JVM arm. The second is our own crypto code in the arm whose value is that it has none of ours; that
  is the reason to prefer the first, and it is written down here as a reason, not a verdict.
- AC: a PKCS#1 key, unencrypted and encrypted in OpenSSL's traditional form, is tried on both arms
  and what each does is recorded — measured, replacing the paragraph in
  [producer-contract](../api/producer-contract.md) that says "not measured".
- AC: whichever rule is chosen, one key file gives one answer on both arms, at construction.
- Anchors: `kafkakn-core/src/jvmMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.jvm.kt`,
  `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/TlsKeys.kt`, `ci/broker/certs.sh`.

## Findings (2026-09-25)

**Measured before the rule**, both forms derived from the suite's own client key so the certificate
still matches (`logs/b-42/measured-before-the-rule-*.log`):

| key | native | JVM |
|---|---|---|
| PKCS#1, plain | constructed, sent over the listener that requires a certificate | refused at construction: *"Invalid PEM keystore configs"* ← `IOException: overrun` |
| PKCS#1, traditionally encrypted | constructed, sent | refused at construction: *"Invalid PEM keystore configs"* ← `IOException: extra data at the end` |

**Decided: refuse on both arms, with the way out.** One rule in common code (`requirePkcs8`, called
from `checkTlsKeys`, so producer, consumer and admin all have it) reads the key file — the one
file read common code does, through `readPemFile` per platform — and refuses anything that is not
`BEGIN PRIVATE KEY` or `BEGIN ENCRYPTED PRIVATE KEY`, naming `openssl pkcs8 -topk8`. The conversion
was run as the message gives it and produced PKCS#8 keys, one encrypted and one not, that match the
certificate. Watched red first: native constructed, the JVM threw its own `KafkaException`. The
PKCS#8 key every client-certificate test uses is the positive control, and `MutualTlsTest` still
connects with it on both arms.

**Rejected:** converting on the JVM arm — the arm's value is carrying none of this library's own code,
and OpenSSL's traditional encryption would have been the first cryptography in it.
