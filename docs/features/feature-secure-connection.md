---
id: feature-secure-connection
title: Connect to a secured broker — TLS, client certificates, SASL
type: feature
status: active
owner: unassigned
involved_services:
  - kafkakn-core
  - test-broker
client_entries: []
api:
  - producer-contract
tags: [producer, security]
---

# Connect to a secured broker — TLS, client certificates, SASL

## 1. Overview

A broker reachable over plaintext is not a normal deployment, so a producer that cannot speak TLS is
not usable. On the JVM this is the platform's TLS; on native it is the OpenSSL already linked into
the binary, which costs nothing additional at runtime — the measured binary's `ldd` set does not
change when TLS is turned on ([research §1.2](../research/research-architecture.md)).

**Built and measured 2026-09-17**: 500 records over the SSL listener on each arm, counted over the
plaintext one, and both arms refusing a broker signed by an authority they were not given. **Client
certificates since 2026-09-24** ([B-31](../backlog/B-31-client-certificates.md)): a third listener
that requires one, and both arms answering it.

## 2. Business rules

- Configuration is Kafka's own: `security.protocol=SSL`, `ssl.ca.location`.
- Certificate verification is **on**, and neither a convenience nor the raw key turns it off:
  `enable.ssl.certificate.verification` is refused at construction on both arms.
- Hostname checking can be relaxed, and that is the only part of TLS this API lets a caller weaken:
  `ssl.endpoint.identification.algorithm=none`.
- A peer that cannot be verified fails loudly, and the failure names certificate verification.
- A broker that requires a client certificate gets one: `ssl.certificate.location`,
  `ssl.key.location` and, for an encrypted key, `ssl.key.password` — librdkafka's spelling, as for the
  CA ([B-31](../backlog/B-31-client-certificates.md)). The certificate and its key come together or
  not at all, refused at construction on both arms.
- SASL `PLAIN`, `SCRAM-SHA-256` and `SCRAM-SHA-512`, over plaintext or TLS: `sasl.mechanism`,
  `sasl.username`, `sasl.password` ([B-32](../backlog/B-32-sasl-plain-and-scram.md)). The mechanism
  is required with a SASL protocol, and the username and password come together — both refused at
  construction on both arms. OAUTHBEARER is [B-33](../backlog/B-33-sasl-oauthbearer.md); GSSAPI is
  not in the native bundle and is not offered.

## 3. Scenarios (BDD / test cases)

### Scenario: A record reaches the broker over TLS
* **Given:** the test broker with an SSL listener and a CA the client trusts.
* **When:** 500 records are sent over the SSL listener.
* **Then:** the topic's end offsets, **read over the plaintext listener**, have grown by exactly 500.
* **Automated:** `TlsTest.a_record_reaches_the_broker_over_tls`, counted by `ci/b-11/run.sh` through
  `kafka-console-consumer` on the plaintext port. Measured 500/500 on each arm.
* *The verification path is deliberately not the path under test.*

### Scenario: An untrusted peer is refused, and says why
* **Given:** a CA that did not sign the broker's certificate.
* **When:** a record is sent over the SSL listener.
* **Then:** the call throws, and the failure names certificate verification — not merely that the
  broker is unreachable, which is the same thing a closed port says.
* **Automated:** `TlsTest.an_untrusted_peer_is_refused_and_says_why`, asserting on the message and
  every cause. Measured — the native arm: *"SSL handshake failed: ... certificate verify failed:
  broker certificate could not be verified, verify that `ssl.ca.location` is correctly configured"*;
  the JVM arm: *"SSL handshake failed ... unable to find valid certification path to requested
  target"*.

### Scenario: The plaintext listener is not silently accepted as TLS
* **Given:** the plaintext listener.
* **When:** a TLS connection is attempted to it.
* **Then:** it fails as a protocol failure.
* **Automated:** `TlsTest.the_plaintext_listener_is_not_silently_accepted_as_tls`.
* *Without it, "TLS worked" cannot be told from "TLS was quietly not used".*

### Scenario: The key that would turn trust off is refused, on both arms
* **Given:** a configuration carrying `enable.ssl.certificate.verification`.
* **When:** a producer is constructed.
* **Then:** construction throws and the message names the key — on the native arm as well as on the
  JVM one, where the Java client had never heard of it anyway.
* **Automated:** `VerificationTest.turning_certificate_verification_off_is_refused_at_construction`
  and `…the_key_is_refused_even_where_it_asks_for_verification_to_stay_on`. Watched failing on the
  native arm before the rule existed: it accepted the key and constructed a producer.
* *The value does not matter. A key that constructs on one arm and throws on the other is the defect,
  whatever it is set to.*

### Scenario: Hostname checking can be relaxed, in one spelling, on both arms
* **Given:** `ssl.endpoint.identification.algorithm` set to `none`, and separately to `https`.
* **When:** a producer is constructed on each arm.
* **Then:** both construct; and the empty string the Java client documents is refused on both,
  because librdkafka cannot take it.
* **Automated:** `VerificationTest.hostname_verification_is_still_reachable_on_both_arms`,
  `…the_default_spelling_of_hostname_verification_is_accepted_too`,
  `…the_empty_value_kafka_clients_documents_is_refused_on_both_arms`, and
  `TranslateForJavaTest` for the `none` → `""` translation itself.
* *Construction succeeding does not prove the translation is right: `kafka-clients` accepts `none`
  as well and quietly enforces nothing, so the translation is asserted directly.*

### Scenario: The right client certificate connects where one is required
* **Given:** a listener set to `ssl.client.auth=required`, beside the plaintext and TLS ones, which
  the broker's own tools have just shown refusing a client with no certificate and one with a
  certificate from the wrong authority (`broker.sh mtls-selftest`).
* **When:** 200 records are sent with a certificate the broker's authority signed, its key encrypted
  PKCS#8 and `ssl.key.password` set.
* **Then:** all 200 are there, **counted over the plaintext listener** by `kafka-console-consumer`.
* **Automated:** `MutualTlsTest.the_right_client_certificate_connects`, counted by `ci/b-31/run.sh`.
  Measured 200/200 on each arm.

### Scenario: No certificate is refused, and the refusal says so
* **Given:** the same listener, and the configuration that connects to the TLS listener next door.
* **When:** a record is sent.
* **Then:** the call throws, and the failure names the certificate.
* **Automated:** `MutualTlsTest.no_certificate_is_refused_and_the_message_says_so`. Measured — the
  native arm: *"… SSL routines::tlsv13 alert certificate required: SSL alert number 116"*; the JVM
  arm, two causes down: *"(certificate_required) Received fatal alert: certificate_required"*.

### Scenario: A certificate from the wrong authority is refused
* **Given:** a well-formed client certificate signed by an authority the broker does not trust.
* **When:** a record is sent.
* **Then:** the call throws, and the failure names the certificate.
* **Automated:** `MutualTlsTest.a_certificate_from_the_wrong_authority_is_refused`.
* *Neither client sends this certificate — see the quirks — so the broker's refusal is the same
  sentence as for no certificate at all, on both arms.*

### Scenario: A certificate without its key is refused at construction, on both arms
* **Given:** `ssl.certificate.location` without `ssl.key.location`, and the reverse.
* **When:** a producer is constructed.
* **Then:** construction throws, and the message names both keys.
* **Automated:** `MutualTlsTest.a_certificate_without_its_key_is_refused_at_construction_on_both_arms`,
  asserting on the message. Watched failing on native before the rule: librdkafka constructed a
  producer from the certificate alone.

### Scenario: PLAIN and both SCRAM digests connect, over plaintext and over TLS
* **Given:** the SASL listeners (9096 plaintext, 9097 TLS), which the broker's own tools have just
  shown refusing a wrong password for PLAIN and for SCRAM (`broker.sh sasl-selftest`).
* **When:** 100 records are sent with each of `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, and with
  `SCRAM-SHA-512` over `SASL_SSL` with `ssl.ca.location`.
* **Then:** every record is there, **counted over the plaintext listener** by
  `kafka-console-consumer`.
* **Automated:** `SaslTest.plain_connects`, `…scram_sha_256_connects`, `…scram_sha_512_connects`,
  `…scram_sha_512_over_tls_connects`, counted by `ci/b-32/run.sh`. Measured 100/100 each, both arms.

### Scenario: A password the JAAS format must escape connects on both arms
* **Given:** a broker user whose password holds a double quote and a backslash.
* **When:** 100 records are sent with it.
* **Then:** all 100 are there, on both arms.
* **Automated:** `SaslTest.a_password_with_a_double_quote_and_a_backslash_connects`, counted by
  `ci/b-32/run.sh`; and `TranslateForJavaTest` reading the password back through the Java client's
  own `JaasContext`, line breaks included.
* *The native arm sends the password raw, so its records arriving is what says the broker's own JAAS
  file was escaped right — the JVM arm getting in then says the translation was.*

### Scenario: The wrong password is refused, and the message names authentication
* **Given:** a wrong password, for PLAIN and for SCRAM-SHA-256.
* **When:** a record is sent.
* **Then:** the call throws, and the failure names authentication.
* **Automated:** `SaslTest.the_wrong_password_is_refused_and_the_message_names_authentication`.
  Measured — JVM: *"Authentication failed: Invalid username or password"*; native: *"Local:
  Authentication failure: … SASL authentication error: Authentication failed: Invalid username or
  password"*, after `message.timeout.ms`.

### Scenario: An incomplete SASL configuration is refused at construction, and names the key
* **Given:** a SASL protocol with no `sasl.mechanism`; a username without its password or the
  reverse; credentials for `GSSAPI` or for no mechanism.
* **When:** a producer is constructed.
* **Then:** construction throws, on both arms, and the message names the key.
* **Automated:** `SaslTest.a_sasl_protocol_without_a_mechanism_is_refused_at_construction_on_both_arms`,
  `…a_username_without_its_password_is_refused…`, `…credentials_for_a_mechanism_that_takes_none_are_refused…`,
  each asserting on the message. Watched failing on native first: librdkafka refused the first two in
  its own words, neither naming the key.

## 4. Quirks

- **Nothing about TLS is proved by the fact that OpenSSL is linked.** A linked library that was
  never called is the same evidence as one that does not work; the spike published a size figure for
  TLS before anything had ever opened a TLS connection.
- **librdkafka's own error text ends "install ca-certificates package".** In a minimal container
  image there are none, and that is the error a user will see first.
- On native the CA is a file path (`ssl.ca.location`); on the JVM it is the trust store. The contract
  keeps librdkafka's spelling and the JVM arm translates it into `ssl.truststore.location` plus
  `ssl.truststore.type=PEM` (`translateForJava`) — the one place that arm is not a plain delegate.
  A caller who writes `ssl.truststore.location` themselves is left alone.
- **The two clients disagree about how to spell "off" for hostname checking, not about the key.**
  librdkafka refuses an empty value (*"cannot be set to empty value"*, measured 2026-09-17); the Java
  client documents the empty string. The contract takes `none` and translates. Worth knowing before
  copying a `ssl.endpoint.identification.algorithm=` line out of Kafka's documentation.
- **The two arms are not equally informative about the plaintext port.** Told to speak TLS to it,
  librdkafka says *"SSL connection closed by peer: connecting to a PLAINTEXT broker listener?"*,
  while the Java client reports only *"Topic ... not present in metadata after 20000 ms"*. Both fail,
  which is what the scenario requires; only one says why. Worth knowing before believing a metadata
  timeout.
- **A certificate that cannot be verified reaches the caller as `Local: Message timed out`** unless
  the producer keeps what the error callback said. `rd_kafka_produce` only enqueues, so the record is
  queued, retried and finally timed out; the sentence naming the certificate arrived on a different
  callback, minutes earlier ([research §2.9](../research/research-architecture.md)).
- **Certificate verification stays on, and the raw key is refused too** — on both arms, at
  construction, with the key named in the message
  ([B-18](../backlog/B-18-verification-cannot-be-turned-off.md)). This used to say
  `enable.ssl.certificate.verification` remained reachable for whoever insists. It does not, and the
  reason is the configuration rule rather than a view about security: the key exists only in
  librdkafka, so honouring it would mean the one way to switch trust off lived on the arm with no
  oracle.
- **Hostname checking is the exception, and it is named as one.**
  `ssl.endpoint.identification.algorithm` is on both arms and travels. Off is spelled **`none`**:
  librdkafka refuses an empty value outright — measured — while the Java client documents the empty
  string, so the JVM arm translates. It is the only remaining way to weaken TLS through this API and
  the contract says so out loud.

- **The client certificate is the one TLS translation that reads files.** librdkafka takes two
  paths; the Java client's PEM key store takes a path only as one file holding both, and two separate
  things only as their contents. So the JVM arm reads the certificate and the key at construction
  and hands the text over ([producer-contract](../api/producer-contract.md), TLS).
- **Neither client sends a client certificate the broker would not trust.** librdkafka's
  `rd_kafka_ssl_cert_callback` (`librdkafka-2.13.0.tar.gz!/src/rdkafka_ssl.c`) withholds one whose issuer is not in the
  server's `certificate_authorities`, and the Java client sends none either — measured, not read:
  the broker answered `certificate_required` to both arms. A caller with a certificate from
  the wrong authority therefore reads *"certificate required"* — true, and not the sentence they will
  look for.
- **librdkafka constructs a producer from a certificate with no key.** It checks the pair only when a
  key is set (`check_pkey`). Refused by this library on both arms, measured 2026-09-24.

- **Credentials reach the Java client only inside a JAAS string**, and a JAAS string ends a quoted
  value at a double quote and at a line break. The JVM arm escapes both, and the backslash. A
  translation that did not would fail only on the JVM arm — as a parse error, or as a different
  password the broker then refuses, which reads exactly like a wrong one.
- **The broker image will not start a SASL listener without `KAFKA_OPTS`**: its configure script
  `ensure`s it, so the fixture's users are in a JAAS file named there. The SCRAM credentials are not
  in the file; they live in the metadata log and are created on every `broker.sh up`, one mechanism
  per call — both in one `--add-config` is refused, *"A user credential cannot be altered twice in the
  same request"*.

## 5. Code anchors

| What | Where |
|---|---|
| the TLS configuration path | `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/ProducerConfig.kt` |
| the scenarios above | `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/TlsTest.kt` |
| the broker's SSL overlay and certificates | `ci/broker/docker-compose.tls.yml`, `ci/broker/certs.sh` |
| the CA and hostname translations on the JVM arm | `kafkakn-core/src/jvmMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.jvm.kt` |
| the two TLS keys and what each arm may do with them | `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/TlsKeys.kt` |
| what librdkafka last complained about | `kafkakn-core/src/nativeMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.native.kt` |
| the run that measured it | `ci/b-11/run.sh` |
| the client-certificate scenarios | `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/MutualTlsTest.kt` |
| the listener that requires one, and its self-test | `ci/broker/docker-compose.tls.yml`, `ci/harness/broker.sh` |
| the run that measured client certificates | `ci/b-31/run.sh` |
| the SASL rules both arms enforce | `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/SaslKeys.kt` |
| the SASL scenarios | `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/SaslTest.kt` |
| the run that measured SASL | `ci/b-32/run.sh` |
