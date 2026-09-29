---
id: B-98
title: "A registry served over HTTPS, reached from both arms (Curl on native)"
status: done
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

## Iteration 1 (2026-09-29): HTTPS works on both arms alone, and cannot link beside kafkakn on native

**What exists, on the branch `feat/b-98-https-to-the-registry`:**
- The fixture registry has an HTTPS listener on 18082 beside its plaintext one, serving the broker's certificate
  (fixture CA, SAN `127.0.0.1`). `broker.sh up` waits for it and checks that the wrong CA is refused by `curl`.
- `RegistryHttpsTest`, green on both arms: a schema registered and read back over HTTPS (CIO with the fixture CA on
  the JVM, `HttpClient(Curl) { engine { caInfo = ... } }` on native), and a certificate from the wrong CA refused.
  The words: native *"TLS verification failed ... SSL peer certificate or SSH remote key was not OK
  (CURLE_PEER_FAILED_VERIFICATION)"*; JVM `SunCertPathBuilderException: unable to find valid certification path to
  requested target`.

**What was measured about Curl on native (Ktor 3.5.2):**
- **libcurl is not the system's.** `ktor-client-curl-linuxX64Cinterop-libcurlMain-3.5.2.klib` carries
  `included/libcurl.a`, `libssl.a`, `libcrypto.a` and `libnghttp2.a`, with `linkerOpts.linux=-lz`. The test binary's
  `ldd` gains only `libz.so.1` over kafkakn-core's; its `NEEDED` has no libcurl. The OpenSSL inside is 3.6.3.
- **Its default CA bundle is Debian's path**, `/etc/ssl/certs/ca-certificates.crt`, compiled in: on another
  distribution a caller passes `caInfo`.
- So no `libcurl4-openssl-dev` is needed anywhere; the CI step first added for it was taken out again.

**What stops the item: a native binary with kafkakn-core and Curl does not link.** `OneProcessTest` (the case the
module is for: encode through the registry over HTTPS, produce over TLS, in one process) passes on the JVM. On
`linuxX64` the link fails: `ld.lld: error: duplicate symbol: i2d_SSL_SESSION`, `ssl_load_ciphers`,
`ssl3_handshake_write`, ... — `libssl.a` from kafkakn-core's cinterop klib (OpenSSL 3.0.13, built by
`ci/librdkafka/build.sh`) against the same objects in Ktor's Curl klib (3.6.3). Two static OpenSSLs of different
versions in one binary. The owner's B-92 decision, "a native caller passes `HttpClient(Curl)`", holds only for a
binary that does not also use kafkakn-core, which is not the binary anyone builds.

## Question: how does a native kafkakn service reach a registry over HTTPS?

1. **One OpenSSL: HTTPS through kafkakn's own.** kafkakn-core already links OpenSSL 3.0.13 statically. The
   registry module's native side gets a small Ktor `HttpClientEngine` (or a transport under `SchemaRegistry`) over
   Ktor's native sockets plus that OpenSSL through cinterop: TLS, peer and host-name verification, HTTP/1.1 with a
   JSON body, which is all the registry's API needs. No libcurl, nothing new in `ldd`, the same CA handling as the
   broker's TLS. Cost M. The module's native half then depends on kafkakn-core's C bundle.
2. **libcurl in kafkakn's C bundle.** Build libcurl statically against the bundle's own OpenSSL (librdkafka would
   gain `WITH_CURL`, which is also its OIDC token fetcher), expose `curl_easy_*` through the cinterop, and write the
   engine over it. One OpenSSL too; more C to build and keep current in the old-glibc image. Cost M–L.
3. **HTTP only on native, for now.** The JVM reaches HTTPS through CIO; a native service reaches a registry over
   HTTP (in-cluster, or through a TLS-terminating sidecar), and the docs say so, with this link failure as the reason.
   `RegistryHttpsTest` stays for the JVM and for native without kafkakn-core. Cost XS. Revisit when a caller needs it.
4. *Rejected:* linking with `-z muldefs` so the first definition wins. It would run one OpenSSL's code against the
   other's structures, silently.

**Recommendation: 3 now, 1 when a native caller needs HTTPS.** 1 is the clean design, but it is a TLS client to own;
3 costs nothing and is honest. The owner decides.

**The owner's answer (2026-09-29): 3.** A native service reaches a registry over HTTP; option 1 is the route when a
native caller needs HTTPS. Recorded in `backlog.md`'s decisions.

## Done (2026-09-29)

- **What stayed:** the fixture registry's HTTPS listener and `broker.sh`'s check that the wrong CA is refused;
  `RegistryHttpsTest` on both arms (native through Curl, in the module's test binary, which does not link
  `kafkakn-core`); `OneProcessTest` moved to `jvmTest`, with `kafkakn-core` a JVM test dependency only. Green on both
  arms on the Linux box: 2 + 2 HTTPS tests, and the one-process test on the JVM.
- **The ACs, as the decision reshaped them:** both arms register and read over HTTPS verifying the fixture CA, and
  both refuse the wrong CA naming certificate verification, measured. The README and the service document say how a
  native caller reaches a registry (HTTP), why (the link failure), what Curl adds to `ldd` (`libz.so.1` only), and its
  compiled-in CA path.
- **Left out by the decision:** HTTPS from a native binary that also uses `kafkakn-core`.
