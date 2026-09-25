#!/usr/bin/env bash
# Certificates for the TLS listener, and a second CA that signed none of them.
#
# Two certificate authorities, not one. A test that only ever presents the right CA cannot tell
# "verification passed" from "verification never ran" - and the second outcome is the one that makes
# a TLS test worthless, because it is also what a client with verification disabled looks like. The
# wrong CA is how that question gets asked.
#
# PEM throughout, no keytool and no JKS: Kafka 4.x reads `ssl.keystore.type=PEM` directly, librdkafka
# only ever wanted a PEM CA file, and the JVM client reads a PEM truststore. One format for all three
# is one fewer conversion to get wrong.
#
# Nothing lands in the source tree, and that is not only about keys in a repository being a habit
# that survives into somewhere it matters. The working copy on the build box is a ONE-WAY REPLICA:
# anything written there is erased on the next cycle, and anything written on the Mac arrives with
# the replica's own file mode - 0600, which the broker container's non-root user cannot read. So the
# certificates are generated on the box that runs the broker, beside the C bundle, in a cache
# directory the replica never touches.
set -euo pipefail

OUT=${KAFKAKN_TLS_DIR:-$HOME/.cache/kafkakn/tls}
DAYS=${DAYS:-3650}
# A fixture password, in a fixture whose certificates are regenerated in a second. It is written to
# a file because the broker image insists on reading it from one.
KEY_PASSWORD=${KEY_PASSWORD:-kafkakn}
# The client key's password, which the suite passes as `ssl.key.password` (B-31). A different value
# from the broker's, so a client handed the wrong one cannot succeed by coincidence.
CLIENT_KEY_PASSWORD=${CLIENT_KEY_PASSWORD:-kafkakn-client}

# B-42: the right client key again, in OpenSSL's TRADITIONAL (PKCS#1) form - plain, and encrypted the
# traditional way. Derived from client.key rather than generated, so the certificate still matches,
# and derived on every run, BEFORE the early exit below: new files must never be the reason the whole
# set is regenerated under a running broker, which is the trap the next comment describes.
derive_pkcs1() {
    [ -s "$OUT/client.key" ] || return 0
    [ -s "$OUT/client-pkcs1.key" ] || openssl rsa -in "$OUT/client.key" -passin "pass:$CLIENT_KEY_PASSWORD" \
        -traditional -out "$OUT/client-pkcs1.key" 2>/dev/null
    [ -s "$OUT/client-pkcs1-encrypted.key" ] || openssl rsa -in "$OUT/client.key" -passin "pass:$CLIENT_KEY_PASSWORD" \
        -traditional -aes256 -passout "pass:$CLIENT_KEY_PASSWORD" -out "$OUT/client-pkcs1-encrypted.key" 2>/dev/null
    chmod 644 "$OUT"/client-pkcs1*.key 2>/dev/null || true
    grep -q 'BEGIN RSA PRIVATE KEY' "$OUT/client-pkcs1.key" && grep -q 'Proc-Type: 4,ENCRYPTED' "$OUT/client-pkcs1-encrypted.key" || {
        echo "certs.sh: the PKCS#1 keys are not in the form B-42 asks about" >&2; exit 1; }
}

# The broker's SASL users (B-32), written on EVERY run like the keys
# above: a file only the full generation wrote would never change on a box whose certificates are
# still valid, and the broker would keep reading the old one.
write_jaas() {
    cat > "$OUT/broker-jaas.conf" <<'JAAS'
KafkaServer {
    org.apache.kafka.common.security.plain.PlainLoginModule required
        user_alice="alice-secret"
        user_quoted="kafkakn\"quote\\slash";
    org.apache.kafka.common.security.scram.ScramLoginModule required;
};
JAAS
    chmod 644 "$OUT/broker-jaas.conf"
    cat > "$OUT/client-sasl-oauthbearer.properties" <<'PROPS'
security.protocol=SASL_PLAINTEXT
sasl.mechanism=OAUTHBEARER
sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required unsecuredLoginStringClaim_sub="alice";
PROPS
    chmod 644 "$OUT/client-sasl-oauthbearer.properties"
}

# IDEMPOTENT, and that is a correctness property rather than a speed one. The broker loads its
# keystore once, at startup; regenerating the certificates under a running broker leaves it holding
# a certificate no client trusts any more, and the symptom is an SSL handshake failure that looks
# exactly like a misconfigured client. Measured here on 2026-09-17, and it cost a run.
#
# FORCE=1 regenerates - and then the broker has to be recreated, not merely restarted into the same
# container, which is what `broker.sh down` followed by `up` does.
if [ -z "${FORCE:-}" ] && [ -s "$OUT/ca.pem" ] && [ -s "$OUT/broker.keystore.p12" ] \
        && [ -s "$OUT/wrong-ca.pem" ] && [ -s "$OUT/client.pem" ] && [ -s "$OUT/wrong-client.pem" ] \
        && openssl x509 -in "$OUT/broker.pem" -noout -checkend 86400 >/dev/null 2>&1; then
    echo "  certificates already in $OUT, and the broker's is valid for another day - kept"
    derive_pkcs1
    write_jaas
    exit 0
fi

rm -rf "$OUT"
mkdir -p "$OUT"
cd "$OUT"

# The CA the client is told to trust.
openssl req -x509 -newkey rsa:2048 -nodes -days "$DAYS" \
    -keyout ca.key -out ca.pem -subj "/CN=kafkakn-test-ca" 2>/dev/null

# The broker's certificate, signed by it. The SAN matters: the clients connect to 127.0.0.1 and
# hostname verification is ON - which is the point, so it is not disabled to make this easier.
openssl req -newkey rsa:2048 -nodes \
    -keyout broker.key -out broker.csr -subj "/CN=localhost" 2>/dev/null
openssl x509 -req -in broker.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
    -days "$DAYS" -out broker.pem \
    -extfile <(printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n') \
    2>/dev/null

# What the broker loads: a PKCS12 keystore, and NOT the PEM the clients read.
#
# PEM was tried first and the broker refused it: `SSL key store password cannot be specified with
# PEM format, only key password may be specified`. The image gives no way to avoid specifying one -
# its configure script `ensure`s KAFKA_SSL_KEYSTORE_CREDENTIALS exists the moment an SSL listener is
# advertised and exports its contents as the keystore password, exiting on `${!1}: unbound variable`
# without it. So the server side uses the one format that takes a password, and the two clients keep
# their PEM CA, which is the half this project actually has to get right.
openssl pkcs12 -export -in broker.pem -inkey broker.key -certfile ca.pem -name broker \
    -out broker.keystore.p12 -passout "pass:$KEY_PASSWORD" 2>/dev/null
printf '%s' "$KEY_PASSWORD" > key-password
printf '%s' "$KEY_PASSWORD" > keystore-password

# A second, unrelated authority. It signed nothing here, so a client told to trust only this one must
# refuse the broker - and must say that it was the certificate it refused.
openssl req -x509 -newkey rsa:2048 -nodes -days "$DAYS" \
    -keyout wrong-ca.key -out wrong-ca.pem -subj "/CN=kafkakn-wrong-ca" 2>/dev/null

# CLIENT certificates, for the listener that requires one (B-31). Two again, for the same reason as
# the two authorities: one signed by the CA the broker trusts, and one signed by the authority that
# signed nothing - a certificate the broker must refuse even though it is a perfectly good one.
#
# The right client's key is ENCRYPTED, and as PKCS#8. Encrypted so that `ssl.key.password` is
# exercised rather than merely accepted: an unencrypted key connects whatever the password says.
# PKCS#8 because that is the only form `kafka-clients` 4.3.1 parses - its PEM key store reads
# `PRIVATE KEY` / `ENCRYPTED PRIVATE KEY` through `PKCS8EncodedKeySpec` and `EncryptedPrivateKeyInfo`
# (DefaultSslEngineFactory.PemStore), while librdkafka hands the file to OpenSSL and reads either.
client_cert() { # <name> <ca>
    openssl req -newkey rsa:2048 -nodes -keyout "$1.plain.key" -out "$1.csr" \
        -subj "/CN=kafkakn-$1" 2>/dev/null
    openssl x509 -req -in "$1.csr" -CA "$2.pem" -CAkey "$2.key" -CAcreateserial -days "$DAYS" \
        -out "$1.pem" -extfile <(printf 'extendedKeyUsage=clientAuth\n') 2>/dev/null
}
client_cert client ca
openssl pkcs8 -topk8 -in client.plain.key -out client.key -v2 aes-256-cbc \
    -passout "pass:$CLIENT_KEY_PASSWORD" 2>/dev/null
client_cert wrong-client wrong-ca
mv wrong-client.plain.key wrong-client.key
rm -f client.plain.key ./*.csr

# The broker container does not run as root and has to be able to read these.
chmod 644 ./*.pem key-password keystore-password

# A fixture that produced nothing must not look like one that worked.
for f in ca.pem broker.keystore.p12 wrong-ca.pem client.pem client.key wrong-client.pem wrong-client.key; do
    [ -s "$f" ] || { echo "certs.sh produced no $f" >&2; exit 1; }
done

# The two authorities must genuinely be different, or every assertion below them is vacuous.
if [ "$(openssl x509 -in ca.pem -noout -fingerprint)" = "$(openssl x509 -in wrong-ca.pem -noout -fingerprint)" ]; then
    echo "the two CAs are the same certificate - the negative test would pass for the wrong reason" >&2
    exit 1
fi

# And the wrong one must actually fail to verify the broker, checked here rather than assumed.
openssl verify -CAfile ca.pem broker.pem >/dev/null || {
    echo "the right CA does not verify the broker certificate" >&2; exit 1; }
if openssl verify -CAfile wrong-ca.pem broker.pem >/dev/null 2>&1; then
    echo "the wrong CA verifies the broker certificate - it is not a wrong CA" >&2
    exit 1
fi

# The same two questions for the client certificates: the broker's authority verifies the right
# one and not the wrong one, and the right key really is encrypted - a key that opens without its
# password would let the suite pass with `ssl.key.password` silently dropped.
openssl verify -CAfile ca.pem client.pem >/dev/null || {
    echo "the CA does not verify the client certificate" >&2; exit 1; }
if openssl verify -CAfile ca.pem wrong-client.pem >/dev/null 2>&1; then
    echo "the broker's CA verifies the wrong client certificate - it is not a wrong one" >&2
    exit 1
fi
grep -q 'BEGIN ENCRYPTED PRIVATE KEY' client.key || {
    echo "the client key is not encrypted PKCS#8 - ssl.key.password would go unexercised" >&2; exit 1; }
if openssl pkey -in client.key -passin pass: -noout 2>/dev/null; then
    echo "the client key opens without its password" >&2; exit 1
fi

# The broker's own tools take a PEM key store as ONE file holding the chain and the key - which is
# exactly the shape the contract does not ask a caller for, and why the JVM arm passes contents.
cat client.pem client.key > client-bundle.pem
cat wrong-client.pem wrong-client.key > wrong-client-bundle.pem

# SASL (B-32). The broker's users live in a JAAS file because the image insists on one: its configure
# script `ensure`s KAFKA_OPTS the moment a SASL listener is advertised, and the one thing worth putting
# there is `java.security.auth.login.config`. One KafkaServer section carries both login modules - the
# PLAIN users are listed here, the SCRAM credentials are created through the broker's own tools once
# it is up (`broker.sh up`), because SCRAM keeps them in the metadata log and not in a file.
#
# `quoted` is the user the item exists for: a password with a double quote and a backslash, which a
# JAAS string must escape. Written here ESCAPED, because this file is parsed too - and the proof it
# was escaped right is the native arm, which sends the password raw and still has to get in.

# Client configurations for the broker's OWN tools, so the fixture can be questioned without
# involving kafkakn at all. The paths are the container's, because that is where they are read.
cat > client-ssl.properties <<PROPS
security.protocol=SSL
ssl.truststore.type=PEM
ssl.truststore.location=/etc/kafka/secrets/ca.pem
PROPS
cat > client-ssl-wrong-ca.properties <<PROPS
security.protocol=SSL
ssl.truststore.type=PEM
ssl.truststore.location=/etc/kafka/secrets/wrong-ca.pem
PROPS
cat > client-mtls.properties <<PROPS
security.protocol=SSL
ssl.truststore.type=PEM
ssl.truststore.location=/etc/kafka/secrets/ca.pem
ssl.keystore.type=PEM
ssl.keystore.location=/etc/kafka/secrets/client-bundle.pem
ssl.key.password=$CLIENT_KEY_PASSWORD
PROPS
cat > client-mtls-wrong.properties <<PROPS
security.protocol=SSL
ssl.truststore.type=PEM
ssl.truststore.location=/etc/kafka/secrets/ca.pem
ssl.keystore.type=PEM
ssl.keystore.location=/etc/kafka/secrets/wrong-client-bundle.pem
PROPS
sasl_props() { # <file> <protocol> <mechanism> <user> <password, JAAS-escaped>
    local module=org.apache.kafka.common.security.scram.ScramLoginModule
    [ "$3" = PLAIN ] && module=org.apache.kafka.common.security.plain.PlainLoginModule
    {
        echo "security.protocol=$2"
        echo "sasl.mechanism=$3"
        echo "sasl.jaas.config=$module required username=\"$4\" password=\"$5\";"
        if [ "$2" = SASL_SSL ]; then
            echo "ssl.truststore.type=PEM"
            echo "ssl.truststore.location=/etc/kafka/secrets/ca.pem"
        fi
    } > "$1"
}
sasl_props client-sasl-plain.properties SASL_PLAINTEXT PLAIN alice alice-secret
sasl_props client-sasl-plain-wrong.properties SASL_PLAINTEXT PLAIN alice not-the-password
sasl_props client-sasl-scram256.properties SASL_PLAINTEXT SCRAM-SHA-256 alice alice-secret
sasl_props client-sasl-scram256-wrong.properties SASL_PLAINTEXT SCRAM-SHA-256 alice not-the-password
sasl_props client-sasl-ssl-scram512.properties SASL_SSL SCRAM-SHA-512 alice alice-secret
chmod 644 ./*.properties ./*.pem ./*.conf client.key wrong-client.key

derive_pkcs1
write_jaas
echo "  certificates in $OUT: ca.pem, broker.keystore.p12, wrong-ca.pem, client.pem, wrong-client.pem"
echo "  the right CA verifies the broker, the wrong CA does not - both checked, not assumed"
