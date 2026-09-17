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

# IDEMPOTENT, and that is a correctness property rather than a speed one. The broker loads its
# keystore once, at startup; regenerating the certificates under a running broker leaves it holding
# a certificate no client trusts any more, and the symptom is an SSL handshake failure that looks
# exactly like a misconfigured client. Measured here on 2026-09-17, and it cost a run.
#
# FORCE=1 regenerates - and then the broker has to be recreated, not merely restarted into the same
# container, which is what `broker.sh down` followed by `up` does.
if [ -z "${FORCE:-}" ] && [ -s "$OUT/ca.pem" ] && [ -s "$OUT/broker.keystore.p12" ] \
        && [ -s "$OUT/wrong-ca.pem" ] && openssl x509 -in "$OUT/broker.pem" -noout -checkend 86400 >/dev/null 2>&1; then
    echo "  certificates already in $OUT, and the broker's is valid for another day - kept"
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

# The broker container does not run as root and has to be able to read these.
chmod 644 ./*.pem key-password keystore-password

# A fixture that produced nothing must not look like one that worked.
for f in ca.pem broker.keystore.p12 wrong-ca.pem; do
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
chmod 644 ./*.properties

echo "  certificates in $OUT: ca.pem, broker.keystore.p12, wrong-ca.pem"
echo "  the right CA verifies the broker, the wrong CA does not - both checked, not assumed"
