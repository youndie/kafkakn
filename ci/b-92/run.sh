#!/usr/bin/env bash
# B-92: the two things that can only be measured.
#   - HTTP and HTTPS from a Kotlin/Native binary through Ktor's CIO client: does it answer, how big is the binary, and
#     does its ldd stay the one a kafkakn binary has (research §1.2: a statically linked C bundle, nothing added)?
#   - The registry fixture: cp-schema-registry beside the broker, on the host network because the broker advertises
#     127.0.0.1, on a port of its own on this shared box.
#
#   ci/b-92/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
export GRADLE_OPTS=-Dorg.gradle.daemon=false
REGISTRY_IMAGE=confluentinc/cp-schema-registry:8.3.2
REGISTRY=http://127.0.0.1:18081

echo "=== environment ==="
date -Is
bash ci/harness/broker.sh up > /dev/null || exit 1

echo
echo "=== the registry fixture ==="
docker rm -f kafkakn-registry > /dev/null 2>&1
docker run -d --name kafkakn-registry --network host \
    -e SCHEMA_REGISTRY_HOST_NAME=127.0.0.1 \
    -e SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:18081 \
    -e SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=PLAINTEXT://127.0.0.1:9092 \
    -e SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR=1 \
    "$REGISTRY_IMAGE" > /dev/null || { echo "  could not start $REGISTRY_IMAGE" >&2; exit 1; }
started=$(date +%s)
until curl -sf "$REGISTRY/subjects" > /dev/null; do
    [ $(( $(date +%s) - started )) -lt 90 ] || { echo "  no answer from $REGISTRY" >&2; docker logs kafkakn-registry 2>&1 | tail -5; exit 1; }
    sleep 1
done
echo "  $REGISTRY_IMAGE answers on $REGISTRY after $(( $(date +%s) - started )) s: /subjects = $(curl -s "$REGISTRY/subjects")"
echo "  version: $(curl -s "$REGISTRY/v1/metadata/version" 2>/dev/null)"

echo
echo "=== HTTP and HTTPS from Kotlin/Native, through Ktor's CIO client ==="
./gradlew --console=plain -p ci/b-92/probe linkReleaseExecutableLinuxX64 > build/b-92-probe.out 2>&1 \
    || { tail -15 build/b-92-probe.out; exit 1; }
KEXE=ci/b-92/probe/build/bin/linuxX64/releaseExecutable/kafkakn-http-probe.kexe
"$KEXE" "$REGISTRY/subjects" "https://repo1.maven.org/maven2/" "https://self-signed.badssl.com/" "https://expired.badssl.com/" 2>&1 | sed 's/^/  /'
printf '  binary: %s bytes\n' "$(stat -c %s "$KEXE")"
echo "  ldd:"; ldd "$KEXE" | awk '{ print "    " $1 }'
SOAK=kafkakn-soak/build/bin/linuxX64/releaseExecutable/kafkakn-soak.kexe
if [ -x "$SOAK" ]; then echo "  a kafkakn binary's ldd, for comparison:"; ldd "$SOAK" | awk '{ print "    " $1 }'; fi
