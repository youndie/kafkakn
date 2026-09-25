#!/usr/bin/env bash
# B-40: the native suite on a contributor's Mac, against the same broker, and the differential against
# the JVM arm - the loop a contributor needs, measured.
#
# Runs ON THE MAC. The broker, its certificates and the JVM arm stay on the Linux box, reached over SSH:
#
#   KAFKAKN_BROKER_SSH="-p 2222 you@127.0.0.1" KAFKAKN_BROKER_DIR=kafkakn bash ci/b-40/run.sh
#
# The listeners advertise 127.0.0.1, so the Mac reaches them through a tunnel on the same ports rather
# than through the box's address - the broker's metadata would send a client to 127.0.0.1 anyway.
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
SSH_ARGS=${KAFKAKN_BROKER_SSH:?set KAFKAKN_BROKER_SSH to the ssh arguments that reach the broker box}
REMOTE_DIR=${KAFKAKN_BROKER_DIR:-kafkakn}
OBS=kafkakn-core/build/observations
RESULTS=kafkakn-core/build/test-results/macosArm64Test
ACCOUNTING=kafkakn-acct-b40-$(date +%s)
# shellcheck disable=SC2086
remote() { ssh -o BatchMode=yes $SSH_ARGS "cd $REMOTE_DIR && $*"; }

echo "=== environment ==="
date "+%Y-%m-%dT%H:%M:%S%z"
echo "  $(sw_vers -productName) $(sw_vers -productVersion), $(uname -m); bundle: $(ls -d "$HOME"/.cache/kafkakn/librdkafka-*-macosArm64 2>/dev/null)"
[ "$(uname -s)-$(uname -m)" = "Darwin-arm64" ] || { echo "run this on an arm64 Mac" >&2; exit 1; }

echo
echo "=== the broker, on the Linux box ==="
remote "bash ci/harness/broker.sh up" || exit 1
remote "bash ci/harness/broker.sh topic kafkakn >/dev/null; bash ci/harness/broker.sh strict-topic kafkakn-strict >/dev/null; \
    for arm in jvm macosArm64; do bash ci/harness/broker.sh topic $ACCOUNTING-\$arm >/dev/null; done; echo '  topics ready'"

echo
echo "=== the fixture's certificates, copied - the TLS scenarios read them from \$HOME/.cache/kafkakn/tls ==="
mkdir -p "$HOME/.cache/kafkakn/tls"
# shellcheck disable=SC2086
ssh -o BatchMode=yes $SSH_ARGS "tar -C \$HOME/.cache/kafkakn/tls -cf - ." | tar -C "$HOME/.cache/kafkakn/tls" -xf -
echo "  $(ls "$HOME/.cache/kafkakn/tls" | wc -l | tr -d ' ') files"

echo
echo "=== a tunnel to the listeners ==="
PORTS="9092 9094 9095 9096 9097"
FORWARDS=$(for p in $PORTS; do printf -- '-L %s:127.0.0.1:%s ' "$p" "$p"; done)
# shellcheck disable=SC2086
ssh -o BatchMode=yes -o ExitOnForwardFailure=yes -f -N $FORWARDS $SSH_ARGS || { echo "  the tunnel did not open" >&2; exit 1; }
TUNNEL=$(pgrep -f -- "-L 9092:127.0.0.1:9092" | head -1)
trap '[ -n "$TUNNEL" ] && kill "$TUNNEL" 2>/dev/null' EXIT
echo "  ports $PORTS forwarded (ssh pid $TUNNEL)"

echo
echo "=== the native suite, on the Mac ==="
rm -rf "$OBS" "$RESULTS"
KAFKAKN_BOOTSTRAP=127.0.0.1:9092 KAFKAKN_TOPIC=kafkakn KAFKAKN_STRICT_TOPIC=kafkakn-strict \
    KAFKAKN_SSL_BOOTSTRAP=127.0.0.1:9094 KAFKAKN_ACCOUNTING_TOPIC=$ACCOUNTING \
    LOCAL=1 ./gradlew --console=plain :kafkakn-core:macosArm64Test --rerun > build/b-40-macos.out 2>&1
code=$?
t=$(grep -ho 'tests="[0-9]*"' "$RESULTS"/TEST-*.xml 2>/dev/null | grep -oE '[0-9]+' | paste -sd+ - | bc)
f=$(grep -ho 'failures="[0-9]*"' "$RESULTS"/TEST-*.xml 2>/dev/null | grep -oE '[0-9]+' | paste -sd+ - | bc)
printf '  macosArm64Test exit=%s tests=%s failures=%s\n' "$code" "${t:-0}" "${f:-?}"
[ "$code" -eq 0 ] || { grep -E "FAILED|^e:" build/b-40-macos.out | head -20; echo "  THE NATIVE SUITE FAILED ON THE MAC"; exit 1; }

echo
echo "=== the JVM arm, on the Linux box, against the same broker ==="
remote "rm -rf $OBS; KAFKAKN_BOOTSTRAP=127.0.0.1:9092 KAFKAKN_TOPIC=kafkakn KAFKAKN_STRICT_TOPIC=kafkakn-strict \
    KAFKAKN_SSL_BOOTSTRAP=127.0.0.1:9094 KAFKAKN_ACCOUNTING_TOPIC=$ACCOUNTING \
    ./gradlew --console=plain :kafkakn-core:jvmTest --rerun > \$HOME/b40-jvm.out 2>&1; echo \"  jvmTest exit=\$?\""
# shellcheck disable=SC2086
ssh -o BatchMode=yes $SSH_ARGS "cat $REMOTE_DIR/$OBS/jvm.txt" > build/b-40-jvm.txt

echo
echo "=== the arms still agree on what only a client knows ==="
bash ci/harness/compare-arms.sh build/b-40-jvm.txt "$OBS/macosArm64.txt" || exit 1

echo
echo "B-40: the native suite passes on a Mac against the broker, and agrees with the JVM arm"
