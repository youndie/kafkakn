#!/usr/bin/env bash
# B-36: assign, seek and poll on both arms, against a third party on both ends.
#
# The records were written by the Kafka distribution's own client (ci/harness/Records.java, through
# `broker.sh up`), and the same tool reads them back as hex: the oracle. Both arms' readings are
# compared with it AND with each other, byte for byte, and a difference is a failure. Where each seek
# should land is asked of kafka-get-offsets.sh.
#
# Not kafka-console-producer/consumer: they carry text, and a value that is not UTF-8 - the case a
# byte-typed consumer exists for - comes back from them as U+FFFD.
#
#   ci/b-36/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations
TOPIC=kafkakn-consume
SEEK_TIMESTAMP=1700000005500
export KAFKAKN_BOOTSTRAP=127.0.0.1:9092 KAFKAKN_TOPIC=kafkakn-b36-$(date +%s)
kc() { docker exec kafkakn-broker "$@"; }
offset_at() { kc /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server 127.0.0.1:9092 --topic "$TOPIC" --time "$1" 2>/dev/null | cut -d: -f3; }

echo "=== environment ==="
date -Is

echo
echo "=== broker, and the oracle ==="
bash "$H" up || exit 1
bash "$H" topic "$KAFKAKN_TOPIC" >/dev/null
bash "$H" records dump "$TOPIC" > build/b-36-oracle.txt
echo "  the distribution's own client reads $(wc -l < build/b-36-oracle.txt) records from $TOPIC"
EARLIEST=$(offset_at -2)
LATEST=$(offset_at -1)
AT_TIME=$(offset_at "$SEEK_TIMESTAMP")
echo "  kafka-get-offsets.sh: earliest $EARLIEST, latest $LATEST, at $SEEK_TIMESTAMP -> $AT_TIME"
[ -n "$EARLIEST" ] && [ -n "$AT_TIME" ] && [ "$(wc -l < build/b-36-oracle.txt)" -gt 0 ] || { echo "  the oracle is empty" >&2; exit 1; }

echo
echo "=== both arms, one test task per invocation, rerun every time ==="
rm -rf "$OBS"
for task in jvmTest linuxX64Test; do
    ./gradlew --console=plain ":kafkakn-core:$task" --rerun --tests '*ConsumerTest*' > "build/b-36-$task.out" 2>&1
    code=$?
    printf '  %-14s exit=%s\n' "$task" "$code"
    [ "$code" -eq 0 ] || { tail -30 "build/b-36-$task.out"; echo "  THE SUITE FAILED on $task"; exit 1; }
done

echo
echo "=== what each arm read, against the oracle and against each other ==="
fail=0
for arm in jvm linuxX64; do
    sed -n 's/^consume\.dump=//p' "$OBS/$arm-local.txt" | tail -1 | tr ';' '\n' > "build/b-36-$arm.txt"
    [ -s "build/b-36-$arm.txt" ] || { echo "  $arm recorded no reading" >&2; exit 1; }
    if diff -q build/b-36-oracle.txt "build/b-36-$arm.txt" >/dev/null; then
        printf '  %-9s %s records, byte for byte the oracle'"'"'s\n' "$arm" "$(wc -l < "build/b-36-$arm.txt")"
    else
        echo "  $arm DIFFERS from the oracle:" >&2
        diff build/b-36-oracle.txt "build/b-36-$arm.txt" | head -10 >&2
        fail=1
    fi
done
diff -q build/b-36-jvm.txt build/b-36-linuxX64.txt >/dev/null && echo "  jvm and linuxX64 read the same bytes" \
    || { echo "  THE ARMS DIFFER" >&2; fail=1; }
echo "  the records the fixture exists for, as both read them:"
sed -n '2,6p' build/b-36-jvm.txt | sed 's/^/    /'

echo
echo "=== where each seek landed, against kafka-get-offsets.sh ==="
for arm in jvm linuxX64; do
    f="$OBS/$arm-local.txt"
    for pair in beginning:$EARLIEST offset:7 timestamp:$AT_TIME; do
        name=${pair%%:*}
        want=${pair#*:}
        got=$(sed -n "s/^consume\.seek\.$name=//p" "$f" | tail -1)
        if [ "$got" = "$want" ]; then
            printf '  %-9s %-10s %s\n' "$arm" "$name" "$got"
        else
            printf '  %-9s %-10s %s  EXPECTED %s\n' "$arm" "$name" "${got:-nothing}" "$want" >&2
            fail=1
        fi
    done
    printf '  %-9s %-10s read %s after it (latest is %s)\n' "$arm" end "$(sed -n 's/^consume\.seek\.end\.read=//p' "$f" | tail -1)" "$LATEST"
done

echo
echo "=== the native arm's private group.id never reached the cluster as a group ==="
GROUPS=$(kc /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --list 2>/dev/null)
if echo "$GROUPS" | grep -q '^kafkakn-assign-'; then
    echo "  the cluster lists a kafkakn-assign group:" >&2
    echo "$GROUPS" | grep '^kafkakn-assign-' | head -3 >&2
    fail=1
else
    echo "  kafka-consumer-groups.sh --list: no kafkakn-assign-* group ($(echo "$GROUPS" | grep -c .) groups in all)"
fi

echo
echo "=== H7, recorded ==="
for arm in jvm linuxX64; do
    f="$OBS/$arm-local.txt"
    printf '  %-9s a cancelled poll returned in %s ms; overlapping callers read %s records, none twice\n' "$arm" \
        "$(sed -n 's/^consume\.cancel\.ms=//p' "$f" | tail -1)" "$(sed -n 's/^consume\.overlap\.read=//p' "$f" | tail -1)"
done

echo
[ "$fail" -eq 0 ] || { echo "B-36: RED"; exit 1; }
echo "B-36: both arms read the third party's records byte for byte and in order, and seek where the broker says"
