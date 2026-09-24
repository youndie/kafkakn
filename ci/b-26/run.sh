#!/usr/bin/env bash
# B-26: compression.type is named portable in the contract. Is anything actually compressed?
#
# A record that arrives proves nothing about compression: an uncompressed batch arrives too and reads
# back byte for byte. The third party here is the broker's own stored log segment, read with
# kafka-dump-log.sh, which prints the codec of every batch and the payload of every record in it. Each
# record CompressionTest sent carries a stamp naming the arm and the codec it was sent with, so every
# record can be matched to the batch it landed in.
#
# THE CONTROL is the `none` row, and the check is that five rows come back with FIVE DIFFERENT codecs.
# A reader that reported the codec it was told to expect would make every row agree with its label;
# it cannot make five labels produce five distinct values on disk.
#
#   ci/b-26/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
C=kafkakn-broker
OBS=kafkakn-core/build/observations
TOPIC=kafkakn-comp-$(date +%s)
CODECS="none gzip snappy lz4 zstd"
ARMS="jvm linuxX64"
export KAFKAKN_BOOTSTRAP=127.0.0.1:9092 KAFKAKN_TOPIC=$TOPIC

echo "=== environment ==="
date -Is
echo "  topic $TOPIC, fresh, so every batch on it is this run's"

echo
echo "=== broker ==="
bash "$H" up || exit 1
bash "$H" topic "$TOPIC" | head -1

echo
echo "=== the sends, both arms ==="
rm -rf "$OBS"
# ONE TASK PER INVOCATION. With two test tasks on one command line Gradle applied `--tests` to one of
# them and ran the whole suite on the other - the first run of this script failed on AccountingTest,
# which refuses to run without the topic its own runner creates (B-24). A filter that silently covers
# half of what it was given is worse than no filter: the other half fails for someone else's reason.
for task in jvmTest linuxX64Test; do
    ./gradlew --console=plain ":kafkakn-core:$task" --tests '*CompressionTest*' 2>&1 | tail -1
    [ "${PIPESTATUS[0]}" -eq 0 ] || { echo "  THE SENDS FAILED on $task"; exit 1; }
done

echo
echo "=== what the broker stored, read by the broker's own tool ==="
DUMP=build/b-26.dump
: > "$DUMP"
for dir in $(docker exec "$C" sh -c "ls -d /tmp/kafka-logs/$TOPIC-*"); do
    for seg in $(docker exec "$C" sh -c "ls $dir/*.log"); do
        docker exec "$C" /opt/kafka/bin/kafka-dump-log.sh --files "$seg" --print-data-log 2>/dev/null >> "$DUMP"
    done
done
[ -s "$DUMP" ] || { echo "  the dump is empty - nothing to read, which is not a pass" >&2; exit 1; }
echo "  $(grep -c '^baseOffset' "$DUMP") batches, $(grep -c '^| offset' "$DUMP") records"

# For every record, the codec of the batch it sits in: "stamp codec" per line.
python3 - "$DUMP" > build/b-26.records <<'PY'
import re, sys
codec = None
for line in open(sys.argv[1], errors="replace"):
    m = re.search(r"compresscodec: (\w+)", line)
    if line.startswith("baseOffset") and m:
        codec = m.group(1).lower()
        continue
    m = re.search(r"payload: (comp-[^:]+):", line)
    if line.startswith("| offset") and m:
        print(m.group(1), codec)
PY

echo
echo "=== each arm, each codec: how many records, and which codec their batches carry ==="
fail=0
for arm in $ARMS; do
    facts="$OBS/$arm-local.txt"
    [ -s "$facts" ] || { echo "  no facts from $arm - its tests did not run" >&2; exit 1; }
    for codec in $CODECS; do
        stamp=$(sed -n "s/^compression.$codec.stamp=//p" "$facts" | tail -1)
        want=$(sed -n "s/^compression.$codec.count=//p" "$facts" | tail -1)
        found=$(awk -v s="$stamp" '$1 == s' build/b-26.records | wc -l)
        codecs=$(awk -v s="$stamp" '$1 == s { print $2 }' build/b-26.records | sort -u | tr '\n' ' ')
        printf '  %-9s %-7s records %4s of %-4s  stored as: %s\n' "$arm" "$codec" "$found" "$want" "$codecs"
        [ "$found" = "$want" ] || { echo "    $arm/$codec: $found of $want records found in the segments" >&2; fail=1; }
        [ "$codecs" = "$codec " ] || { echo "    $arm/$codec: stored as '$codecs', not '$codec'" >&2; fail=1; }
    done
done

echo
echo "=== the control: five labels, five different codecs on disk, per arm ==="
for arm in $ARMS; do
    distinct=$(for codec in $CODECS; do
        stamp=$(sed -n "s/^compression.$codec.stamp=//p" "$OBS/$arm-local.txt" | tail -1)
        awk -v s="$stamp" '$1 == s { print $2 }' build/b-26.records | sort -u
    done | sort -u | wc -l)
    printf '  %-9s %s distinct codecs\n' "$arm" "$distinct"
    [ "$distinct" -eq 5 ] || { echo "    $arm: $distinct distinct codecs for five labels - the check cannot tell them apart" >&2; fail=1; }
done

echo
echo "=== and the values read back, byte for byte, by the broker's own consumer ==="
bash "$H" values "$TOPIC" | sort > build/b-26.values
for arm in $ARMS; do
    for codec in $CODECS; do
        stamp=$(sed -n "s/^compression.$codec.stamp=//p" "$OBS/$arm-local.txt" | tail -1)
        want=$(sed -n "s/^compression.$codec.count=//p" "$OBS/$arm-local.txt" | tail -1)
        exact=$(grep -c "^$stamp:[0-9]*:$(printf 'kafkakn%.0s' $(seq 1 40))\$" build/b-26.values)
        [ "$exact" = "$want" ] || { echo "  $arm/$codec: $exact of $want values came back intact" >&2; fail=1; }
    done
done
[ "$fail" -eq 0 ] && echo "  every value intact"

echo
echo "=== what each arm says about a codec that does not exist ==="
for arm in $ARMS; do
    printf '  %-9s %s\n' "$arm" "$(sed -n 's/^compression.refusal=//p' "$OBS/$arm-local.txt" | tail -1 | cut -c1-160)"
done

echo
[ "$fail" -eq 0 ] || { echo "B-26: RED"; exit 1; }
echo "B-26: every codec, on both arms, is what the broker stored - and none is what none looks like"
