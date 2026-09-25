#!/usr/bin/env bash
# B-47: a null value is a tombstone, and an empty value is not, on both arms, as the broker stores them.
#
# The oracle is the distribution's own client (ci/harness/Records.java), which prints a null value as `~`
# and bytes as `x<hex>`. The empty value is exactly `x`. Two questions:
#   1. what each arm put on the wire: each key's LAST record, `~` for `-gone` and `x` for `-kept`;
#   2. what compaction does with it: the earlier value of both keys is removed, `-gone` keeps only its
#      tombstone, and `-kept` keeps its empty value. That is the difference a compacted topic acts on.
#
#   ci/b-47/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations
TOPIC=kafkakn-compact
fail=0
bad() { echo "    $*" >&2; fail=1; }
hex() { printf '%s' "$1" | od -An -tx1 | tr -d ' \n'; }

echo "=== environment ==="
date -Is

echo
echo "=== broker, and the compacted fixture ==="
bash "$H" up || exit 1
bash "$H" records dump "$TOPIC" > /dev/null || { echo "  cannot read $TOPIC" >&2; exit 1; }

echo
echo "=== both arms send ==="
rm -rf "$OBS"
for task in jvmTest linuxX64Test; do
    ./gradlew --console=plain ":kafkakn-core:$task" --rerun --tests '*TombstoneTest*' > "build/b-47-$task.out" 2>&1
    code=$?
    printf '  %-14s exit=%s\n' "$task" "$code"
    [ "$code" -eq 0 ] || { tail -30 "build/b-47-$task.out"; echo "  THE SUITE FAILED on $task"; exit 1; }
done
# One observation, by design: what matters here is the broker's, below. One is still not zero, so an
# arm whose test did not run still fails the comparison.
MIN_OBSERVATIONS=1 bash ci/harness/compare-arms.sh "$OBS/jvm.txt" "$OBS/linuxX64.txt" || exit 1

# The values of one key, in offset order, as the dump prints them.
values() { awk -F/ -v k="x$(hex "$2")" '$3 == k { print $4 }' "$1"; }

echo
echo "=== 1. what each arm put on the wire: each key's last record ==="
bash "$H" records dump "$TOPIC" > build/b-47-sent.dump
BEFORE=x$(hex before)
for arm in jvm linuxX64; do
    stamp=$(sed -n 's/^tombstone.stamp=//p' "$OBS/$arm-local.txt" | tail -1)
    [ -n "$stamp" ] || { echo "  no stamp from $arm - its test did not run" >&2; exit 1; }
    gone=$(values build/b-47-sent.dump "$stamp-gone" | tail -1)
    kept=$(values build/b-47-sent.dump "$stamp-kept" | tail -1)
    printf '  %-9s null value -> %-4s empty value -> %s\n' "$arm" "${gone:-nothing}" "${kept:-nothing}"
    [ "$gone" = "~" ] || bad "$arm: the null value was stored as '$gone', not as a null"
    [ "$kept" = "x" ] || bad "$arm: the empty value was stored as '$kept', not as zero bytes"
    STAMPS="${STAMPS:-} $stamp"
done
[ "$fail" -eq 0 ] || { echo "B-47: RED"; exit 1; }

echo
echo "=== 2. what compaction leaves ==="
# The cleaner only takes a segment that is no longer active, and a segment rolls when a record arrives
# after segment.ms. So one more record, a second later, puts everything above into a cleanable segment.
# It needs a KEY: the compacted topic refuses a keyless record, and the first version of this script sent
# one, lost it silently, and waited three minutes for a cleaner that had nothing it was allowed to clean.
sleep 2
end_before=$(bash "$H" offsets "$TOPIC")
echo "roll-$(date +%s):roll" | bash "$H" produce-keyed "$TOPIC" >/dev/null || { echo "  the rolling record was refused" >&2; exit 1; }
[ "$(bash "$H" offsets "$TOPIC")" -gt "$end_before" ] || { echo "  the rolling record did not land" >&2; exit 1; }
compacted=
for i in $(seq 1 36); do
    bash "$H" records dump "$TOPIC" > build/b-47-compacted.dump
    left=0
    for stamp in $STAMPS; do
        for key in "$stamp-gone" "$stamp-kept"; do
            values build/b-47-compacted.dump "$key" | grep -qx "$BEFORE" && left=$((left + 1))
        done
    done
    [ "$left" -eq 0 ] && { compacted=yes; echo "  the cleaner ran: no earlier value is left (after ~$((i * 5)) s)"; break; }
    sleep 5
done
[ -n "$compacted" ] || { echo "  the cleaner did not run within three minutes - nothing below would mean anything" >&2; exit 1; }
for arm in jvm linuxX64; do
    stamp=$(sed -n 's/^tombstone.stamp=//p' "$OBS/$arm-local.txt" | tail -1)
    gone=$(values build/b-47-compacted.dump "$stamp-gone" | tr '\n' ' ')
    kept=$(values build/b-47-compacted.dump "$stamp-kept" | tr '\n' ' ')
    printf '  %-9s the tombstoned key holds: %-6s the emptied key holds: %s\n' "$arm" "${gone:-nothing}" "${kept:-nothing}"
    # The tombstone itself stays until delete.retention.ms (a day, by default), so it is either there or
    # not; what must not be there is a value.
    [ "$gone" = "~ " ] || [ -z "$gone" ] || bad "$arm: the tombstoned key still has a value: $gone"
    [ "$kept" = "x " ] || bad "$arm: the emptied key should keep its empty value, and holds: ${kept:-nothing}"
done

echo
[ "$fail" -eq 0 ] || { echo "B-47: RED"; exit 1; }
echo "B-47: a null value is a tombstone on both arms, an empty value is a value, and compaction treats them so"
