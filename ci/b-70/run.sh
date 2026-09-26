#!/usr/bin/env bash
# B-70: kafkakn-soak, a service built on kafkakn, run under chaos. Four instances in one group, two native and two
# JVM, each an exactly-once read-process-write loop that is crash-only. While the input trickles, every CHAOS_EVERY
# seconds one instance is killed (SIGKILL, restarted 3 s later) or frozen past its session (SIGSTOP, then SIGCONT).
# An instance that exits by itself is restarted too, and why it exited is counted.
#
# Checked at the end, with the Java client, never with kafkakn:
#   - every input record is in the output exactly once, under read_committed;
#   - the group's commits reach every input partition's end;
#   - each process's RSS, sampled every minute: start, end and peak, per process lifetime.
# The shared box is protected: each instance runs in its own systemd scope (MemoryMax, TasksMax), and a watchdog
# stops the run if MemAvailable falls below 1.5 GB.
#
#   DURATION=3600 ci/b-70/run.sh        # seconds of trickling input; the default is an hour
#   EXEMPT=n1 ci/b-70/run.sh            # B-72: keep one slot out of the chaos, and report its memory's slope
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
DURATION=${DURATION:-3600}
CHAOS_EVERY=${CHAOS_EVERY:-45}
FREEZE_FOR=${FREEZE_FOR:-20}
PARTITIONS=6
SLOTS="n1 n2 j1 j2"
EXEMPT=${EXEMPT:-}
CHAOS_SLOTS=$(for s in $SLOTS; do [ "$s" = "$EXEMPT" ] || printf '%s ' "$s"; done)
STAMP=$(date +%s)
RUN=/tmp/b70-$STAMP
INPUT=kafkakn-soak-in-$STAMP
OUTPUT=kafkakn-soak-out-$STAMP
GROUP=kafkakn-soak-$STAMP
KEXE=$ROOT/kafkakn-soak/build/bin/linuxX64/releaseExecutable/kafkakn-soak.kexe
mkdir -p "$RUN"
fail=0
bad() { echo "    $*" >&2; fail=1; }
kc() { docker exec kafkakn-broker "$@"; }
now() { date +%s; }
# Alive, and not a zombie: an instance that exited and was not waited for still answers kill -0, which is how the
# smoke run missed j2 exiting after its freeze.
alive() { kill -0 "$1" 2> /dev/null && [ "$(awk '/^State:/ { print $2 }' "/proc/$1/status" 2> /dev/null)" != Z ]; }
say() { echo "[$(date +%T)] $*"; }

echo "=== environment ==="
date -Is
echo "  run directory $RUN; input $INPUT ($PARTITIONS partitions), output $OUTPUT, group $GROUP"

echo
echo "=== broker, and the service built on both arms ==="
bash "$H" up || exit 1
export GRADLE_OPTS=-Dorg.gradle.daemon=false
./gradlew --console=plain :kafkakn-soak:linkReleaseExecutableLinuxX64 :kafkakn-soak:soakJvmLaunch > "$RUN/build.out" 2>&1 \
    || { tail -20 "$RUN/build.out"; exit 1; }
# shellcheck disable=SC1091
source kafkakn-soak/build/soak/jvm.launch
PARTITIONS=$PARTITIONS bash "$H" topic "$INPUT" > /dev/null
PARTITIONS=1 bash "$H" topic "$OUTPUT" > /dev/null

declare -A PID KILLS FREEZES EXITS
start() { # <slot>
    local slot=$1 cmd
    case "$slot" in
        n*) cmd=("$KEXE") ;;
        j*) cmd=("$JAVA" -Xmx512m -cp "$CP" io.github.youndie.kafkakn.soak.SoakMain) ;;
    esac
    # A scope of its own: this box is shared, and one runaway instance must not take other sessions' work with it.
    systemd-run --user --scope --quiet -p MemoryMax=1G -p MemorySwapMax=0 -p TasksMax=512 -- \
        env SOAK_INPUT="$INPUT" SOAK_OUTPUT="$OUTPUT" SOAK_GROUP="$GROUP" SOAK_SLOT="$slot" "${cmd[@]}" \
        >> "$RUN/$slot.log" 2>&1 &
    PID[$slot]=$!
    echo "$(now) $slot start ${PID[$slot]}" >> "$RUN/events.txt"
}
for slot in $SLOTS; do KILLS[$slot]=0; FREEZES[$slot]=0; EXITS[$slot]=0; start "$slot"; done
sleep 2
for slot in $SLOTS; do
    printf '  %s pid %s: %s\n' "$slot" "${PID[$slot]}" "$(tr '\0' ' ' < "/proc/${PID[$slot]}/cmdline" 2>/dev/null | cut -c1-80)"
done

echo
echo "=== the input, sized to the trickle's own rate for ${DURATION} s ==="
PARTITIONS=$PARTITIONS bash "$H" topic "$INPUT-rate" > /dev/null
t0=$(date +%s%3N)
bash "$H" records trickle "$INPUT-rate" "$PARTITIONS" 1000 0 > /dev/null
rate=$((1000 * 1000 / ($(date +%s%3N) - t0)))
RECORDS=$(((rate * DURATION * 9 / 10) / PARTITIONS * PARTITIONS))
echo "  trickle rate $rate records/s; $RECORDS records"
bash "$H" records trickle "$INPUT" "$PARTITIONS" "$RECORDS" 0 > "$RUN/trickle.out" 2>&1 &
TRICKLE=$!

echo
echo "=== chaos while the input trickles ==="
sample() {
    local t slot pid rss
    t=$(now)
    for slot in $SLOTS; do
        pid=${PID[$slot]}
        rss=$(awk '/^VmRSS/ { print $2 }' "/proc/$pid/status" 2>/dev/null)
        [ -n "$rss" ] && echo "$t $slot $pid $rss" >> "$RUN/rss.txt"
    done
}
aborted=
last_chaos=$(now)
last_sample=0
while kill -0 "$TRICKLE" 2> /dev/null; do
    sleep 5
    avail=$(awk '/^MemAvailable/ { print int($2 / 1024) }' /proc/meminfo)
    if [ "$avail" -lt 1536 ]; then
        aborted="MemAvailable ${avail} MB"
        say "ABORT: $aborted"
        break
    fi
    for slot in $SLOTS; do
        if ! alive "${PID[$slot]}"; then
            wait "${PID[$slot]}" 2> /dev/null
            EXITS[$slot]=$((EXITS[$slot] + 1))
            echo "$(now) $slot exited" >> "$RUN/events.txt"
            start "$slot"
        fi
    done
    if [ $(($(now) - last_sample)) -ge 60 ]; then sample; last_sample=$(now); fi
    if [ $(($(now) - last_chaos)) -ge "$CHAOS_EVERY" ]; then
        last_chaos=$(now)
        # shellcheck disable=SC2086
        set -- $CHAOS_SLOTS
        shift $((RANDOM % $#))
        slot=$1
        if [ $((RANDOM % 2)) -eq 0 ]; then
            say "kill $slot (${PID[$slot]})"
            kill -9 "${PID[$slot]}" 2> /dev/null
            wait "${PID[$slot]}" 2> /dev/null
            KILLS[$slot]=$((KILLS[$slot] + 1))
            echo "$(now) $slot killed" >> "$RUN/events.txt"
            sleep 3
            start "$slot"
        else
            say "freeze $slot (${PID[$slot]}) for ${FREEZE_FOR} s"
            kill -STOP "${PID[$slot]}" 2> /dev/null
            FREEZES[$slot]=$((FREEZES[$slot] + 1))
            echo "$(now) $slot frozen" >> "$RUN/events.txt"
            sleep "$FREEZE_FOR"
            kill -CONT "${PID[$slot]}" 2> /dev/null
        fi
    fi
done > "$RUN/chaos.txt"
tail -3 "$RUN/chaos.txt" | sed 's/^/  /'

echo
echo "=== the input done; draining without chaos until the group's lag is zero ==="
lag_left() {
    kc /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --describe --group "$GROUP" 2>/dev/null \
        | awk -v t="$INPUT" '$2 == t { if ($6 == "-") s += 1; else s += $6 } END { print s + 0 }'
}
if [ -z "$aborted" ]; then
    kill "$TRICKLE" 2> /dev/null
    for _ in $(seq 1 60); do
        for slot in $SLOTS; do
            alive "${PID[$slot]}" || { wait "${PID[$slot]}" 2> /dev/null; EXITS[$slot]=$((EXITS[$slot] + 1)); echo "$(now) $slot exited" >> "$RUN/events.txt"; start "$slot"; }
        done
        left=$(lag_left)
        [ "$left" -eq 0 ] && break
        sleep 5
    done
    sample
    echo "  lag left: $left"
fi
for slot in $SLOTS; do kill -9 "${PID[$slot]}" 2> /dev/null; done
kill "$TRICKLE" 2> /dev/null
wait 2> /dev/null

echo
echo "=== what each instance went through ==="
for slot in $SLOTS; do
    reasons=$(sed -n 's/^soak failed: //p' "$RUN/$slot.log" | sed 's/:.*//' | sort | uniq -c | sort -rn | awk '{ printf "%s %s; ", $1, $2 }')
    printf '  %-3s killed %s, frozen %s, exited by itself %s  %s\n' "$slot" "${KILLS[$slot]}" "${FREEZES[$slot]}" "${EXITS[$slot]}" "$reasons"
done

echo
echo "=== memory, per process lifetime (RSS, MB) ==="
python3 - "$RUN/rss.txt" <<'PY'
import collections, sys
lives = collections.OrderedDict()
for line in open(sys.argv[1]):
    t, slot, pid, rss = line.split()
    lives.setdefault((slot, pid), []).append((int(t), int(rss) / 1024))
for (slot, pid), samples in lives.items():
    if len(samples) < 2:
        continue
    minutes = (samples[-1][0] - samples[0][0]) / 60
    first, last, peak = samples[0][1], samples[-1][1], max(s[1] for s in samples)
    print("  %-3s pid %-8s %5.1f min  start %6.1f  end %6.1f  peak %6.1f  growth %+6.1f" % (slot, pid, minutes, first, last, peak, last - first))
PY

if [ -n "$EXEMPT" ]; then
    echo
    echo "=== $EXEMPT, kept out of the chaos: its memory over the run (B-72) ==="
    python3 - "$RUN/rss.txt" "$EXEMPT" <<'PY'
import sys
slot = sys.argv[2]
rows = [line.split() for line in open(sys.argv[1])]
pids = {}
for t, s, pid, rss in rows:
    if s == slot:
        pids.setdefault(pid, []).append((int(t), int(rss) / 1024))
pid, samples = max(pids.items(), key=lambda kv: len(kv[1]))
start = samples[0][0]
settled = [x for x in samples if x[0] - start >= 600]
if len(settled) < 2:
    print("  too short to say: %d samples after the first 10 minutes" % len(settled))
    sys.exit(0)
n = len(settled)
mx = sum(t for t, _ in settled) / n
my = sum(r for _, r in settled) / n
slope = sum((t - mx) * (r - my) for t, r in settled) / sum((t - mx) ** 2 for t, _ in settled) * 3600
print("  pid %s, %d lives in the slot, %.1f minutes in this one" % (pid, len(pids), (samples[-1][0] - start) / 60))
print("  RSS at 10 min %.1f MB, at the end %.1f MB, peak %.1f MB; slope %+.2f MB/hour over %d samples"
      % (settled[0][1], samples[-1][1], max(r for _, r in samples), slope, n))
PY
fi

echo
echo "=== the output, counted by the Java client ==="
[ -n "$aborted" ] && bad "the run was aborted: $aborted"
kc /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server 127.0.0.1:9092 --topic "$INPUT" --time -1 2>/dev/null > "$RUN/input-ends.txt"
bash "$H" records dump "$OUTPUT" > "$RUN/output.txt"
verdict=$(python3 - "$RUN/input-ends.txt" "$RUN/output.txt" <<'PY'
import collections, sys
expected = set()
for line in open(sys.argv[1]):
    topic, p, end = line.strip().split(":")
    expected |= {(int(p), o) for o in range(int(end))}
seen = collections.Counter()
for line in open(sys.argv[2]):
    parts = line.strip().split("/")
    if len(parts) < 4 or not parts[3].startswith("x"):
        continue
    p, o, _ = bytes.fromhex(parts[3][1:]).decode().split(":", 2)
    seen[(int(p), int(o))] += 1
missing = expected - set(seen)
twice = sum(1 for k, n in seen.items() if n > 1)
foreign = sum(1 for k in seen if k not in expected)
print(len(expected), sum(seen.values()), len(missing), twice, foreign)
PY
)
read -r expected got missing twice foreign <<< "$verdict"
printf '  input %s records; output %s records; missing %s, more than once %s, unknown %s\n' "$expected" "$got" "$missing" "$twice" "$foreign"
[ "${expected:-0}" -gt 0 ] || bad "no input was counted"
[ "${missing:-1}" -eq 0 ] || bad "$missing input records never reached the output"
[ "${twice:-1}" -eq 0 ] || bad "$twice input records reached the output more than once"
[ "${foreign:-1}" -eq 0 ] || bad "$foreign output records name no input record"
commits=$(kc /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --describe --group "$GROUP" 2>/dev/null \
    | awk -v t="$INPUT" '$2 == t { print $3 ":" $4 "/" $5 }' | sort -n | paste -sd' ')
printf '  committed/end per input partition: %s\n' "$commits"
echo "$commits" | tr ' ' '\n' | awk -F'[:/]' 'NF && $2 != $3 { exit 1 }' || bad "the group's commits do not reach every end"

echo
[ "$fail" -eq 0 ] || { echo "B-70: RED ($RUN)"; exit 1; }
echo "B-70: $DURATION s under chaos, every input record in the output exactly once ($RUN)"
