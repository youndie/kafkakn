#!/usr/bin/env bash
# RQ-A: does close() keep its promise inside a real ordered shutdown?
#
# The subject is a service this repository does not own, and a shutdown no test in this repository
# arranged: a webhook gateway with an ingress, a SQLite database and kore's ordered stop, publishing
# every accepted event through kafkakn. A round is a steady stream of webhooks, a SIGTERM at a moment
# nothing planned for, and then two independent readings of what happened.
#
# THE ORACLE IS THE SERVICE'S OWN TABLE, NOT THE PRODUCER. `events` records what the gateway accepted
# before this library was involved; the topic's keys, read by the broker's own consumer, say what
# arrived. A producer asked whether it delivered what it delivered answers yes.
#
# Two pre-registered decisions, made before any round ran, because deciding them afterwards would
# decide the result:
#
#   * the row is committed BEFORE the publish, which is what makes a loss visible at all;
#   * SIGTERM only, never SIGKILL. A process killed outright cannot run `close`, and the gap between
#     the row and the send is then an OUTBOX question rather than a question about this library.
#
# The last round is a positive control: the broker is stopped before the signal, so records cannot
# land. If that round does not report loss, this harness has not been shown able to see one, and
# every green above it means nothing.
#
#   ci/b-19/run.sh [rounds]
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
BROKER="$HERE/../harness/broker.sh"
XYK=${XYK:-$HOME/xyk}
ROUNDS=${1:-${ROUNDS:-20}}
PORT=${PORT:-8099}
BOOTSTRAP=${BOOTSTRAP:-127.0.0.1:9092}
WORK=${WORK:-/tmp/kafkakn-b-19}
HOOK=rq-a
# Long enough that the signal lands in the middle of a stream rather than at its start, short enough
# that twenty rounds fit in an afternoon.
STREAM_SECONDS=${STREAM_SECONDS:-40}
CONCURRENCY=${CONCURRENCY:-8}
CONTAINER=kafkakn-broker
# A TOPIC NAME THAT CANNOT BE REUSED BETWEEN RUNS. The first version named topics after the round
# alone, so the second run of the harness read the first run's records back and reported 2 612
# records with no row behind them — a finding that was entirely the harness's own doing.
RUN=${RUN:-$(date +%s)}

BIN="$XYK/server/build/bin/native/releaseExecutable/server.kexe"

say() { printf '%s\n' "$*"; }

# A PORT PER ROUND, because a port is not free the moment its process is. The first run of this
# harness lost its control round to `EADDRINUSE` seconds after a clean exit — the listening socket
# was still in TIME_WAIT — and a round that never started reads exactly like a round that passed.
wait_for_ready() {
    for _ in $(seq 1 60); do
        if curl -fsS "http://127.0.0.1:$1/health/ready" >/dev/null 2>&1; then return 0; fi
        sleep 0.5
    done
    return 1
}

# A round. The only thing that differs between the ordinary ones and the control is whether the
# broker is still there when the signal arrives.
round() {
    local label=$1 mode=$2 port=$3
    local topic="xyk-rq-a-$RUN-$label"
    local db="$WORK/$label.db"
    local log="$WORK/$label.log"
    local gen="$WORK/$label.gen"

    bash "$BROKER" topic "$topic" >/dev/null 2>&1

    XYK_DB_PATH="$db" \
    XYK_PORT="$port" \
    XYK_HOST=127.0.0.1 \
    XYK_ALLOW_UNVERIFIED=true \
    XYK_DELIVERY_WORKERS=0 \
    XYK_BOOTSTRAP_ENDPOINT_ID="$HOOK" \
    XYK_BOOTSTRAP_SCHEME=none \
    XYK_BOOTSTRAP_SECRET=unused-by-the-none-scheme \
    XYK_KAFKA_BOOTSTRAP_SERVERS="$BOOTSTRAP" \
    XYK_KAFKA_TOPIC="$topic" \
        "$BIN" > "$log" 2>&1 &
    local pid=$!

    if ! wait_for_ready "$port"; then
        say "  $label: the service never became ready - here is its log"
        tail -5 "$log"
        kill -9 "$pid" 2>/dev/null
        return 1
    fi

    python3 "$HERE/generator.py" "http://127.0.0.1:$port/hooks/$HOOK" "$STREAM_SECONDS" "$CONCURRENCY" \
        > "$gen" 2>&1 &
    local gpid=$!

    # Somewhere in the middle, and not the same somewhere twice. A signal that always arrives at the
    # same offset tests one moment repeatedly and calls it twenty rounds.
    sleep "$(awk -v s="$RANDOM" 'BEGIN { srand(s); printf "%.2f", 3 + rand() * 5 }')"

    if [ "$mode" = control ]; then
        # The positive control. Records cannot land from here on, so the rows written after this line
        # are a loss the reconciliation below has to notice.
        docker stop "$CONTAINER" >/dev/null 2>&1
    fi

    local started ended
    started=$(date +%s.%N)
    kill -TERM "$pid"

    local exited=
    for _ in $(seq 1 120); do
        if ! kill -0 "$pid" 2>/dev/null; then exited=yes; break; fi
        sleep 0.25
    done
    ended=$(date +%s.%N)
    wait "$pid" 2>/dev/null
    local code=$?
    kill -TERM "$gpid" 2>/dev/null
    wait "$gpid" 2>/dev/null

    if [ -z "$exited" ]; then
        say "  $label: the process did not stop within 30s of SIGTERM - it was left alone, not killed"
        return 1
    fi

    [ "$mode" = control ] && bash "$BROKER" up >/dev/null 2>&1

    local seconds
    seconds=$(awk -v a="$started" -v b="$ended" 'BEGIN { printf "%.2f", b - a }')

    # The two readings, taken independently and compared afterwards.
    sqlite3 "$db" 'select id from events;' 2>/dev/null | sort > "$WORK/$label.rows"
    bash "$BROKER" keys "$topic" 2>/dev/null | grep -v '^$' | sort -u > "$WORK/$label.keys"

    local rows records missing extra refused silent deadline drain release
    rows=$(wc -l < "$WORK/$label.rows")
    records=$(wc -l < "$WORK/$label.keys")
    comm -23 "$WORK/$label.rows" "$WORK/$label.keys" > "$WORK/$label.missing"
    comm -13 "$WORK/$label.rows" "$WORK/$label.keys" > "$WORK/$label.extra"
    missing=$(wc -l < "$WORK/$label.missing")
    extra=$(wc -l < "$WORK/$label.extra")

    # A publish the service itself reported as refused, BY EVENT ID. The contract's promise is that a
    # record accepted by `send` is acknowledged **or its `send` throws**, so a throw is a legitimate
    # non-delivery and must be subtracted from the loss rather than counted as one. What is left over
    # — a row, no record, and nothing said about it — is the number this whole item is about.
    grep -o 'kafka sink refused [0-9a-f]*' "$log" | awk '{ print $4 }' | sort -u > "$WORK/$label.refused"
    refused=$(wc -l < "$WORK/$label.refused")
    comm -23 "$WORK/$label.missing" "$WORK/$label.refused" > "$WORK/$label.silent"
    silent=$(wc -l < "$WORK/$label.silent")

    # The word kore prints when a stage runs past its deadline. It is spelled the same way in the
    # transcript that this harness has watched happen, which is the only reason this line is not a
    # check that can never fire.
    deadline=ok
    grep -q 'DEADLINE_EXCEEDED' "$log" && deadline=EXCEEDED
    drain=$(awk '/^DRAIN /{ print $4 }' "$log" | tail -1)
    release=$(awk '/^RELEASE_CONSUMERS /{ print $4 }' "$log" | tail -1)

    printf '%-10s rows=%-6s records=%-6s missing=%-5s silent=%-5s extra=%-4s refused=%-4s stop=%-7s exit=%s %s drain=%s release=%s\n' \
        "$label" "$rows" "$records" "$missing" "$silent" "$extra" "$refused" "${seconds}s" "$code" \
        "$deadline" "${drain:--}" "${release:--}"

    # TWO DIFFERENT VERDICTS, ON PURPOSE. An ordinary round is judged on `silent`, because a `send`
    # that throws is a non-delivery the contract allows and the service named out loud. The control is
    # judged on `missing`, because what it has to demonstrate is that the row-against-record
    # comparison can come out non-zero at all — whether the service managed to name each loss is a
    # separate question and not the one the control is asked.
    if [ "$mode" = control ]; then
        echo "$missing" > "$WORK/$label.verdict"
    else
        echo "$silent" > "$WORK/$label.verdict"
    fi
    return 0
}

say "RQ-A: close() inside a real ordered shutdown - $ROUNDS rounds plus one positive control"
rm -rf "$WORK"
mkdir -p "$WORK"

bash "$BROKER" up || exit 1
# The fixture has to be able to say no before anything it says yes to is worth reading.
bash "$BROKER" selftest || exit 1

say "building the publisher (release, so a deadline that is exceeded is not a debug binary's fault)"
(cd "$XYK" && ./gradlew :server:linkReleaseExecutableNative -q --console=plain) || exit 1
[ -x "$BIN" ] || { say "no binary at $BIN"; exit 1; }
say "  $(sha256sum "$BIN" | cut -c1-16) $(stat -c %s "$BIN") bytes"
# Which snapshot this was measured against. The coordinate alone names a moving target.
find "$HOME/.gradle/caches/modules-2/files-2.1/io.github.youndie.kafkakn" -name '*.klib' 2>/dev/null \
    | sed 's|.*/||' | sort -u | head -3 | sed 's/^/  resolved: /'

for n in $(seq 1 "$ROUNDS"); do
    round "$(printf 'round-%02d' "$n")" normal "$(( PORT + n ))"
done
round control control "$(( PORT + ROUNDS + 1 ))"

say ""
say "SUMMARY"
lost=0
for f in "$WORK"/round-*.verdict; do
    [ -e "$f" ] || continue
    lost=$(( lost + $(cat "$f") ))
done
control=$(cat "$WORK/control.verdict" 2>/dev/null || echo "-")
say "  accepted, missing from the topic, and nothing said about it - the ordinary rounds: $lost"
say "  the positive control saw missing: $control  (a control that saw nothing invalidates the rounds above)"
