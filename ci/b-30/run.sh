#!/usr/bin/env bash
# B-30: transactions on both arms, checked by the broker's own consumer and transaction coordinator.
#
# THE ORACLE CHANGES HERE. Every run before this one counted end offsets; a commit or abort marker
# occupies an offset, so on a transactional topic the end offset is not a count. This counts RECORDS,
# twice: under read_committed, which is the claim, and under read_uncommitted, which shows an aborted
# transaction's records really were written - a zero under read_committed alone is also what a
# producer that sent nothing looks like. And `kafka-transactions.sh describe` says how each ended.
#
#   ci/b-30/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations
TOPIC=kafkakn-txn-$(date +%s)
export KAFKAKN_BOOTSTRAP=127.0.0.1:9092 KAFKAKN_TOPIC=$TOPIC

echo "=== environment ==="
date -Is
echo "  topic $TOPIC, fresh"

echo
echo "=== broker ==="
bash "$H" up || exit 1
bash "$H" topic "$TOPIC" | head -1

echo
echo "=== both arms, one test task per invocation, rerun every time ==="
rm -rf "$OBS"
for task in jvmTest linuxX64Test; do
    ./gradlew --console=plain ":kafkakn-core:$task" --rerun --tests '*TransactionTest*' > "build/b-30-$task.out" 2>&1
    code=$?
    printf '  %-14s exit=%s\n' "$task" "$code"
    [ "$code" -eq 0 ] || { tail -30 "build/b-30-$task.out"; echo "  THE SUITE FAILED on $task"; exit 1; }
done

echo
echo "=== the topic, read twice by the broker's own consumer ==="
CONSUME_MS=20000 ISOLATION=read_committed bash "$H" values "$TOPIC" > build/b-30-committed.txt
CONSUME_MS=20000 ISOLATION=read_uncommitted bash "$H" values "$TOPIC" > build/b-30-uncommitted.txt
printf '  read_committed: %s records, read_uncommitted: %s\n' \
    "$(wc -l < build/b-30-committed.txt)" "$(wc -l < build/b-30-uncommitted.txt)"

count() { grep -ac "^$2:" "$1"; }
fail=0
expect() { # <arm> <what> <isolation> <got> <want>
    if [ "$4" -eq "$5" ]; then
        printf '  %-9s %-13s %-17s %3s  (expected %s)\n' "$1" "$2" "$3" "$4" "$5"
    else
        printf '  %-9s %-13s %-17s %3s  EXPECTED %s\n' "$1" "$2" "$3" "$4" "$5" >&2
        fail=1
    fi
}
for arm in jvm linuxX64; do
    f="$OBS/$arm-local.txt"
    n=$(sed -n 's/^txn\.count=//p' "$f" | tail -1)
    [ -n "$n" ] || { echo "  $arm recorded nothing - its tests did not run" >&2; exit 1; }
    for what in commit helper-commit abort helper-abort fenced; do
        stamp=$(sed -n "s/^txn\.$what\.stamp=//p" "$f" | tail -1)
        [ -n "$stamp" ] || { echo "  $arm recorded no $what stamp" >&2; exit 1; }
        rc=$(count build/b-30-committed.txt "$stamp")
        ru=$(count build/b-30-uncommitted.txt "$stamp")
        case "$what" in
            commit | helper-commit)
                expect "$arm" "$what" read_committed "$rc" "$n" ;;
            abort | helper-abort | fenced)
                # fenced: the second producer's init aborts the first one's open transaction.
                expect "$arm" "$what" read_committed "$rc" 0
                expect "$arm" "$what" read_uncommitted "$ru" "$n" ;;
        esac
    done
done

echo
echo "=== how the transaction coordinator says each one ended ==="
for arm in jvm linuxX64; do
    f="$OBS/$arm-local.txt"
    for pair in commit:CompleteCommit abort:CompleteAbort; do
        what=${pair%%:*}
        want=${pair#*:}
        id=$(sed -n "s/^txn\.$what\.id=//p" "$f" | tail -1)
        state=$(bash "$H" txn-state "$id")
        if [ "$state" = "$want" ]; then
            printf '  %-9s %-7s %s\n' "$arm" "$what" "$state"
        else
            printf '  %-9s %-7s %s  EXPECTED %s\n' "$arm" "$what" "${state:-nothing}" "$want" >&2
            fail=1
        fi
    done
done

echo
echo "=== what each arm's fenced producer said, under the one exception ==="
for arm in jvm linuxX64; do
    printf '  %-9s %s\n' "$arm" "$(sed -n 's/^txn\.fenced\.failure=//p' "$OBS/$arm-local.txt" | tail -1 | cut -c1-300)"
done

echo
[ "$fail" -eq 0 ] || { echo "B-30: RED"; exit 1; }
echo "B-30: committed records are all visible, aborted ones none of them yet all written, and a fenced producer throws one exception - on both arms"
