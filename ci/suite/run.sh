#!/usr/bin/env bash
# B-81: the whole suite, on both arms, against the fixture broker - the run that CI makes on every pull request and
# every push to main, and that anyone can make on a box with Docker, a JDK and the C bundle.
#
# Until B-81 this ran only by hand, through each item's own runner on the Linux box, and "merge on green" read a
# green that had not run a single test. So this script says three things or fails:
#   - the fixture can say no: the TLS, mutual-TLS and SASL listeners refuse what they must, before anything is run;
#   - every test that ran on each arm passed, and each arm ran at least MIN_TESTS of them, because a suite that
#     compiled nothing or skipped its way through is green for nothing;
#   - the arms agree on everything only a client knows (ci/harness/compare-arms.sh), which is the reason there are
#     two arms at all.
#
# Tests the broker cannot survive (pausing or stopping it) stay behind their own switches and do not run here; their
# items' runners are where they run.
#
#   ci/suite/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations
RESULTS=kafkakn-core/build/test-results
MIN_TESTS=${MIN_TESTS:-140}
export KAFKAKN_BOOTSTRAP=127.0.0.1:9092 KAFKAKN_TOPIC=kafkakn KAFKAKN_STRICT_TOPIC=kafkakn-strict
export KAFKAKN_SSL_BOOTSTRAP=127.0.0.1:9094
export KAFKAKN_ACCOUNTING_TOPIC=kafkakn-acct-suite-$(date +%s)

echo "=== environment ==="
date -Is
nproc; free -m | sed -n 2p

echo
echo "=== the JVM test classes first ==="
# Before the broker, because `broker.sh up` writes the consumer's fixture topic with ci/harness/Records.java on the
# kafka-clients jar Gradle resolved. On a machine Gradle has never run on, CI's first, there is no such jar yet, and
# `up` failed with "could not write kafkakn-consume". Compiling the JVM tests resolves it, and is needed anyway.
./gradlew --no-daemon --console=plain :kafkakn-core:compileTestKotlinJvm 2>&1 | tail -2
[ "${PIPESTATUS[0]}" -eq 0 ] || { echo "the JVM test classes did not compile" >&2; exit 1; }

echo
echo "=== the fixture, and that it can say no ==="
bash "$H" up || exit 1
for arm in jvm linuxX64; do bash "$H" topic "$KAFKAKN_ACCOUNTING_TOPIC-$arm" > /dev/null || exit 1; done
bash "$H" tls-selftest || exit 1
bash "$H" mtls-selftest || exit 1
bash "$H" sasl-selftest || exit 1

echo
echo "=== both arms, the whole suite ==="
rm -rf "$OBS" "$RESULTS"
# --continue: a red JVM arm must not hide what the native arm would have said.
./gradlew --no-daemon --console=plain --continue jvmTest linuxX64Test 2>&1 | tail -5
gradle=${PIPESTATUS[0]}
fail=0
for arm in jvmTest linuxX64Test; do
    ls "$RESULTS/$arm"/TEST-*.xml > /dev/null 2>&1 || { echo "  $arm: NO RESULTS - it did not run" >&2; fail=1; continue; }
    t=$(grep -ho 'tests="[0-9]*"' "$RESULTS/$arm"/TEST-*.xml | grep -oE '[0-9]+' | paste -sd+ | bc)
    f=$(grep -ho 'failures="[0-9]*"' "$RESULTS/$arm"/TEST-*.xml | grep -oE '[0-9]+' | paste -sd+ | bc)
    e=$(grep -ho 'errors="[0-9]*"' "$RESULTS/$arm"/TEST-*.xml | grep -oE '[0-9]+' | paste -sd+ | bc)
    printf '  %-14s tests=%-4s failures=%s errors=%s\n' "$arm" "$t" "$f" "$e"
    [ "$t" -ge "$MIN_TESTS" ] || { echo "  $arm ran $t tests, fewer than $MIN_TESTS" >&2; fail=1; }
    [ "$f" -eq 0 ] && [ "$e" -eq 0 ] || fail=1
    for r in $(grep -l '<failure\|<error' "$RESULTS/$arm"/TEST-*.xml 2>/dev/null); do
        grep -o 'testcase name="[^"]*"[^>]*>[[:space:]]*<\(failure\|error\) message="[^"]\{0,200\}' "$r" \
            | sed 's/^/    FAILED /' | head -5
    done
done
[ "$gradle" -eq 0 ] || fail=1

echo
echo "=== the arms, on what only a client knows ==="
MIN_OBSERVATIONS=${MIN_OBSERVATIONS:-80} bash ci/harness/compare-arms.sh "$OBS/jvm.txt" "$OBS/linuxX64.txt" || fail=1

echo
[ "$fail" -eq 0 ] || { echo "SUITE: RED"; exit 1; }
echo "SUITE: both arms green, and they agree"
