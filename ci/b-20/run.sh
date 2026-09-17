#!/usr/bin/env bash
# RQ-C: ten minutes on a machine that has never seen this repository.
#
# `ci/downstream` proves the artefact resolves and links - in CI, on a runner this project configured,
# against a cache this project purges. This asks the same question from a machine with nothing on it:
# no clone, no Gradle cache, no `~/.konan`, and no knowledge of this repository beyond what the README
# prints. The difference between the two is every step the README does not name.
#
# THE PROJECT IS BUILT FROM THE README ITSELF. The repository and dependency lines are extracted from
# the fenced block under "Getting it" rather than copied here: a copy would go stale silently and
# this check would then be measuring a project nobody is told how to write.
#
# What is deliberately OUTSIDE the clock: starting the broker. A stranger who wants a Kafka producer
# has a Kafka. Everything else - the toolchain, the dependency, the compiler, the link, the run - is
# inside it, because that is what they wait for.
#
#   ci/b-20/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
BROKER=ci/harness/broker.sh
TOPIC=${TOPIC:-kafkakn-stranger}
WORK=${WORK:-/tmp/kafkakn-b-20}
# A machine with Gradle and a JDK on it, and nothing else. The item's own words: "a fresh Linux box,
# Gradle, the README's lines and nothing else".
IMAGE=${IMAGE:-gradle:9.2.0-jdk21}
BUDGET=${BUDGET:-600}

say() { printf '%s\n' "$*"; }

echo "=== environment ==="
date -Is
say "  image: $IMAGE, budget: ${BUDGET}s"
# Read off the README rather than set here: the Kotlin version is part of what the stranger is told,
# and a version pinned in this script would be this repository telling itself what it already knows.
say "  kotlin, as the README names it: $(grep -o 'kotlin("multiplatform") version "[^"]*"' README.md | head -1)"

echo
echo "=== the broker, outside the clock ==="
bash "$BROKER" up || exit 1
bash "$BROKER" topic "$TOPIC" | head -1

echo
echo "=== the project, written from the README ==="
rm -rf "$WORK"
mkdir -p "$WORK/project/src/nativeMain/kotlin" "$WORK/home/.gradle"
python3 - "$ROOT/README.md" "$WORK/project/readme-block.kts" <<'PY' || exit 1
import re
import sys

readme = open(sys.argv[1]).read()
start = readme.index("## Getting it")
block = re.search(r"```kotlin\n(.*?)```", readme[start:], re.S)
if block is None:
    print("  no fenced kotlin block under 'Getting it' - the README changed shape", file=sys.stderr)
    raise SystemExit(1)
text = block.group(1)
for required in ("repositories", "dependencies", "kafkakn-core"):
    if required not in text:
        print(f"  the block under 'Getting it' does not mention {required}", file=sys.stderr)
        raise SystemExit(1)
open(sys.argv[2], "w").write(text)
print("  extracted %d lines from README.md" % len(text.splitlines()))
PY
sed 's/^/  | /' "$WORK/project/readme-block.kts"

cat > "$WORK/project/settings.gradle.kts" <<'KTS'
rootProject.name = "stranger"
KTS

# THE BLOCK IS THE WHOLE BUILD FILE, and it has to be. The first run of this check pasted the
# README's two fragments into a scaffold written here, and the build failed in 25 seconds on
# `Unresolved reference 'implementation'` - a top-level `dependencies { }` has no such configuration
# in a multiplatform project, which is the only kind that can link the native artefact. A scaffold
# written by this repository would have hidden that for ever, because the scaffold is the part a
# stranger does not have.
mv "$WORK/project/readme-block.kts" "$WORK/project/build.gradle.kts"

cat > "$WORK/project/src/nativeMain/kotlin/Main.kt" <<'KOT'
import io.github.youndie.kafkakn.ProducerConfig
import io.github.youndie.kafkakn.ProducerRecord
import io.github.youndie.kafkakn.kafkaProducer
import kotlinx.coroutines.runBlocking

// The smallest thing the README promises: one record, and where it landed. `send` suspends, so
// something has to run a coroutine - which is itself a question about what the dependency brings.
fun main() = runBlocking {
    val producer = kafkaProducer(
        ProducerConfig(
            "bootstrap.servers" to "127.0.0.1:9092",
            "acks" to "all",
        ),
    )
    val where = producer.send(ProducerRecord("TOPIC_NAME", "one record from a stranger".encodeToByteArray()))
    println("STRANGER-OK ${where.topic}-${where.partition}@${where.offset}")
    producer.close()
}
KOT
sed -i "s/TOPIC_NAME/$TOPIC/" "$WORK/project/src/nativeMain/kotlin/Main.kt"

echo
echo "=== the clock starts ==="
BEFORE=$(bash "$BROKER" offsets "$TOPIC")
START=$(date +%s)
# Every line stamped with the seconds since the start, so the split below is read off the run rather
# than guessed afterwards. `--network host` is how the container reaches the broker; nothing else of
# this machine is visible to it, and HOME and the Gradle cache are empty directories made just now.
docker run --rm --network host \
    --user "$(id -u):$(id -g)" \
    -e HOME=/home/stranger \
    -e GRADLE_USER_HOME=/home/stranger/.gradle \
    -v "$WORK/project:/project" \
    -v "$WORK/home:/home/stranger" \
    -w /project "$IMAGE" \
    bash -c 'gradle --no-daemon --console=plain linkReleaseExecutableNative &&
             ./build/bin/native/releaseExecutable/stranger.kexe' \
    2>&1 | awk -v s="$START" '{ printf "%6d  %s\n", systime() - s, $0; fflush() }' | tee "$WORK/run.log"
END=$(date +%s)
AFTER=$(bash "$BROKER" offsets "$TOPIC")

echo
echo "=== the two numbers ==="
TOTAL=$(( END - START ))
# The boundary is the last line that is about the Kotlin/Native toolchain arriving. Most of a first
# run is `~/.konan` downloading, and that is Kotlin/Native's price rather than kafkakn's - a red
# caused only by the download has to be readable as that, not argued about afterwards.
KONAN=$(grep -iE 'kotlin-native-prebuilt|konan|Download.*native' "$WORK/run.log" | tail -1 | awk '{ print $1 }')
if [ -n "$KONAN" ]; then
    say "  toolchain arriving:   ${KONAN}s"
    say "  everything after it:  $(( TOTAL - KONAN ))s"
else
    say "  toolchain arriving:   not seen in the log - either already present or named differently"
    say "  everything after it:  ${TOTAL}s"
fi
say "  what the stranger waits: ${TOTAL}s of a ${BUDGET}s budget"

echo
echo "=== verdict ==="
if ! grep -q 'STRANGER-OK' "$WORK/run.log"; then
    say "RED: the binary never produced a record. What it needed that the README does not say is in"
    say "     $WORK/run.log, and it becomes README text before anything else."
    grep -iE 'error|not found|could not|unresolved|failed' "$WORK/run.log" | head -8 | sed 's/^/  /'
    exit 1
fi
say "  the topic's end offsets: $BEFORE -> $AFTER"
[ "$AFTER" -gt "$BEFORE" ] || { say "RED: the binary printed success and the broker holds nothing more"; exit 1; }
[ "$TOTAL" -le "$BUDGET" ] || { say "RED: ${TOTAL}s against a ${BUDGET}s budget"; exit 1; }
say "GREEN: from an empty machine to a record on the topic in ${TOTAL}s"
