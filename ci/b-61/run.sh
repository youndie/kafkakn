#!/usr/bin/env bash
# B-61: a topic's configuration, described and changed incrementally by the admin client on both arms, against
# the broker's own tool.
#
# Each arm's AdminConfigsTest creates a topic with retention.ms set, sets max.message.bytes, then deletes
# retention.ms. What the arms say is compared across them; then each arm's full description (every key, its
# value and its source) is held against kafka-configs.sh --describe --all on that arm's topic. The tool's
# source for a key is the first of its synonyms, and the broker's three sources (dynamic, dynamic default,
# static) are one here, as in the library.
#
#   ci/b-61/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations
fail=0
bad() { echo "    $*" >&2; fail=1; }
kc() { docker exec kafkakn-broker "$@"; }
fact() { sed -n "s/^configs\.$2=//p" "$OBS/$1-local.txt" | tail -1; }

echo "=== environment ==="
date -Is

echo
echo "=== broker, and both arms ==="
bash "$H" up || exit 1
rm -rf "$OBS"
for task in jvmTest linuxX64Test; do
    ./gradlew --console=plain ":kafkakn-core:$task" --rerun --tests '*AdminConfigsTest*' > "build/b-61-$task.out" 2>&1
    code=$?
    printf '  %-14s exit=%s\n' "$task" "$code"
    [ "$code" -eq 0 ] || { grep -E "FAILED|AssertionError|expected" "build/b-61-$task.out" | head -10; echo "  THE SUITE FAILED on $task"; exit 1; }
done
MIN_OBSERVATIONS=7 bash ci/harness/compare-arms.sh "$OBS/jvm.txt" "$OBS/linuxX64.txt" || exit 1
sed 's/^/  /' "$OBS/jvm.txt"

echo
echo "=== each arm's description against kafka-configs.sh --describe --all ==="
for arm in jvm linuxX64; do
    topic=$(fact "$arm" topic)
    said=$(fact "$arm" after.delete)
    [ -n "$topic" ] && [ -n "$said" ] || { bad "$arm recorded no topic or description"; continue; }
    kc /opt/kafka/bin/kafka-configs.sh --bootstrap-server 127.0.0.1:9092 --describe --all --entity-type topics --entity-name "$topic" \
        > "build/b-61-tool-$arm.txt" 2>&1
    verdict=$(SAID="$said" python3 - "build/b-61-tool-$arm.txt" <<'EOF'
import os, re, sys
SOURCES = {"DYNAMIC_TOPIC_CONFIG": "TOPIC", "DYNAMIC_BROKER_CONFIG": "BROKER",
           "DYNAMIC_DEFAULT_BROKER_CONFIG": "BROKER", "STATIC_BROKER_CONFIG": "BROKER", "DEFAULT_CONFIG": "DEFAULT"}
tool = {}
for line in open(sys.argv[1]):
    m = re.match(r"\s+([a-z0-9.]+)=(.*?) sensitive=(true|false) synonyms=\{(.*)\}\s*$", line)
    if not m:
        continue
    key, value, sensitive, synonyms = m.groups()
    # The tool prints no source of its own, only the synonyms: every level that sets the key, nearest first.
    # None at all ("synonyms={}", as for retention.ms, whose broker synonym log.retention.ms is unset) means
    # nobody set it: the default, which is the source both clients report for such a key.
    first = synonyms.split(":", 1)[0] if synonyms else "DEFAULT_CONFIG"
    tool[key] = ("null" if sensitive == "true" else value) + "/" + SOURCES.get(first, "UNKNOWN")
said = dict(entry.split("=", 1) for entry in os.environ["SAID"].split(";") if entry)
differ = sorted(k for k in set(tool) | set(said) if tool.get(k) != said.get(k))
print(len(tool), len(said), len(differ))
for k in differ[:10]:
    print("    %s: arm %s, tool %s" % (k, said.get(k), tool.get(k)))
for k in ("retention.ms", "max.message.bytes"):
    print("    %s: %s" % (k, tool.get(k)))
EOF
)
    read -r tool_keys arm_keys differ <<< "$(head -1 <<< "$verdict")"
    printf '  %-9s %s keys from the tool, %s from the arm, %s differ\n' "$arm" "$tool_keys" "$arm_keys" "$differ"
    tail -n +2 <<< "$verdict"
    [ "${tool_keys:-0}" -gt 20 ] || bad "$arm: the tool's output was not read ($tool_keys keys); the comparison proves nothing"
    [ "${differ:-1}" -eq 0 ] || bad "$arm: $differ keys differ from the broker's tool"
done

echo
[ "$fail" -eq 0 ] || { echo "B-61: RED"; exit 1; }
echo "B-61: both arms describe and change a topic's configuration as the broker's tool reports it, and agree"
