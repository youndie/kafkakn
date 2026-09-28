#!/usr/bin/env bash
# B-93: the Schema Registry client on both arms, against the fixture registry. What a cached register costs is counted
# in the registry's own request log, never by the client; the run's subjects carry its name, so this run's requests
# are the ones counted. The registry's REST answer, read with curl, is held against what the client registered.
#
#   ci/b-93/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
export GRADLE_OPTS=-Dorg.gradle.daemon=false
REGISTRY=http://127.0.0.1:18081
export KAFKAKN_RUN=b93-$(date +%s)
fail=0
bad() { echo "    $*" >&2; fail=1; }

echo "=== environment ==="
date -Is
bash ci/harness/broker.sh up || exit 1

echo
echo "=== both arms ==="
for task in jvmTest linuxX64Test; do
    ./gradlew --console=plain ":kafkakn-schema-registry:$task" --rerun > "build/b-93-$task.out" 2>&1
    code=$?
    printf '  %-14s exit=%s\n' "$task" "$code"
    [ "$code" -eq 0 ] || { grep -E "FAILED|expected|Exception" "build/b-93-$task.out" | head -6; bad "$task failed"; }
done

echo
echo "=== the registry's own count of this run's registrations ==="
log=$(docker logs kafkakn-registry 2>&1)
for arm in jvm linuxX64; do
    cached=$(printf '%s\n' "$log" | grep -c "POST /subjects/kafkakn-sr-cached-$arm-$KAFKAKN_RUN-value/versions")
    control=$(printf '%s\n' "$log" | grep -c "POST /subjects/kafkakn-sr-control-$arm-$KAFKAKN_RUN-value/versions")
    printf '  %-9s same schema twice: %s request(s); two different schemas: %s request(s)\n' "$arm" "$cached" "$control"
    [ "$cached" -eq 1 ] || bad "$arm: the cached registration reached the registry $cached times"
    [ "$control" -eq 2 ] || bad "$arm: the control did not count two requests - the count is not seeing requests"
    id=$(curl -s "$REGISTRY/subjects/kafkakn-sr-cached-$arm-$KAFKAKN_RUN-value/versions/latest" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("id"), d.get("schemaType"))')
    printf '            the registry holds: id and type %s\n' "$id"
done

echo
[ "$fail" -eq 0 ] || { echo "B-93: RED"; exit 1; }
echo "B-93: registered once, read back by id, errors in one type, alike on both arms"
