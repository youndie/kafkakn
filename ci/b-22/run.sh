#!/usr/bin/env bash
# The four answers `apply-patches.sh` can give, each watched happening.
#
# A message nobody has watched print is a message that is wrong as often as not - and the first
# version of that script proved it: its reverse probe reported "already applied" against a PRISTINE
# source, because GNU patch ignores `-R` when the patch is not applied and quietly goes forward. The
# one answer that gets a live patch deleted was the one it gave by default.
#
# The fixtures are the include block of librdkafka 2.13.0's src/rdrand.c in four states. They are
# small on purpose: the subject is the decision, not the compiler.
#
#   ci/b-22/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
APPLY="$ROOT/ci/librdkafka/apply-patches.sh"
PATCHES="$ROOT/ci/librdkafka/patches"
WORK=${WORK:-/tmp/kafkakn-b-22}

rm -rf "$WORK"
mkdir -p "$WORK"
fail=0

# case, expected exit, expected sentence
run_case() {
    local name=$1 expected_code=$2 expected=$3
    local dir="$WORK/$name"
    if [ "$name" = "target-missing" ]; then
        mkdir -p "$dir/src"
    else
        cp -r "$HERE/fixtures/$name" "$dir"
    fi
    local out code
    out=$(bash "$APPLY" "$dir" "$PATCHES" 2>&1)
    code=$?
    printf '%-16s exit=%-3s ' "$name" "$code"
    if [ "$code" -ne "$expected_code" ]; then
        printf 'WRONG EXIT (wanted %s)\n' "$expected_code"
        echo "$out" | sed 's/^/    /'
        fail=1
        return
    fi
    if ! printf '%s' "$out" | grep -q "$expected"; then
        printf 'WRONG MESSAGE (wanted "%s")\n' "$expected"
        echo "$out" | sed 's/^/    /'
        fail=1
        return
    fi
    printf '%s\n' "$expected"
    printf '%s' "$out" > "$WORK/$name.out"
}

echo "=== the four answers ==="
run_case pristine        0 "applying"
run_case already-guarded 1 "PATCH OBSOLETE"
run_case moved           1 "PATCH NO LONGER APPLIES"
run_case target-missing  1 "PATCH TARGET MISSING"

echo
echo "=== the patch actually changed the pristine source ==="
if grep -q "defined(HAVE_GETENTROPY)" "$WORK/pristine/src/rdrand.c"; then
    echo "  the guard is in the file - the green case did something"
else
    echo "  THE GREEN CASE CHANGED NOTHING - it passed without applying anything" >&2
    fail=1
fi

echo
echo "=== the four answers are four, not one repeated ==="
# A script that printed the same refusal for every case would pass every assertion above that is
# about exit codes. The distinctness is the property those messages are FOR.
distinct=$(cat "$WORK"/*.out 2>/dev/null | grep -cE "PATCH (OBSOLETE|NO LONGER APPLIES|TARGET MISSING)")
if [ "$distinct" -eq 3 ]; then
    echo "  three refusals, three different sentences"
else
    echo "  the refusals are not distinct: $distinct of 3" >&2
    fail=1
fi

echo
[ "$fail" -eq 0 ] && echo "B-22: every answer the bundle build can give has been watched happening" || {
    echo "B-22: RED" >&2
    exit 1
}
