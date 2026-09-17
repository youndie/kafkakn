#!/usr/bin/env bash
# Compare what the two arms observed.
#
# The differential oracle in one file: the same commonTest sources run on jvm and on linuxX64, each
# recording what only the client can know, and this step failing if they disagree.
#
# THE VACUITY GUARD IS THE IMPORTANT PART. Two files that do not exist agree perfectly, and so do two
# empty ones; without the checks below, a suite that never ran would produce the strongest possible
# "the arms agree".
set -uo pipefail

A=${1:?usage: compare-arms.sh <arm-a-file> <arm-b-file>}
B=${2:?usage: compare-arms.sh <arm-a-file> <arm-b-file>}
MIN=${MIN_OBSERVATIONS:-3}

for f in "$A" "$B"; do
    [ -f "$f" ] || { echo "MISSING: $f - the arm did not run, which is not the same as agreeing" >&2; exit 1; }
    n=$(grep -c . "$f")
    [ "$n" -ge "$MIN" ] || {
        echo "TOO FEW OBSERVATIONS in $f: $n, expected at least $MIN" >&2
        echo "  (an empty file agrees with every other empty file)" >&2
        exit 1
    }
done

echo "  $(basename "$A"): $(grep -c . "$A") observations"
echo "  $(basename "$B"): $(grep -c . "$B") observations"

# Sorted, because the order two arms record in is not part of the claim.
if diff <(sort "$A") <(sort "$B") > /tmp/arms.diff 2>&1; then
    echo "  THE ARMS AGREE on every observation"
    exit 0
fi

echo "  THE ARMS DISAGREE:"
sed 's/^/    /' /tmp/arms.diff
exit 1
