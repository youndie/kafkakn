# The coordinate, read in ONE place.
#
# Three scripts used to parse `gradle.properties` themselves, and two of them parsed the wrong key
# the moment the conventions renamed `group` to `sborka.group`: the preflight probed `/snapshots//`
# and refused the publish with a message about routes, which is red for the wrong reason.
#
# `sborka.group` first, `group` second, because a repository can be on either side of that migration
# and a helper that knows only one of them is the same defect again.
#
#   . ci/lib/coordinate.sh   # sets GROUP, GROUP_PATH, VERSION
kafkakn_coordinate() {
    local properties=${1:-gradle.properties}
    GROUP=$(sed -n 's/^sborka\.group=//p' "$properties")
    [ -n "$GROUP" ] || GROUP=$(sed -n 's/^group=//p' "$properties")
    VERSION=$(sed -n 's/^version=//p' "$properties")
    [ -n "$GROUP" ] || { echo "no group in $properties - neither sborka.group nor group" >&2; return 1; }
    [ -n "$VERSION" ] || { echo "no version in $properties" >&2; return 1; }
    GROUP_PATH=${GROUP//./\/}
}
