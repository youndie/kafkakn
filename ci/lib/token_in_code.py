#!/usr/bin/env python3
"""Count a token in a source file, ignoring comments.

Written because a check for "no -Xoverride-konan-properties is used" counted its own comment saying
that none is used. A checker that cannot tell documentation from code produces a false reading and,
worse, teaches people to delete the documentation rather than fix the checker.

    token_in_code.py <file> <token>   ->  prints the count, exits 0

Handles `//`, `/* */` and `#` comments, which covers Kotlin, the Gradle DSL and shell.
"""

import re
import sys

BLOCK = re.compile(r"/\*.*?\*/", re.S)
LINE = re.compile(r"//.*")
HASH = re.compile(r"(?m)^\s*#.*")


def main() -> int:
    path, token = sys.argv[1], sys.argv[2]
    with open(path, encoding="utf-8") as handle:
        source = handle.read()
    code = HASH.sub("", LINE.sub("", BLOCK.sub("", source)))
    print(code.count(token))
    return 0


if __name__ == "__main__":
    sys.exit(main())
