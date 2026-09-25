#!/usr/bin/env python3
"""No `TODO(` in the library's main sources.

`TODO()` compiles, type-checks as anything, and throws only when it is called. So a stub made of it
looks like an implementation to every reader and every tool until somebody calls it. One stood in
common code for the whole life of the producer: `UnimplementedProducer`, every method
`TODO("no producer yet")`. It was never referenced after the first actual arrived, and nothing flagged
it, because nothing ever called it (B-45).

The rule is about **code**, not mentions. Comments are stripped first, so KDoc may say `TODO(` to
explain this rule. Test sources are not scanned: a test double that throws where it is not meant to be
reached is ordinary there.

    no_todo_in_main.py [source-root]
    no_todo_in_main.py --selftest     the rule, run against a sample it must flag and one it must not
"""

import pathlib
import re
import sys

BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.S)
LINE_COMMENT = re.compile(r"//.*")
STUB = re.compile(r"\bTODO\s*\(")

FLAGGED_SAMPLE = """
internal object Stub : KafkaProducer {
    override suspend fun send(record: ProducerRecord): RecordMetadata = TODO("no producer yet")
}
"""

CLEAN_SAMPLE = """
/** Never `TODO("...")`: see scripts/no_todo_in_main.py. */
public suspend fun send(record: ProducerRecord): RecordMetadata = delegate.send(record) // not a TODO(
"""


def offending_lines(source: str) -> list[int]:
    code = BLOCK_COMMENT.sub(lambda m: "\n" * m.group(0).count("\n"), source)
    return [n for n, line in enumerate(code.splitlines(), 1) if STUB.search(LINE_COMMENT.sub("", line))]


def selftest() -> int:
    if not offending_lines(FLAGGED_SAMPLE):
        print("selftest: the stub sample was NOT flagged - the rule checks nothing")
        return 1
    if offending_lines(CLEAN_SAMPLE):
        print("selftest: the clean sample WAS flagged - comments are not being stripped")
        return 1
    print("selftest: the stub is flagged, and the comments that mention it are not")
    return 0


def main(root: pathlib.Path) -> int:
    files = [f for f in sorted(root.glob("*/src/*Main/**/*.kt")) if "/build/" not in f.as_posix()]
    if not files:
        print(f"no main sources under {root} - nothing checked, and that is a failure")
        return 1
    found = [(f, n) for f in files for n in offending_lines(f.read_text())]
    for f, n in found:
        print(f"{f}:{n}: TODO( in main sources - an implementation that throws when called")
    if found:
        return 1
    print(f"no TODO( in {len(files)} main source files")
    return 0


if __name__ == "__main__":
    if sys.argv[1:] == ["--selftest"]:
        sys.exit(selftest())
    sys.exit(main(pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else ".")))
