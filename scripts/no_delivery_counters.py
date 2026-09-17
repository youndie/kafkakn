#!/usr/bin/env python3
"""The public surface must not offer a count of deliveries.

The defect this project is shaped around was not a missing feature: it was a number that answered a
narrower question than the one its reader asked. A naive binding counted delivery reports, every one
of them correct, and reported complete success while 264 826 records of 1 000 000 had never been
queued at all - a record that is refused produces no delivery report, so nothing it counted could
ever have been wrong.

A `sentCount` on the producer rebuilds that. It is truthful about what was enqueued and it will be
read as a success rate, because that is the question the caller has. So the reconciliation lives in
the suite, against the broker's end offsets, and the library offers no number to misread
(B-09, docs/features/feature-backpressure-and-accounting.md).

The rule is about **declarations**, not mentions: the documents and the KDoc above have to be able to
say `sentCount` in order to explain why there is not one. Comments are stripped first, and only
`val`/`var`/`fun` names are matched - an earlier checker in this repository counted its own comment
saying the forbidden thing was absent.

    no_delivery_counters.py [source-root]
    no_delivery_counters.py --selftest     the rule, run against a sample it must flag and one it must not
"""

import pathlib
import re
import sys

BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.S)
LINE_COMMENT = re.compile(r"//.*")

DECLARATION = re.compile(r"\b(?:val|var|fun)\s+([A-Za-z_][A-Za-z0-9_]*)")
# A delivery word next to a quantity word, in either order, however it is spelled.
COUNTER = re.compile(
    r"(?:sent|deliver\w*|ack\w*|success\w*|failed|failure\w*)(?:count|total|rate|number)"
    r"|(?:count|total|rate|number)of(?:sent|deliver\w*|ack\w*|success\w*|failed)",
    re.I,
)

FLAGGED_SAMPLE = """
public class Producer {
    /** how many were sentCount - explaining is allowed */
    public val deliveredCount: Long = 0
}
"""

CLEAN_SAMPLE = """
public class Producer {
    // Deliberately no deliveredCount here: see docs/features/feature-backpressure-and-accounting.md
    public suspend fun send(record: ProducerRecord): RecordMetadata = TODO()
    public val outstanding: Int = 0
}
"""


def strip_comments(source: str) -> str:
    return LINE_COMMENT.sub("", BLOCK_COMMENT.sub("", source))


def offenders(source: str) -> list[str]:
    code = strip_comments(source)
    return [name for name in DECLARATION.findall(code) if COUNTER.search(name)]


def selftest() -> int:
    """A guard nobody has seen fail is a guard with an unknown failure mode."""
    flagged = offenders(FLAGGED_SAMPLE)
    clean = offenders(CLEAN_SAMPLE)
    ok = flagged == ["deliveredCount"] and clean == []
    print(f"  sample with a counter -> flagged {flagged}")
    print(f"  sample without one, that names one in a comment -> flagged {clean}")
    print("selftest: PASS" if ok else "selftest: FAIL - the rule does not do what it says")
    return 0 if ok else 1


def main() -> int:
    if "--selftest" in sys.argv[1:]:
        return selftest()

    root = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else "kafkakn-core/src/commonMain")
    if not root.is_dir():
        print(f"no {root} - nothing checked, which is not the same as nothing wrong")
        return 1

    files = sorted(root.rglob("*.kt"))
    if not files:
        print(f"no Kotlin sources under {root} - nothing checked")
        return 1

    findings = [(path, name) for path in files for name in offenders(path.read_text(encoding="utf-8"))]
    for path, name in findings:
        print(f"  {path}: declares {name!r} - a delivery count the caller will read as a success rate")

    print(f"public surface: {len(files)} files, {len(findings)} delivery counters")
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
