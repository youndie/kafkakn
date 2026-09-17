#!/usr/bin/env python3
"""`commonMain` must not name a platform.

The project's argument is that one `expect` surface is satisfied by two independent implementations
and that the JVM one can therefore act as an oracle for the native one. That only holds while the
common surface is genuinely common: the moment it mentions `org.apache.kafka` or `kotlinx.cinterop`,
one arm has leaked its shape into the contract and the other is being held to a standard written
from its competitor's internals.

It erodes silently - an import added for one convenient type - so it is a gate rather than a habit.

**Comments are stripped before looking.** The first version of this check grepped the raw text and
reported `KafkaProducer.kt` because its KDoc explains what `rd_kafka_produce` does. Documentation
that names the thing it is warning about is exactly what a good comment looks like; a checker that
cannot tell it from an import trains people to delete the comment.
"""

import pathlib
import re
import sys

FORBIDDEN = (
    "org.apache.kafka",
    "kafka.clients",
    "librdkafka",
    "rdkafka",
    "kotlinx.cinterop",
    "platform.posix",
    "java.",
)

BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.S)
LINE_COMMENT = re.compile(r"//.*")


def strip_comments(source: str) -> str:
    return LINE_COMMENT.sub("", BLOCK_COMMENT.sub("", source))


def main() -> int:
    root = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else "kafkakn-core/src/commonMain")
    if not root.is_dir():
        print(f"no {root} - nothing checked, which is not the same as nothing wrong")
        return 1

    files = sorted(root.rglob("*.kt"))
    if not files:
        print(f"no Kotlin sources under {root} - nothing checked")
        return 1

    findings = []
    for path in files:
        code = strip_comments(path.read_text(encoding="utf-8"))
        for token in FORBIDDEN:
            if token in code:
                findings.append((path, token))

    for path, token in findings:
        print(f"  {path}: names {token!r} outside a comment")

    print(f"commonMain: {len(files)} files, {len(findings)} platform references")
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
