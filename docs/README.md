# Documentation map

Layers linked by id. Read downwards: why it is built this way, then what it promises, then what it
does.

| Document | What it is |
|---|---|
| [research/research-architecture.md](research/research-architecture.md) | **Start here.** Verified facts with their addresses, the decisions and what was rejected, the hypotheses and where each is settled. The obvious design is wrong in four documented ways and this is where they are. |
| [api/producer-contract.md](api/producer-contract.md) | The `expect` surface and what each call promises. A test cites a place here. |
| [features/](features/) | What the library does: [produce a record](features/feature-produce-a-record.md), [backpressure and accounting](features/feature-backpressure-and-accounting.md), [a secure connection](features/feature-secure-connection.md). |
| [services/](services/) | [kafkakn-core](services/kafkakn-core.md) — the module, its targets, its build. [test-broker](services/test-broker.md) — what the suite runs against, and why it is a real broker. |
| [../backlog.md](../backlog.md) | The queue: goal, stages, decisions, and the generated index. |
| [backlog/](backlog/) | One file per item. |

## The rule that governs the documents

**`main` describes what exists**, and `status: draft` is reserved for a document living in an open
pull request — the checker enforces that on the default branch. A design-only repository does not
get to mark everything `draft` and call it honest: the documents here are `status: active`, and what
is not built is labelled ***target*** in the text, scenario by scenario. That is the format's answer
and it is a better one, because the status field then says where a document is in the workflow while
the prose says what is true.

Today every scenario in every feature carries *target* and nothing is implemented. A scenario that
loses that marker without a test behind it is a lie the next reader will believe.

## Coverage map

Every document on disk, listed once. `[x]` written, `[ ]` planned.

### Research (1)

- [x] [research-architecture](research/research-architecture.md) — why the JVM arm is the oracle, what librdkafka costs, and the four ways the obvious design is wrong

### Services (2)

- [x] [kafkakn-core](services/kafkakn-core.md) — the one published module: targets, the source-set layout, and the quirks that will bite
- [x] [test-broker](services/test-broker.md) — a real broker rather than a fake, and the third parties every assertion goes through

### API (2)

- [x] [producer-contract](api/producer-contract.md) — the `expect` surface, what each call promises, and the five things both actuals must agree on
- [x] [consumer-contract](api/consumer-contract.md) — designed before it is built: the threading each client demands, the shape, twenty defaults read from both artefacts, and what the first consumer will not do; all *target*

### Features (3)

- [x] [feature-produce-a-record](features/feature-produce-a-record.md) — hand over a record, learn where it landed; the whole library in one call
- [x] [feature-backpressure-and-accounting](features/feature-backpressure-and-accounting.md) — the defect this project is shaped around, and the reconciliation that catches it
- [x] [feature-secure-connection](features/feature-secure-connection.md) — TLS through the OpenSSL already in the binary, and why a linked library proves nothing
