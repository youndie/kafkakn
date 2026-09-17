# One gate, and CI runs exactly this target.
#
# A local check set that differs from the CI one turns "green here, red there" into the normal state
# of affairs, and then neither is read. Whatever is not in `make check` is not a gate.
#
# NOT docs-bootstrap's own Makefile, which is parameterised for its `example/` tree: copied verbatim
# it printed "no docs tree - nothing checked" three times and would have gone green having checked
# nothing at all. A gate that cannot find its subject must say so, which is what `guard` below is.

PY ?= python3

.PHONY: check gate guard report fix help

help:
	@echo "make check   - the gate: blocking checks, exactly what CI runs"
	@echo "make report  - non-blocking: BDD coverage, code anchors"
	@echo "make fix     - regenerate the backlog index and the coverage map"

check: gate report

# Blocking. A failure here means the documentation contradicts itself, which is a defect and not a
# matter of opinion.
gate: guard
	$(PY) scripts/backlog_index.py --check
	$(PY) scripts/docs_check.py
	$(PY) scripts/coverage_map.py --check

# The subject has to exist before any verdict about it means anything. Each script below prints
# "nothing checked" and exits zero on an empty tree, so without this a deleted docs/ is a green run.
guard:
	@test -d docs || { echo "no docs/ tree - the gate has no subject"; exit 1; }
	@n=$$(ls docs/backlog/B-*.md 2>/dev/null | wc -l); \
	  test "$$n" -gt 0 || { echo "no backlog items - the index check would pass vacuously"; exit 1; }; \
	  echo "guard: docs/ present, $$n backlog items"

# Non-blocking, read by a person. Anchors point at code that does not exist yet - nothing is
# implemented - so the NOT FOUND list is expected to be long until stage 0 closes. That is the one
# legitimate exception; when the code exists, an entry there is a defect.
report:
	$(PY) scripts/bdd_report.py
	$(PY) scripts/code_anchors.py --repos .

fix:
	$(PY) scripts/backlog_index.py
	$(PY) scripts/coverage_map.py --fix
