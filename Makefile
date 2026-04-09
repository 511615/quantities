PYTHON ?= python

.PHONY: test test-static test-unit test-audit test-integration test-compatibility test-ci info

test:
	$(PYTHON) -m pytest

test-static:
	$(PYTHON) -m ruff check .
	$(PYTHON) -m mypy src
	$(PYTHON) -m pytest tests/static -q

test-unit:
	$(PYTHON) -m pytest tests/unit -q

test-audit:
	$(PYTHON) -m pytest tests/audit/leakage tests/audit/reproducibility -q

test-integration:
	$(PYTHON) -m pytest tests/integration -q

test-compatibility:
	$(PYTHON) -m pytest tests/compatibility -q -m legacy

test-ci:
	$(PYTHON) -m pytest tests/static tests/unit tests/audit/leakage tests/audit/reproducibility tests/integration -q

info:
	$(PYTHON) -m quant_platform.cli.main info
