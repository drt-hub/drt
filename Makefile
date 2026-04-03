.PHONY: install dev lint fmt test clean sync-skills check-skills sync-version release-check topics sync-labels

# ── Development ────────────────────────────────────────────────────────────────

install:
	uv pip install -e .

dev:
	uv pip install -e ".[dev,bigquery]"

lint:
	ruff check drt tests
	mypy drt

fmt:
	ruff format drt tests
	ruff check --fix drt tests

test:
	pytest

clean:
	rm -rf dist build .eggs *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type d -name .pytest_cache -exec rm -rf {} +

# ── SSoT automation ────────────────────────────────────────────────────────────

sync-skills:  ## Copy skills/drt/skills/*/SKILL.md → .claude/commands/ (strip frontmatter)
	@for skill in skills/drt/skills/*/SKILL.md; do \
		name=$$(basename $$(dirname $$skill)); \
		awk 'BEGIN{skip=0} /^---$$/{skip++;next} skip<2{next} {print}' $$skill \
			> .claude/commands/$$name.md; \
		echo "  synced $$name"; \
	done
	@echo "✓ Skills synced (maintainer-only commands untouched)"

check-skills:  ## Verify .claude/commands/ matches skills (CI gate)
	@fail=0; \
	for skill in skills/drt/skills/*/SKILL.md; do \
		name=$$(basename $$(dirname $$skill)); \
		expected=$$(awk 'BEGIN{skip=0} /^---$$/{skip++;next} skip<2{next} {print}' $$skill); \
		actual=$$(cat .claude/commands/$$name.md 2>/dev/null || echo ""); \
		if [ "$$expected" != "$$actual" ]; then \
			echo "✗ $$name.md is out of sync with SKILL.md"; \
			fail=1; \
		fi; \
	done; \
	if [ $$fail -eq 1 ]; then \
		echo "Run 'make sync-skills' to fix."; \
		exit 1; \
	fi; \
	echo "✓ All skills in sync"

sync-version:  ## Propagate version from pyproject.toml to all plugin JSONs
	@python3 scripts/sync-version.py

release-check:  ## Run automated release consistency checks
	@bash scripts/release-check.sh

# ── Repo maintenance (maintainer only) ───────────────────────────────────────

topics:  ## Sync repository topics to GitHub
	gh repo edit drt-hub/drt \
	  --add-topic reverse-etl \
	  --add-topic dbt \
	  --add-topic bigquery \
	  --add-topic duckdb \
	  --add-topic python \
	  --add-topic cli \
	  --add-topic etl \
	  --add-topic data-engineering \
	  --add-topic postgres

sync-labels:  ## Trigger label sync workflow on GitHub
	gh workflow run sync-labels.yml --repo drt-hub/drt
