# StreamWright Makefile - Compact & Auto-Discovering
MODE ?= prod
FORCE_DEPS ?= false
CORE_DIR = core
DIST_BASE = /tmp/sdist/streamwright

# Dynamic connector auto-discovery (detects any directory with a pyproject.toml)
READER_DIRS    := $(patsubst %/pyproject.toml,%,$(wildcard connectors/readers/*/pyproject.toml))
ADS_DIRS       := $(patsubst %/pyproject.toml,%,$(wildcard connectors/ads/*/pyproject.toml))
HTTP_DIRS      := $(patsubst %/pyproject.toml,%,$(wildcard connectors/http/*/pyproject.toml))
CONNECTOR_DIRS := $(READER_DIRS) $(ADS_DIRS) $(HTTP_DIRS)

# Derived package names for uninstall (e.g. google_ads -> streamwright-google-ads)
CONNECTOR_PKGS := $(addprefix streamwright-,$(subst _,-,$(notdir $(CONNECTOR_DIRS))))
ALL_PKGS       := streamwright $(CONNECTOR_PKGS)

.PHONY: help check-contracts lint check-vendored install install-all install-core install-connectors install-readers \
	build build-all clean clean-dist verify verify-connectors uninstall \
	test test-all validate _show-packages

# Default target: show help and discovered inventory
help:
	@echo "StreamWright - Available commands:"
	@echo ""
	@echo "  make install [MODE=dev|prod|dist]    - Install streamwright core (default: prod)"
	@echo "  make install-connectors [MODE=...]   - Install all $(words $(CONNECTOR_DIRS)) connectors"
	@echo "  make install-readers [MODE=...]      - Install all $(words $(READER_DIRS)) reader connectors"
	@echo "  make install-<name> [MODE=...]       - Install specific connector (e.g. install-postgres, install-s3)"
	@echo "  make build                           - Build distributions for core and all connectors"
	@echo "  make uninstall                       - Uninstall streamwright and all connectors"
	@echo "  make clean                           - Clean build and test artifacts"
	@echo "  make clean-dist                      - Clean distribution directory"
	@echo "  make verify                          - Verify core installation"
	@echo "  make verify-connectors               - Verify all connector installations"
	@echo "  make test                            - Lint, then run core and reader tests (pytest)"
	@echo "  make test-all                        - Lint, then run all tests including ads and http"
	@echo "  make validate                        - Validate examples/ with streamwright validate --strict"
	@echo "  make check-contracts                 - Check G1/G2 plug-in boundaries and connector packaging"
	@echo "  make lint                            - Check the code standards: ruff (ruff.toml) + scripts/check_code_standards.py"
	@echo "  make check-vendored                  - Check the companion repos' copies of check_code_standards.py are identical"
	@echo ""

	@echo "Installation modes: MODE=prod (pip install .), MODE=dev (pip install -e .), MODE=dist"
	@echo ""
	@echo "Auto-discovered connectors ($(words $(CONNECTOR_DIRS)) total):"
	@echo "  Readers ($(words $(READER_DIRS))): $(notdir $(READER_DIRS))"
	@echo "  Ads     ($(words $(ADS_DIRS))): $(notdir $(ADS_DIRS))"
	@echo "  HTTP    ($(words $(HTTP_DIRS))): $(notdir $(HTTP_DIRS))"

check-contracts:
	@python3 scripts/check_plugin_contract.py
	@python3 scripts/check_connectors.py

# The code standards (docs/design/connector-contract.md section 7); blocking. RUFF=path/to/ruff
RUFF ?= ruff
lint:
	$(RUFF) check .
	python3 scripts/check_code_standards.py --strict

# streamwright-spark and streamwright-mcp vendor scripts/check_code_standards.py: their copies must stay identical.
# COMPANIONS: the folders of their checkouts (one that is missing is skipped).
COMPANIONS ?= ../streamwright-spark ../streamwright-mcp
check-vendored:
	@for repo in $(COMPANIONS); do \
		copy="$$repo/scripts/check_code_standards.py"; \
		if [ -d "$$repo" ]; then \
			cmp scripts/check_code_standards.py "$$copy" || { echo "$$copy differs from scripts/check_code_standards.py"; exit 1; }; \
			echo "$$copy is identical"; \
		else echo "skipped $$repo (not checked out)"; fi; \
	done

# Installation targets
install: install-all
install-all: install-core
	@echo "✅ StreamWright core installed successfully!"
	@$(MAKE) _show-packages

install-core:
	@echo "Installing streamwright in $(MODE) mode..."
	@$(MAKE) -C $(CORE_DIR) install MODE=$(MODE) FORCE_DEPS=$(FORCE_DEPS)

install-connectors:
	@for dir in $(CONNECTOR_DIRS); do \
		echo "Installing $$dir..."; \
		$(MAKE) -C $$dir install MODE=$(MODE) FORCE_DEPS=$(FORCE_DEPS) || exit 1; \
	done
	@echo "✅ All $(words $(CONNECTOR_DIRS)) connectors installed!"

install-readers:
	@for dir in $(READER_DIRS); do \
		echo "Installing $$dir..."; \
		$(MAKE) -C $$dir install MODE=$(MODE) FORCE_DEPS=$(FORCE_DEPS) || exit 1; \
	done
	@echo "✅ All $(words $(READER_DIRS)) reader connectors installed!"

# Generic pattern rule for individual connectors: matches install-postgres, install-google-ads, install-google_ads, etc.
install-%:
	@target="$*"; \
	target_under=$$(echo "$$target" | tr '-' '_'); \
	dir=""; \
	for d in $(CONNECTOR_DIRS); do \
		base=$$(basename "$$d"); \
		if [ "$$base" = "$$target" ] || [ "$$base" = "$$target_under" ]; then \
			dir="$$d"; break; \
		fi; \
	done; \
	if [ -n "$$dir" ]; then \
		echo "Installing $$dir in $(MODE) mode..."; \
		$(MAKE) -C "$$dir" install MODE=$(MODE) FORCE_DEPS=$(FORCE_DEPS); \
	else \
		echo "❌ Connector '$*' not found under connectors/"; exit 1; \
	fi

# Build distributions
build: build-all
build-all: clean-dist
	@echo "Building distributions for all packages..."
	@for dir in $(CORE_DIR) $(CONNECTOR_DIRS); do \
		echo "Building $$dir..."; \
		$(MAKE) -C $$dir build || exit 1; \
	done
	@echo "✅ All distributions built successfully!"
	@find $(DIST_BASE) -name "*.tar.gz" -o -name "*.whl" 2>/dev/null | sort || echo "No distributions found"

clean-dist:
	@rm -rf $(DIST_BASE)
	@echo "✅ Distribution directory cleaned!"

clean:
	@for dir in $(CORE_DIR) $(CONNECTOR_DIRS); do \
		$(MAKE) -C $$dir clean 2>/dev/null || true; \
	done
	find . -type d \( -name "__pycache__" -o -name "*.egg-info" -o -name "build" -o -name "dist" -o -name ".pytest_cache" \) -exec rm -rf {} + 2>/dev/null || true
	@echo "✅ Build and test artifacts cleaned!"

uninstall:
	@echo "Uninstalling StreamWright and all connectors..."
	pip uninstall -y $(ALL_PKGS) 2>/dev/null || true
	@echo "✅ All packages uninstalled!"

verify:
	@echo "Verifying core installation..."
	@$(MAKE) -C $(CORE_DIR) verify

verify-connectors:
	@for dir in $(CONNECTOR_DIRS); do \
		echo ""; \
		$(MAKE) -C $$dir verify; \
	done

test: lint
	python -m pytest --import-mode=importlib core/tests connectors/readers -q

test-all: lint
	python -m pytest --import-mode=importlib core/tests connectors -q

validate:
	streamwright validate --strict examples

_show-packages:
	@echo "Installed StreamWright packages:"
	@pip list | grep -E '^streamwright' || true
