# The Makefile of every StreamWright connector package. A package's own Makefile is two lines:
#
#     PACKAGE_NAME = streamwright-<name>
#     include ../../connector.mk
#
# CONNECTOR (the module and connector name) is the package directory's name.

CONNECTOR ?= $(notdir $(CURDIR))
CONNECTORS_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
REPO_ROOT := $(abspath $(CONNECTORS_DIR)/..)
PACKAGE_PATH := $(patsubst $(REPO_ROOT)/%,%,$(CURDIR))

# Installation mode (dev, prod, dist) and the distribution directory
MODE ?= prod
# shared with core: MODE=dist installs the connector's `streamwright` requirement from here
DIST_DIR ?= /tmp/sdist/streamwright

.PHONY: install uninstall clean test help verify build

help:
	@echo "StreamWright $(CONNECTOR) connector - Available commands:"
	@echo ""
	@echo "  make install [MODE=dev|prod|dist] - Install $(PACKAGE_NAME) package (default: prod)"
	@echo "  make uninstall                    - Uninstall $(PACKAGE_NAME) package"
	@echo "  make build                        - Build distribution packages"
	@echo "  make clean                        - Clean build artifacts"
	@echo "  make test                         - Run the connector's tests"
	@echo "  make verify                       - Verify installation"
	@echo "  make help                         - Show this help message"
	@echo ""
	@echo "Installation modes:"
	@echo "  MODE=dev   - Development mode (pip install -e .)"
	@echo "  MODE=prod  - Production mode (pip install .)"
	@echo "  MODE=dist  - Distribution mode (build + install from $(DIST_DIR))"

install:
ifeq ($(MODE),dev)
	@echo "Installing $(PACKAGE_NAME) package in development mode..."
	pip install -e .
else ifeq ($(MODE),prod)
	@echo "Installing $(PACKAGE_NAME) package in production mode..."
	pip install .
else ifeq ($(MODE),dist)
	@echo "Installing $(PACKAGE_NAME) package from distribution..."
	@$(MAKE) build
	pip install $(PACKAGE_NAME) --find-links $(DIST_DIR) --force-reinstall
else
	@echo "❌ Invalid MODE: $(MODE). Use dev, prod, or dist"
	@exit 1
endif
	@echo "✅ Installation complete!"
	@pip list | grep streamwright

build: clean
	@mkdir -p $(DIST_DIR)
	python -m build --outdir $(DIST_DIR)
	@echo "✅ Build complete: $(DIST_DIR)"

uninstall:
	pip uninstall -y $(PACKAGE_NAME)

clean:
	find . -type d \( -name "__pycache__" -o -name "*.egg-info" -o -name build -o -name dist \) -prune -exec rm -rf {} + 2>/dev/null || true

# From the repository root: the examples the connector's tests load are relative to it
test:
	cd $(REPO_ROOT) && python -m pytest $(PACKAGE_PATH)/tests -q --import-mode=importlib

verify:
	@pip show $(PACKAGE_NAME) >/dev/null 2>&1 && echo "✅ $(PACKAGE_NAME) is installed!" || echo "❌ $(PACKAGE_NAME) is not installed!"
	@python -c "import streamwright.connectors.$(CONNECTOR).connector" 2>/dev/null && echo "✅ streamwright.connectors.$(CONNECTOR) import works!" || echo "❌ streamwright.connectors.$(CONNECTOR) import failed!"
	@streamwright connectors | grep -qE '\[installed\]\s+$(CONNECTOR)\b' && echo "✅ $(CONNECTOR) connector is registered!" || echo "❌ $(CONNECTOR) connector is not registered!"
