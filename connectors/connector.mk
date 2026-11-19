# /*************************************************************************
# * Copyright 2026 Karthick Jaganathan
# *
# * Licensed under the Apache License, Version 2.0 (the "License");
# * you may not use this file except in compliance with the License.
# * You may obtain a copy of the License at
# *
# * https://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# **************************************************************************/

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

# The Python whose environment the targets use (an active pyenv / virtualenv by default)
PYTHON ?= python
# Installation mode (dev, prod, dist) and the distribution directory; FORCE_DEPS=true reinstalls dependencies
MODE ?= prod
FORCE_DEPS ?= false
PIP_FORCE := $(if $(filter true,$(FORCE_DEPS)),--force-reinstall,)
# shared with core: MODE=dist installs the connector's `streamwright` requirement from here
DIST_DIR ?= /tmp/sdist/streamwright

.PHONY: install uninstall clean test help verify build

help:
	@echo "StreamWright $(CONNECTOR) connector - Available commands:"
	@echo ""
	@echo "  make install [MODE=dev|prod|dist] - Install $(PACKAGE_NAME) package (default: prod; FORCE_DEPS=true)"
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
	$(PYTHON) -m pip install -e . $(PIP_FORCE)
else ifeq ($(MODE),prod)
	@echo "Installing $(PACKAGE_NAME) package in production mode..."
	$(PYTHON) -m pip install . $(PIP_FORCE)
else ifeq ($(MODE),dist)
	@echo "Installing $(PACKAGE_NAME) package from distribution..."
	@$(MAKE) build
	$(PYTHON) -m pip install $(PACKAGE_NAME) --find-links $(DIST_DIR) --force-reinstall
else
	@echo "❌ Invalid MODE: $(MODE). Use dev, prod, or dist"
	@exit 1
endif
	@echo "✅ Installation complete!"
	@$(PYTHON) -m pip list | grep streamwright

build: clean
	@mkdir -p $(DIST_DIR)
	$(PYTHON) -m build --outdir $(DIST_DIR)
	@echo "✅ Build complete: $(DIST_DIR)"

uninstall:
	$(PYTHON) -m pip uninstall -y $(PACKAGE_NAME)

clean:
	find . -type d \( -name "__pycache__" -o -name "*.egg-info" -o -name build -o -name dist \) -prune -exec rm -rf {} + 2>/dev/null || true

# From the repository root: the examples the connector's tests load are relative to it
test:
	cd $(REPO_ROOT) && $(PYTHON) -m pytest $(PACKAGE_PATH)/tests -q --import-mode=importlib

verify:
	@$(PYTHON) -m pip show $(PACKAGE_NAME) >/dev/null 2>&1 && echo "✅ $(PACKAGE_NAME) is installed!" || echo "❌ $(PACKAGE_NAME) is not installed!"
	@$(PYTHON) -c "import streamwright.connectors.$(CONNECTOR).spec" 2>/dev/null && echo "✅ streamwright.connectors.$(CONNECTOR) import works!" || echo "❌ streamwright.connectors.$(CONNECTOR) import failed!"
	@$(PYTHON) -m streamwright.core.cli connectors 2>/dev/null | grep -qx "Name: $(CONNECTOR)" && echo "✅ $(CONNECTOR) connector is registered!" || echo "❌ $(CONNECTOR) connector is not registered!"
