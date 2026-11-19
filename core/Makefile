# /*************************************************************************
# * Copyright 2025 Karthick Jaganathan
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

# StreamWright core (the `streamwright` package)

PACKAGE_NAME = streamwright
# The Python whose environment the targets use (an active pyenv / virtualenv by default)
PYTHON ?= python
# Installation mode (dev, prod, dist); FORCE_DEPS=true reinstalls dependencies
MODE ?= prod
FORCE_DEPS ?= false
# Optional extras to install, comma-separated (e.g. EXTRAS=serve)
EXTRAS ?=
DIST_DIR ?= /tmp/sdist/streamwright

PIP_FORCE := $(if $(filter true,$(FORCE_DEPS)),--force-reinstall,)
TARGET := .$(if $(EXTRAS),[$(EXTRAS)],)
DIST_TARGET := $(PACKAGE_NAME)$(if $(EXTRAS),[$(EXTRAS)],)

.PHONY: install uninstall clean test help verify build

help:
	@echo "StreamWright core - Available commands:"
	@echo ""
	@echo "  make install [MODE=dev|prod|dist] [EXTRAS=serve] - Install $(PACKAGE_NAME) (default: prod; FORCE_DEPS=true)"
	@echo "  make uninstall                    - Uninstall $(PACKAGE_NAME)"
	@echo "  make build                        - Build distribution packages into $(DIST_DIR)"
	@echo "  make clean                        - Clean build artifacts"
	@echo "  make test                         - Run the core tests (pytest)"
	@echo "  make verify                       - Verify the installation"
	@echo "  make help                         - Show this help message"
	@echo ""
	@echo "Installation modes:"
	@echo "  MODE=dev   - Development mode (pip install -e .)"
	@echo "  MODE=prod  - Production mode (pip install .)"
	@echo "  MODE=dist  - Distribution mode (build + install from $(DIST_DIR))"

install:
ifeq ($(MODE),dev)
	@echo "Installing $(PACKAGE_NAME) in development mode..."
	$(PYTHON) -m pip install -e "$(TARGET)" $(PIP_FORCE)
else ifeq ($(MODE),prod)
	@echo "Installing $(PACKAGE_NAME) in production mode..."
	$(PYTHON) -m pip install "$(TARGET)" $(PIP_FORCE)
else ifeq ($(MODE),dist)
	@echo "Installing $(PACKAGE_NAME) from distribution..."
	@$(MAKE) build
	$(PYTHON) -m pip install "$(DIST_TARGET)" --find-links $(DIST_DIR) --force-reinstall
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
	@ls -la $(DIST_DIR)/

uninstall:
	$(PYTHON) -m pip uninstall -y $(PACKAGE_NAME)
	@echo "✅ Uninstallation complete!"

clean:
	find . -type d \( -name "__pycache__" -o -name "*.egg-info" -o -name build -o -name dist -o -name .pytest_cache \) -prune -exec rm -rf {} + 2>/dev/null || true
	@echo "✅ Cleanup complete!"

# From the repository root, as CI and the root Makefile run them
test:
	cd .. && $(PYTHON) -m pytest --import-mode=importlib core/tests -q

verify:
	@echo "Verifying $(PACKAGE_NAME) installation..."
	@$(PYTHON) -m pip show $(PACKAGE_NAME) >/dev/null 2>&1 && echo "✅ $(PACKAGE_NAME) is installed!" || echo "❌ $(PACKAGE_NAME) is not installed!"
	@$(PYTHON) -c "import streamwright.core" 2>/dev/null && echo "✅ streamwright.core import works!" || echo "❌ streamwright.core import failed!"
	@$(PYTHON) -m streamwright.core.cli --help >/dev/null 2>&1 && echo "✅ streamwright command works!" || echo "❌ streamwright command failed!"
	@$(PYTHON) -m streamwright.core.cli validate --help >/dev/null 2>&1 && echo "✅ streamwright validate works!" || echo "❌ streamwright validate failed!"
	@$(PYTHON) -c "import starlette, uvicorn" 2>/dev/null && echo "✅ the serve extra is installed (streamwright serve, worker)" || echo "ℹ️  the serve extra is not installed (EXTRAS=serve for streamwright serve)"
