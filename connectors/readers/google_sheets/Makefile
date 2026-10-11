# StreamWright google_sheets connector Makefile

PACKAGE_NAME = streamwright-google-sheets
VERSION = 0.0.1

MODE ?= prod
DIST_DIR = /tmp/sdist/streamwright

.PHONY: install uninstall clean test help verify build

help:
	@echo "StreamWright google_sheets connector - Available commands:"
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
	@echo "Installing from distribution directory $(DIST_DIR)..."
	pip install $(PACKAGE_NAME) --find-links $(DIST_DIR) --force-reinstall
else
	@echo "❌ Invalid MODE: $(MODE). Use dev, prod, or dist"
	@exit 1
endif
	@echo "✅ Installation complete!"

build: clean
	@echo "Building distribution packages for $(PACKAGE_NAME)..."
	@mkdir -p $(DIST_DIR)
	python -m build --outdir $(DIST_DIR)
	@echo "✅ Build complete!"

clean:
	@echo "Cleaning build artifacts..."
	rm -rf build/ dist/ *.egg-info/ src/*.egg-info/ .pytest_cache/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

test:
	pytest tests/ -v

verify:
	@python -c "import streamwright.connectors.google_sheets; print('✅ streamwright.connectors.google_sheets import successful')"
