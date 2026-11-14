#!/usr/bin/env bash
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

#
# Build and publish one StreamWright distribution by its PyPI name — works for any
# current or future package (the package directory is resolved from pyproject
# `name`, so new connectors need no change here).
#
# Usage:
#   scripts/publish.sh streamwright-google-ads            # upload to PyPI
#   scripts/publish.sh streamwright-postgres --test       # upload to TestPyPI (dry run)
set -euo pipefail

DIST="${1:?usage: scripts/publish.sh <dist-name> [--test]}"
MODE="${2:-}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# Resolve the package directory whose pyproject `name` matches DIST.
DIR=""
for pp in "$ROOT/core/pyproject.toml" "$ROOT"/connectors/*/*/pyproject.toml; do
  name=$(grep -m1 '^name = ' "$pp" | sed 's/name = //; s/"//g')
  if [ "$name" = "$DIST" ]; then DIR="$(dirname "$pp")"; break; fi
done
[ -n "$DIR" ] || { echo "error: no package named '$DIST' found" >&2; exit 1; }

echo "==> Building $DIST  ($DIR)"
cd "$DIR"
rm -rf dist build
python -m build
python -m twine check dist/*

if [ "$MODE" = "--test" ]; then
  echo "==> Uploading to TestPyPI"
  python -m twine upload --repository testpypi dist/*
else
  echo "==> Uploading to PyPI"
  python -m twine upload dist/*
fi
