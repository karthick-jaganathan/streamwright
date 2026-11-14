#!/usr/bin/env python
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

"""
Which ProviderAdapter serves each provider of a DuckDBJob: the engine's own `duckdb` catalog, then the connector
definitions that list the `duckdb` engine (`streamwright.connectors`). Installed ones are authorized (allowed) before
they are imported.
"""

from collections.abc import Callable, Collection, Iterable
from typing import Any

from streamwright.core.jobs.check.rules import install_hint
from streamwright.core.jobs.duckdb.adapter import BUILT_IN_ADAPTERS, ProviderAdapter
from streamwright.core.jobs.duckdb.definition_adapter import CAPABILITIES, DefinitionDuckDBAdapter
from streamwright.core.runtime.definitions import definition_names, definitions_for

__all__ = ["discover_adapters", "load_adapters"]

EntryPoints = Callable[[], Any] | None


def discover_adapters(entry_points: EntryPoints = None) -> list[str]:
    """The providers that may have a DuckDB adapter (metadata only, nothing imported)."""
    return sorted(set(BUILT_IN_ADAPTERS) | set(definition_names(entry_points)))


def load_adapters(providers: Iterable[str], allowed: Collection[str] | None = None,
                  entry_points: EntryPoints = None) -> tuple[dict[str, ProviderAdapter], dict[str, str]]:
    """({provider: adapter}, {provider: why it has none}) for `providers`; built-ins need no allow-list entry."""
    wanted = sorted(set(providers))
    found: dict[str, ProviderAdapter] = {name: BUILT_IN_ADAPTERS[name]() for name in wanted
                                         if name in BUILT_IN_ADAPTERS}
    definitions, missing = definitions_for(CAPABILITIES.engine, [name for name in wanted if name not in found],
                                           allowed=allowed, entry_points=entry_points)
    found.update((name, DefinitionDuckDBAdapter(definition)) for name, definition in definitions.items())
    missing.update((name, "no DuckDB adapter is installed for provider %r: %s" % (name, install_hint(name)))
                   for name in wanted if name not in found and name not in missing)
    return found, missing
