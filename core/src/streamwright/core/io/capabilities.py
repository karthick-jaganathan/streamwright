"""
What each core engine supports, combined once from the io families' declarations: `SOURCE_CAPABILITIES` (streamwright
run) and `DUCKDB_CAPABILITIES` (kind: DuckDBJob). The engines' connector bindings, certification and describe use these.
"""

from typing import Final

from streamwright.core.io import database_rules, file_rules
from streamwright.core.runtime.contract import combine_capabilities
from streamwright.core.sdk import messaging

__all__ = ["DUCKDB_CAPABILITIES", "ENGINE_CAPABILITIES", "SOURCE_CAPABILITIES"]

SOURCE_CAPABILITIES: Final = combine_capabilities(file_rules.SOURCE_CAPABILITIES, database_rules.SOURCE_CAPABILITIES,
                                                  messaging.SOURCE_CAPABILITIES)
DUCKDB_CAPABILITIES: Final = combine_capabilities(file_rules.DUCKDB_CAPABILITIES, database_rules.DUCKDB_CAPABILITIES)
ENGINE_CAPABILITIES: Final = (SOURCE_CAPABILITIES, DUCKDB_CAPABILITIES)
