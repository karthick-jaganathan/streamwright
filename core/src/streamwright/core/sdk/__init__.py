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

"""Stable public API for StreamWright connector authors."""

from streamwright.core.config.inputs import InputError, as_date
from streamwright.core.engine.queries import BuiltQuery, QueryError
from streamwright.core.runtime.connector_types import ConnectorContext, ConnectorError, QueryBuilder
from streamwright.core.runtime.contract import ConnectorDefinition, Field, Operation, ResolvedConnection
from streamwright.core.runtime.contract_validation import check_arguments, check_connection, resolve_connection
from streamwright.core.runtime.redact import Redactor
from streamwright.core.sdk.constants import PAGE_SIZE, SDK_VERSION
from streamwright.core.sql.quote import quote_ident, quote_literal, sql_string

__all__ = [
    "PAGE_SIZE",
    "SDK_VERSION",
    "BuiltQuery",
    "ConnectorContext",
    "ConnectorDefinition",
    "ConnectorError",
    "Field",
    "InputError",
    "Operation",
    "QueryBuilder",
    "QueryError",
    "Redactor",
    "ResolvedConnection",
    "as_date",
    "check_arguments",
    "check_connection",
    "quote_ident",
    "quote_literal",
    "resolve_connection",
    "sql_string",
]
