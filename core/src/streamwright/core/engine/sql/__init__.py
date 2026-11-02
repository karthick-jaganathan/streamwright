#!/usr/bin/env python
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
"""
SQL in sources (docs/design/source-format.md): each stream shapes what its requests read with the SQL steps of its
`transform`, DuckDB queries over the stream's own request tables and earlier steps; its `export` names the steps
that are written. Streams never read each other's tables.

Each request of a stream is a table named after it, with one row per record the request read:

    record        JSON  the record as the API returned it: record->>'$.campaign.name', (record->>'clicks')::BIGINT
    partition     JSON  the partition's values: partition->>'account_id'
    config        JSON  the source's config values: config->>'currency'
    window_start  DATE  the incremental window (null for requests that do not use it)
    window_end    DATE
    today         DATE

`$name` in a step is the value of the config input `name` (spec.config): a bound parameter, never SQL text, with the
type spec.config gives it (PARAMETER_TYPES: string VARCHAR, integer BIGINT, number DOUBLE, boolean BOOLEAN, date
DATE, list a LIST of its items' type). A value that is not given and has no default is null. Secrets are never
parameters. A value is tested against a list with `x = ANY($ids)`, `list_contains($ids, x)` or `x IN $ids`; IN and
comparisons that take a list as one value (`x IN ($ids)`, `x = $ids`) are reported.

DuckDB runs embedded and locked down: each step is one SELECT statement that reads only its stream's tables (and its
own CTEs) and the table functions range, generate_series, unnest, json_each and json_tree; no files, network,
extensions, settings or logs; one thread and 1GB of memory, spilling to a private temporary folder that is removed
after the run. In `page` mode the steps of one page get 60 seconds, and each step at most 1,000,000 rows; in `run`
mode each step gets 10 minutes.

These limits are best effort inside the process: DuckDB checks the time between batches of rows (one slow function
call runs to its end), and memory outside its buffer manager is not counted. When the queries are not trusted (a
platform running customers' sources), run each connector run in its own container with hard memory, CPU, disk and
time limits.
"""

from streamwright.core.engine.sql.plans import (
                                                 SourcePlan,
                                                 StreamPlan,
                                                 check_source,
                                                 exports_of,
                                                 incremental_exports,
                                                 partition_fields,
                                                 plan_source,
)
from streamwright.core.engine.sql.sandbox import Sandbox
from streamwright.core.engine.sql.tables import (
                                                 COLUMNS,
                                                 MODES,
                                                 PARAMETER_TYPES,
                                                 SqlError,
                                                 as_json,
                                                 param_type,
                                                 table_schema,
)

__all__ = [
                                                 "COLUMNS",
                                                 "MODES",
                                                 "PARAMETER_TYPES",
                                                 "Sandbox",
                                                 "SourcePlan",
                                                 "SqlError",
                                                 "StreamPlan",
                                                 "as_json",
                                                 "check_source",
                                                 "exports_of",
                                                 "incremental_exports",
                                                 "param_type",
                                                 "partition_fields",
                                                 "plan_source",
                                                 "table_schema",
]
