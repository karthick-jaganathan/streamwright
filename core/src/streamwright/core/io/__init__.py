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
Engine I/O helpers, by feature, composed by connector adapters (connector contract v1, mirroring
`streamwright.spark.io`): functions and frozen configuration objects, no class hierarchies.

- `files`: object-store files for the `source` and `duckdb` engines (store facts, locations inside a connection's
  roots, reads, COPY writes); `file_rules`: the engines' capabilities for the file operations and their semantic
  checks (importable without DuckDB); `locations`: store facts and URL rules; `store_reader`: the source engine's
  locked DuckDB reader of one store; `file_formats`: formats and options; `downloads`: the rows of the files async
  jobs produce (csv, jsonl, json; zip or gzip). `database_rules`, `message_rules`: the engines' capabilities for the
  database/table and message operations; `capabilities`: each core engine's, combined.
"""
