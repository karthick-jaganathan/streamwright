"""
Engine I/O helpers, by feature, composed by connector adapters (connector contract v1, mirroring
`streamwright.spark.io`): functions and frozen configuration objects, no class hierarchies.

- `files`: object-store files for the `source` and `duckdb` engines (store facts, locations inside a connection's
  roots, reads, COPY writes); `file_rules`: the engines' capabilities for the file operations and their semantic
  checks (importable without DuckDB); `locations`: store facts and URL rules; `store_reader`: the source engine's
  locked DuckDB reader of one store; `file_formats`: formats and options.
"""
