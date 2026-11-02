"""
The operations of relational databases: `database.query`, `database.table` and `database.write`, with the union of the
arguments the engines know. The engines' EngineCapabilities say which of them each engine takes.
"""

from typing import Final

from streamwright.core.sdk.contract import Field, Operation, OperationKind

__all__ = ["CONDITION_OPERATORS", "QUERY", "TABLE", "WRITE", "WRITE_MODES"]

# `mode` of a write: add rows; replace the table; merge on `primary_key` (update matches, insert the rest); delete
# the rows whose `primary_key` matches
WRITE_MODES: Final = ("append", "overwrite", "merge", "delete")
CONDITION_OPERATORS: Final = "=, !=, <>, <, <=, >, >=, LIKE, NOT LIKE, IN, NOT IN, IS NULL, IS NOT NULL"
_OPTIONS: Final = Field("options", "mapping", description="the engine's driver options (Spark: JDBC fetchsize, "
                                                          "batchsize, ...)")

QUERY: Final = Operation(
    service="database",
    method="query",
    kind=OperationKind.READ,
    description="Run one read-only SELECT; `$name` parameters take the values of `params`.",
    arguments=(
        Field("query", "string", required=True, description="one SELECT, with $name for the values in params"),
        Field("params", "mapping", description="the values of the query's $name parameters"),
        _OPTIONS,
    ),
)

TABLE: Final = Operation(
    service="database",
    method="table",
    kind=OperationKind.READ,
    description="Read a table: its columns, the rows every `where` condition holds for.",
    arguments=(
        Field("table", "string", required=True, description="the table's name (quoted: any text)"),
        Field("schema", "string", description="its schema (default: the database's default schema)"),
        Field("columns", "list", item_type="string", description="the columns to read (default: all)"),
        Field("where", "list", item_type="mapping",
              description="conditions joined with AND: {column, op, value, type}; op: " + CONDITION_OPERATORS),
        _OPTIONS,
    ),
)

WRITE: Final = Operation(
    service="database",
    method="write",
    kind=OperationKind.WRITE,
    idempotent=False,
    description="Write a step's rows into a table; a missing table is created from the data where the engine can.",
    arguments=(
        Field("table", "string", required=True, description="the target table: table or schema.table"),
        Field("mode", "string", default="append", choices=WRITE_MODES,
              description="append; overwrite: replace the table; merge: update or insert on primary_key; delete: "
                          "remove the rows matching on primary_key"),
        Field("primary_key", "list", item_type="string", description="the key columns of merge and delete"),
        Field("update", "list", item_type="string",
              description="merge: the columns a match updates (default: every column but the key)"),
        Field("staging_table", "string",
              description="merge and delete: a pre-created staging table (default: a temporary one the run creates "
                          "and drops, which needs CREATE and DROP privileges)"),
        _OPTIONS,
    ),
)
