"""
The operations of table formats (Delta Lake): `table.read` and `table.write`, with the union of the arguments the
engines know. The engines' EngineCapabilities say which of them each engine takes.
"""

from typing import Final

from streamwright.core.sdk.contract import Field, Operation, OperationKind
from streamwright.core.sdk.operations.databases import CONDITION_OPERATORS
from streamwright.core.sdk.operations.files import NO_QUERY

__all__ = ["FORMATS", "READ", "SCHEMA_EVOLUTION", "WRITE", "WRITE_MODES"]

FORMATS: Final = ("delta",)
# append: add rows; overwrite: replace what `replace_where` selects, or the `whole_table`; merge: MERGE with
# `condition` and when_* clauses
WRITE_MODES: Final = ("append", "overwrite", "merge")
SCHEMA_EVOLUTION: Final = ("strict", "add_columns")
_PATH: Final = "the table's location: a URL inside the connection's roots, or a path relative to the first root"
_CLAUSES: Final = "{action, set, predicate}"

READ: Final = Operation(
    service="table",
    method="read",
    kind=OperationKind.READ,
    description="Read a table: a version of it, its columns, the rows every `where` condition holds for.",
    arguments=(
        Field("path", "string", required=True, pattern=NO_QUERY, description=_PATH),
        Field("format", "string", default="delta", choices=FORMATS, description="the table format"),
        Field("columns", "list", item_type="string", description="the columns to read (default: all)"),
        Field("where", "list", item_type="mapping",
              description="conditions joined with AND: {column, op, value, type}; op: " + CONDITION_OPERATORS),
        Field("limit", "integer", description="read at most this many rows"),
        Field("version", "integer", description="time travel: read this version of the table"),
        Field("options", "mapping", description="the engine's reader options (Spark: readChangeFeed, ...)"),
    ),
)

WRITE: Final = Operation(
    service="table",
    method="write",
    kind=OperationKind.WRITE,
    description="Write a step's rows into a table as one transaction, replay-safe (an application transaction id per "
                "batch); a missing table is created from the data.",
    arguments=(
        Field("path", "string", required=True, pattern=NO_QUERY, description=_PATH),
        Field("mode", "string", default="append", choices=WRITE_MODES,
              description="append; overwrite: replace_where or whole_table; merge: condition and when_* clauses"),
        Field("partition_by", "list", item_type="string", description="the partition columns of a new table"),
        Field("schema_evolution", "string", default="strict", choices=SCHEMA_EVOLUTION,
              description="strict: refuse new columns; add_columns: add them"),
        Field("replace_where", "string", description="overwrite: the rows to replace, e.g. d = '{{ batch.ts }}'"),
        Field("whole_table", "boolean", description="overwrite: replace the whole table (without replace_where)"),
        Field("condition", "string", description="merge: the ON condition, using target. and source."),
        Field("target_predicate", "string",
              description="merge: prunes target files, derived from the source rows: "
                          "target.d IN ({{ step.d }})"),
        Field("when_matched", "list", item_type="mapping", description="merge: update or delete " + _CLAUSES),
        Field("when_not_matched", "list", item_type="mapping", description="merge: insert " + _CLAUSES),
        Field("when_not_matched_by_source", "list", item_type="mapping",
              description="merge: update or delete " + _CLAUSES),
        Field("options", "mapping", description="the engine's writer options"),
        Field("table_name", "string", description="register the table in the catalog under this name"),
        Field("schema", "string", description="the catalog schema of table_name (default: the connection's)"),
        Field("schema_location", "string", description="the location of a schema the write creates"),
        Field("schema_comment", "string", description="the comment of a schema the write creates"),
        Field("table_properties", "mapping", description="the table's TBLPROPERTIES"),
        Field("comment", "string", description="the table's comment"),
    ),
)
