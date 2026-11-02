"""
The file operations of object stores: `object.read` and `object.write`, with the union of the arguments the engines
know. The engines' EngineCapabilities say which of them each engine takes.
"""

from typing import Final

from streamwright.core.sdk.contract import Field, Operation, OperationKind

__all__ = ["NO_QUERY", "READ", "READ_FORMATS", "WRITE", "WRITE_FORMATS", "WRITE_MODES"]

READ_FORMATS: Final = ("auto", "csv", "tsv", "json", "jsonl", "parquet")
WRITE_FORMATS: Final = ("csv", "tsv", "json", "jsonl", "parquet")
WRITE_MODES: Final = ("error", "overwrite", "append")
# no `?` (a URL's query could carry connection settings), `%`, `#` or backslash in a path
NO_QUERY: Final = r"^[^?%#\\]*$"
_PATH: Final = "a URL of the connection's scheme or a key relative to the first root"

READ: Final = Operation(
    service="object",
    method="read",
    kind=OperationKind.READ,
    description="Read csv, tsv, json, jsonl and parquet files inside the connection's roots.",
    arguments=(
        Field("path", ("string", "list"), required=True, item_type="string", pattern=NO_QUERY,
              description=_PATH + "; a glob; or a list of them"),
        Field("format", "string", default="auto", choices=READ_FORMATS, description="auto: by extension"),
        Field("options", "mapping", description="the format's reader options (delimiter, header, columns, ...)"),
        Field("on_missing", "string", default="skip", choices=("skip", "error"),
              description="a path that matches nothing: skip it with a warning, or fail"),
        Field("match", "string", description="a regex the keys below the folder `path` must fully match"),
        Field("recursive", "boolean", default=False, description="with match: sub-folders too"),
        Field("schema_mode", "string", default="by_name", choices=("by_name", "strict"),
              description="files whose columns differ: union them by name, or reject them"),
        Field("columns", ("string", "list", "mapping"),
              description="header-to-canonical column rules (typed), optionally keyed by a path variable"),
        Field("column_groups", "list", item_type="mapping",
              description="fold the columns a pattern matches into one typed map column"),
        Field("vocabulary", ("string", "list"), description="the canonical columns and types"),
        Field("on_unmapped_column", "string", default="warn", choices=("ignore", "warn", "reject"),
              description="a column no rule of `columns` maps"),
        Field("path_pattern", "string", description="a regex whose named groups become columns"),
        Field("row_index", ("boolean", "string", "mapping"), description="a per-file row number"),
        Field("on_mismatch", "string", default="error", choices=("skip", "error"),
              description="a file of an unknown format or that matches no schema: skip it, or fail"),
        Field("row_errors", "boolean", default=False,
              description="add a _row_errors column instead of failing on bad rows"),
        Field("inject_path_vars", "boolean", default=True, description="add path_pattern's named groups as columns"),
    ),
)

WRITE: Final = Operation(
    service="object",
    method="write",
    kind=OperationKind.WRITE,
    idempotent=False,
    description="Write a step's rows as files under a path inside the connection's roots.",
    arguments=(
        Field("path", "string", required=True, pattern=NO_QUERY, description=_PATH + " (a folder with split_by)"),
        Field("format", "string", required=True, choices=WRITE_FORMATS,
              description="json and jsonl both write one JSON object per line"),
        Field("mode", "string", default="error", choices=WRITE_MODES,
              description="error: refuse to write over existing files; overwrite: replace them; append: add files"),
        Field("options", "mapping", description="the format's writer options"),
        Field("split_by", "list", item_type="string", description="one file per distinct value combination"),
        Field("layout", "string", description="the folders of a split, e.g. {client_id}/dt={event_date:%Y-%m-%d}"),
        Field("file_name", "string", description="the file name of a split, e.g. {client_id}_{today}.csv"),
        Field("partition_by", "list", item_type="string", description="Hive-style folders"),
        Field("single_file", "boolean", default=False, description="coalesce the rows into one file"),
        Field("max_splits", "integer", default=500, description="the most files a split may write"),
        Field("on_exists", "string", default="error", choices=("error", "skip", "overwrite"),
              description="a split file that exists already: fail, keep it, or overwrite it"),
    ),
)
