# Copyright 2025 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""
The deltalake connector on the `duckdb` engine (kind: DuckDBJob): reads through DuckDB's delta extension
(`read: delta`); writes through delta-rs (extra `duckdb`) under the shared table-format write rules - an overwrite
replaces what `replace_where` selects or the `whole_table`, a merge has its `condition` and when_* clauses,
`target_predicate` and `replace_where` may use `{{ step.column }}` (rendered as escaped literals from the batch's
pinned values), schemas are strict unless `schema_evolution: add_columns`. Every write commits a delta-rs application
transaction (`<job>:<output>:<store identity>`, version = the batch sequence), so replaying a batch whose write
committed is skipped and a different state store never collides on the same table.
"""

from collections.abc import Mapping
from typing import Any

from streamwright.connectors.deltalake import client
from streamwright.core.sdk import quote_literal
from streamwright.core.sdk.contract import Operation, ResolvedConnection
from streamwright.core.sdk.engines.duckdb import (
    AdapterError,
    DuckDBSession,
    ReadContext,
    WriteContext,
    WriteReceipt,
    evolves_schema,
    merge_condition,
    render_target_predicate,
)

try:  # the optional extra `duckdb`: reads work without it
    from deltalake import CommitProperties, DeltaTable, Transaction, write_deltalake
    from deltalake.exceptions import TableNotFoundError
except ImportError:
    write_deltalake = None

__all__ = ["DeltaLakeDuckDBAdapter"]


def _locate(session: DuckDBSession, connection: Mapping[str, Any], path: str) -> str:
    """A table path inside the roots (local roots relative to the job's folder)."""
    try:
        return client.resolve_path(path, client.normalize_roots(connection["roots"], session.local_path(".")))
    except ValueError as exc:
        raise AdapterError(str(exc)) from None


def _with_clauses(merger: Any, spec: Any) -> Any:
    for clause in spec.when_matched:
        if clause["action"] == "delete":
            merger = merger.when_matched_delete(predicate=clause.get("predicate"))
        elif clause.get("set"):
            merger = merger.when_matched_update(dict(clause["set"]), predicate=clause.get("predicate"))
        else:
            merger = merger.when_matched_update_all(predicate=clause.get("predicate"))
    for clause in spec.when_not_matched:
        merger = (merger.when_not_matched_insert(dict(clause["set"]), predicate=clause.get("predicate"))
                  if clause.get("set") else merger.when_not_matched_insert_all(predicate=clause.get("predicate")))
    for clause in spec.when_not_matched_by_source:
        merger = (merger.when_not_matched_by_source_delete(predicate=clause.get("predicate"))
                  if clause["action"] == "delete" else merger.when_not_matched_by_source_update(
                      dict(clause.get("set") or {}), predicate=clause.get("predicate")))
    return merger


def _open_table(location: str, options: Mapping[str, str]) -> Any:
    try:
        return DeltaTable(location, storage_options=dict(options))
    except TableNotFoundError:
        return None                       # only "no table yet": credential and network errors propagate


def _predicate(context: WriteContext, table: Any) -> str | None:
    spec = context.spec
    text = {"overwrite": spec.replace_where, "merge": spec.target_predicate}.get(spec.mode)
    if table is None or text is None:
        return None
    return render_target_predicate(text, context.values, context.constants,
                                   int(context.limits.get("reference_values", 10000)))


def _write(context: WriteContext, location: str, table: Any, *, commit: Any, options: Mapping[str, str],
           predicate: str | None) -> int:
    """Writes the output's rows (streamed from DuckDB as Arrow) in one commit; the rows written."""
    spec, con = context.spec, context.session.con
    rows = con.execute("SELECT COUNT(*) FROM %s" % context.source).fetchall()[0][0]
    schema_mode = "merge" if evolves_schema(spec) else None
    data = con.sql("SELECT * FROM %s" % context.source).to_arrow_reader()  # nothing else may use `con` now
    try:
        if spec.mode in ("append", "overwrite") or table is None:
            mode = "error" if table is None else spec.mode
            write_deltalake(location, data, mode=mode, predicate=predicate if mode == "overwrite" else None,
                            partition_by=list(spec.partition_by) or None, schema_mode=schema_mode,
                            storage_options=dict(options), commit_properties=commit)
            return rows
        metrics = _with_clauses(table.merge(source=data, predicate=merge_condition(spec.condition, predicate),
                                            source_alias="source", target_alias="target",
                                            merge_schema=evolves_schema(spec), commit_properties=commit),
                                spec).execute()
        return sum(int(metrics.get(key) or 0) for key in (
            "num_target_rows_inserted", "num_target_rows_updated", "num_target_rows_deleted"))
    finally:
        data.close()


class DeltaLakeDuckDBAdapter:
    """`read: delta` through delta_scan; table writes through delta-rs, replay-safe."""

    def configure(self, session: DuckDBSession, name: str, connection: ResolvedConnection) -> None:
        roots = [str(root) for root in connection["roots"]]
        for index, secret in enumerate(client.storage_secrets(connection, roots)):
            session.load_extension(secret.extension)
            for scope in secret.scopes:
                session.create_secret("%s_%s_%d" % (name, secret.secret_type, index), secret.secret_type,
                                      list(secret.parameters), scope=scope)

    def read_sql(self, context: ReadContext, operation: Operation, arguments: Mapping[str, Any]) -> str:
        context.session.load_extension("delta")
        location = _locate(context.session, context.connection, arguments["path"])
        return "SELECT * FROM delta_scan(%s)" % quote_literal(location)

    def write(self, context: WriteContext, operation: Operation, arguments: Mapping[str, Any]) -> WriteReceipt:
        if write_deltalake is None:
            raise AdapterError("Delta writes need the deltalake package: pip install "
                               "\"streamwright-deltalake[duckdb]\"")
        spec = context.spec
        location = _locate(context.session, context.connection, spec.path)
        options = client.storage_options(context.connection)
        app_id = "%s:%s:%s" % (context.batch.job_name, spec.name, context.batch.store_identity)
        version = context.batch.batch_sequence
        table = _open_table(location, options)
        if table is not None and (table.transaction_version(app_id) or 0) >= version:
            return WriteReceipt(spec.name, location, None, version=table.version(), already_committed=True)
        predicate = _predicate(context, table)
        rows = _write(context, location, table, options=options, predicate=predicate,
                      commit=CommitProperties(app_transactions=[Transaction(app_id, version)]))
        return WriteReceipt(spec.name, location, rows, version=DeltaTable(location, storage_options=options).version(),
                            predicate=predicate)
