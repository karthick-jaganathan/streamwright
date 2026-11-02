"""
Connector contract v1 certification (docs/design/connector-contract.md section 7).

`certify(CONNECTOR)` returns a pytest test class generated from the spec alone (no network, no engine libraries),
checked against the engines' EngineCapabilities (default: the core engines'; add `streamwright.spark.io.file_rules`'
to certify the Spark side too):

    from streamwright.connectors.s3 import CONNECTOR
    from streamwright.core.sdk.testing import certify

    TestCertification = certify(CONNECTOR)

The engine suites exercise the adapters; a connector's test module subclasses them and sets the class attributes:

- SourceAdapterContract: `definition`, `connection` (resolved values), `read_call` ((service, method), arguments),
  `expected_records`; override the `backend` fixture to serve the data the call reads.
- DuckDBAdapterContract: the same, with `read_arguments` of a DuckDB read and `expected_records`.
"""

import json
import re
from collections.abc import Mapping, Sequence
from importlib.util import find_spec
from typing import Any

import pytest

from streamwright.core.io.capabilities import DUCKDB_CAPABILITIES, ENGINE_CAPABILITIES
from streamwright.core.jobs.duckdb.adapter import ReadContext
from streamwright.core.jobs.duckdb.session import Session
from streamwright.core.runtime.connector_types import ConnectorContext
from streamwright.core.runtime.contract import ConnectorDefinition, EngineCapabilities, InvalidConnectionError
from streamwright.core.runtime.contract_validation import check_arguments, check_connection, resolve_values
from streamwright.core.runtime.redact import Redactor
from streamwright.core.sdk.engines.duckdb import DuckDBAdapter, DuckDBWriteAdapter
from streamwright.core.sdk.engines.source import SourceAdapter, SourceSession

__all__ = ["DuckDBAdapterContract", "SourceAdapterContract", "certify"]

SECRET_SAMPLE = "certify-SECRET-value-0042"


def _sample(field):
    """A declared value of a field that its checks accept (a reference for anything without a fixed choice)."""
    if field.secret:
        return "{{ secrets.%s }}" % field.name
    if field.choices:
        return field.choices[0]
    return "{{ config.%s }}" % field.name


class _Certification:
    definition: ConnectorDefinition
    capabilities: tuple[EngineCapabilities, ...]

    def declared(self):
        return {field.name: _sample(field) for field in self.definition.connection}

    def engines(self):
        """The capabilities of the engines the connector has an adapter for (and that are known here)."""
        return [item for item in self.capabilities if item.engine in self.definition.engines]

    def test_the_spec_is_well_formed(self):
        definition = self.definition
        assert definition.operations, "a connector offers at least one operation"
        assert definition.engines, "a connector has at least one engine adapter"
        assert json.dumps(definition.describe(*self.capabilities))  # plain data
        assert check_connection(definition, self.declared()) == []

    def test_the_json_schema_is_derived_from_the_spec(self):
        schema = self.definition.json_schema()
        assert schema["properties"]["provider"] == {"const": self.definition.name}
        assert set(schema["properties"]) == {"provider"} | {field.name for field in self.definition.connection}
        assert set(schema["required"]) == {"provider"} | {field.name for field in self.definition.required_fields}
        assert schema["additionalProperties"] is True  # open to vendor arguments (contract v1.2 §6a)
        for engine in self.definition.engines:
            assert self.definition.json_schema(engine)["properties"] == schema["properties"]

    def test_secret_fields_take_only_secret_references(self):
        for field in self.definition.secret_fields:
            for written in ("written-in-the-file", "{{ config.%s }}" % field.name, "x{{ secrets.a }}"):
                issues = check_connection(self.definition, dict(self.declared(), **{field.name: written}))
                assert [issue.code for issue in issues] == ["literal-secret"], (field.name, written)
                assert written not in str([issue.message for issue in issues])
        for field in self.definition.connection:
            if not field.secret:
                issues = check_connection(self.definition, dict(self.declared(), **{field.name: "{{ secrets.a }}"}))
                assert [issue.code for issue in issues] == ["secret-placement"], field.name

    def test_secret_values_never_show(self):
        values = {field.name: SECRET_SAMPLE + field.name for field in self.definition.secret_fields}
        if not values:
            pytest.skip("the connector declares no secret fields")
        try:
            connection = resolve_values(self.definition, values)
        except InvalidConnectionError as exc:  # e.g. required fields missing: its message keeps the values out
            assert SECRET_SAMPLE not in str(exc)
            return
        assert set(connection.secret_values) == set(values.values())
        assert SECRET_SAMPLE not in connection.redact(" ".join(values.values())) + repr(connection)

    def test_every_engine_adapter_runs_an_operation_and_exists(self):
        for engine in self.engines():
            assert self.definition.operations_for(engine), "the %s adapter runs no operation" % engine.engine
        for engine, target in self.definition.adapters.items():
            module = target.partition(":")[0]
            assert find_spec(module) is not None, "the %s adapter module %s does not exist" % (engine, module)


    def test_each_engine_takes_only_the_arguments_it_supports(self):
        for engine in self.engines():
            for operation in self.definition.operations_for(engine):
                schema = operation.json_schema(engine)["properties"]
                for argument in operation.arguments:
                    issues = check_arguments(self.definition, operation, {argument.name: _sample(argument)}, engine)
                    unsupported = [issue for issue in issues if issue.code == "unsupported-argument"]
                    assert bool(unsupported) == (argument.name not in schema), (engine.engine, operation.name,
                                                                               argument.name)


def certify(definition: ConnectorDefinition,
            capabilities: tuple[EngineCapabilities, ...] = ENGINE_CAPABILITIES) -> type:
    """
    The certification test class of a connector definition (assign it to a `Test*` name in a test module), checked
    against the engines' `capabilities`.
    """
    return type("Certify%s" % re.sub(r"(^|_)(\w)", lambda m: m.group(2).upper(), definition.name), (_Certification,),
                {"definition": definition, "capabilities": tuple(capabilities), "__test__": True})


class _AdapterContract:
    """What both engine suites share: the definition and a resolved connection."""
    definition: ConnectorDefinition = None
    connection: Mapping[str, Any] = None
    expected_records: Sequence[Mapping[str, Any]] = None
    __test__ = False

    @pytest.fixture
    def backend(self):
        """Serves the data the read call reads (override: e.g. an object store stand-in)."""
        return

    def resolved(self):
        return resolve_values(self.definition, self.connection)


class SourceAdapterContract(_AdapterContract):
    """The `source` adapter: it opens a session on a resolved connection that reads pages of records."""
    read_call: tuple = None

    def context(self, connection):
        return ConnectorContext(self.definition.name, Redactor(connection.secret_values))

    def test_the_adapter_follows_the_protocol(self):
        assert isinstance(self.definition.load_adapter("source"), SourceAdapter)

    def test_a_session_reads_pages_of_records(self, backend):
        connection = self.resolved()
        (service, method), arguments = self.read_call
        context = self.context(connection)
        session = self.definition.load_adapter("source").open(connection, context)
        try:
            assert isinstance(session, SourceSession)
            pages = list(session.read(self.definition.operation(service, method), arguments, context))
        finally:
            session.close()
        session.close()  # closing twice is harmless
        assert all(isinstance(page, list) and all(isinstance(record, dict) for record in page) for page in pages)
        assert [record for page in pages for record in page] == list(self.expected_records)


class DuckDBAdapterContract(_AdapterContract):
    """The `duckdb` adapter: configure() sets the connection up in a session; read_sql() reads its data."""
    read_arguments: Mapping[str, Any] = None

    def test_the_adapter_follows_the_protocol(self):
        assert isinstance(self.definition.load_adapter("duckdb"), DuckDBAdapter)

    def test_a_writing_connector_has_a_writing_adapter(self):
        writes = [item for item in self.definition.operations_for(DUCKDB_CAPABILITIES) if item.kind == "write"]
        assert isinstance(self.definition.load_adapter("duckdb"), DuckDBWriteAdapter) == bool(writes)

    def test_read_sql_reads_the_data_without_credentials_in_its_text(self, backend):
        connection, adapter = self.resolved(), self.definition.load_adapter("duckdb")
        operation = next(item for item in self.definition.operations_for(DUCKDB_CAPABILITIES) if item.kind == "read")
        session = Session()
        try:
            adapter.configure(session, "certify", connection)
            context = ReadContext(session, "certify", connection, {"name": "certify"}, batch=None, pinned={},
                                  limits={}, values=None)
            sql = adapter.read_sql(context, operation, self.read_arguments)
            assert not any(value in sql for value in connection.secret_values)  # credentials go in secrets only
            if self.expected_records is not None:
                cursor = session.con.execute(sql)
                columns = [column[0] for column in cursor.description]
                records = [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]
                assert records == list(self.expected_records)
        finally:
            session.close()
