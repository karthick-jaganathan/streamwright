"""Declarative HTTP connectors and SDK connectors on the source engine: the http catalog, literal query text, and
the error translation the sessions of SDK connectors share."""

import pytest

from streamwright.core.engine.queries import BuiltQuery
from streamwright.core.io.api_calls import make_caller, translate_errors, translate_pages
from streamwright.core.io.http import HttpSourceAdapter
from streamwright.core.jobs.policy import ExecutionPolicy, TransientError, classify_transient
from streamwright.core.runtime.connector_types import ConnectorContext, ConnectorError
from streamwright.core.runtime.redact import Redactor
from streamwright.core.runtime.source_connectors import select_transport
from streamwright.core.sdk.contract import ConnectorDefinition, Field, Operation, check_arguments, resolve_values
from streamwright.core.sdk.operations import http
from streamwright.core.validation import vocabulary


def definition(name, *operations):
    adapters = {"source": "streamwright.core.io.http:HttpSourceAdapter"}
    return ConnectorDefinition(name, name.title(), "test", connection=http.auth_fields(("bearer",)),
                               operations=operations, adapters=adapters)


def test_the_auth_fields_are_the_built_in_authentication_of_the_source_format():
    assert set(http.AUTH_TYPES) == set(vocabulary.AUTH_TYPES)
    fields = {item.name: item for item in http.auth_fields()}
    for kind, keys in vocabulary.AUTH_TYPES.items():
        for name in keys["required"] + keys["optional"]:
            assert name in fields, (kind, name)
    assert set(fields) == {"type"} | {name for keys in vocabulary.AUTH_TYPES.values()
                                      for name in keys["required"] + keys["optional"]}
    assert fields["type"].required and fields["type"].choices == http.AUTH_TYPES
    assert {name for name, item in fields.items() if item.secret} == {
        "token", "value", "password", "client_secret", "refresh_token"}


def test_auth_fields_narrow_to_the_types_a_network_takes_and_mark_more_secrets():
    fields = {item.name: item for item in http.auth_fields(("oauth2_refresh_token", "bearer"), secrets=("client_id",))}
    assert set(fields) == {"type", "token_url", "client_id", "client_secret", "refresh_token", "scopes", "token"}
    assert fields["client_id"].secret and fields["type"].choices == ("oauth2_refresh_token", "bearer")
    with pytest.raises(ValueError, match="unknown authentication type"):
        http.auth_fields(("digest",))


def test_a_definition_offering_the_http_request_runs_on_the_http_transport():
    assert select_transport(definition("rest", http.REQUEST)) == "http"
    assert select_transport(definition("vendor", Operation(service="item", method="read"))) == "sdk"


def test_the_http_adapter_says_requests_are_http_requests():
    connection = resolve_values(definition("rest", http.REQUEST), {"type": "bearer", "token": "t"})
    with pytest.raises(ConnectorError, match="rest is a REST API connector: use an http request"):
        HttpSourceAdapter().open(connection, None)


QUERY = Operation(service="query", method="run", arguments=(
    Field("query", "string", required=True, literal=True), Field("limit", "integer")))
SEARCH = definition("search", QUERY)


def test_query_text_is_literal_when_declared_but_a_built_query_is_accepted():
    assert check_arguments(SEARCH, QUERY, {"query": "SELECT 1"}) == []
    assert check_arguments(SEARCH, QUERY, {"query": BuiltQuery("gaql")}) == []
    (issue,) = check_arguments(SEARCH, QUERY, {"query": "SELECT '{{ config.name }}'"})
    assert (issue.code, issue.path) == ("bad-value", "arguments.query")
    assert QUERY.argument("query").json_schema() == {"type": "string", "description": ""}


def test_a_run_checks_rendered_arguments_where_text_may_contain_braces():
    arguments = {"query": "SELECT '{{ a value }}'"}
    assert check_arguments(SEARCH, QUERY, arguments, rendered=True) == []
    assert [issue.code for issue in check_arguments(SEARCH, QUERY, dict(arguments, limit="ten"), rendered=True)] == [
        "bad-value"]


class VendorError(Exception):
    pass


def translate(exc):
    return ConnectorError("vendor: %s" % exc, code=429, retryable=True) if isinstance(exc, VendorError) else None


def test_translated_errors_are_retried_by_the_context_and_unknown_ones_pass_through():
    attempts, sleeps = [], []

    def flaky():
        attempts.append(1)
        if len(attempts) < 3:
            raise VendorError("slow down")
        return "done"
    context = ConnectorContext("vendor", Redactor([]), sleep=sleeps.append)
    assert make_caller(context, translate)(flaky) == "done" and len(attempts) == 3 and len(sleeps) == 2
    with pytest.raises(KeyError):
        make_caller(context, translate)(lambda: {}["missing"])


def test_retry_codes_are_configuration_the_stream_replaces_the_connectors():
    throttled = ConnectorError("throttled", codes=("QUOTA", 613))
    flagged = ConnectorError("vendor says transient", code="OTHER", retryable=True)
    dropped = ConnectorError("connection dropped", retryable=True)
    defaults = ConnectorContext("vendor", Redactor([]), default_codes=("QUOTA",))
    assert throttled.code == "QUOTA" and defaults.retryable(throttled) and defaults.retryable(flagged)
    replaced = ConnectorContext("vendor", Redactor([]), retry={"codes": ["613"]}, default_codes=("QUOTA",))
    assert replaced.retryable(throttled)  # any of its codes, compared as text
    assert not replaced.retryable(flagged)  # the stream's codes replace the defaults and the vendor's flag
    assert replaced.retryable(dropped) and not replaced.retryable(ConnectorError("bad request", code=400))


def test_a_job_step_or_output_retries_by_the_same_rule():
    keep, replace = ExecutionPolicy.of({}), ExecutionPolicy.of({"retry": {"codes": ["THROTTLED"]}})
    flagged = ConnectorError("vendor says transient", code="OTHER", retryable=True)
    assert classify_transient(keep)(flagged)[0] and not classify_transient(replace)(flagged)[0]
    assert classify_transient(replace)(ConnectorError("slow down", codes=("X", "THROTTLED")))[0]
    assert classify_transient(replace)(TransientError())[0]


def test_errors_raised_while_reading_pages_are_translated():
    def pages():
        yield [1]
        raise VendorError("broke")
    seen = translate_pages(pages(), translate)
    assert next(seen) == [1]
    with pytest.raises(ConnectorError, match="vendor: broke") as caught:
        next(seen)
    assert isinstance(caught.value.__cause__, VendorError)
    with pytest.raises(ConnectorError, match="already") as again, translate_errors(translate):
        raise ConnectorError("already translated")
    assert again.value.__cause__ is None
