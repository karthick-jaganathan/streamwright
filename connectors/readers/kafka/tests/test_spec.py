"""Kafka's declared connector contract and engine capabilities."""

import subprocess
import sys

from streamwright.connectors.kafka import CONNECTOR
from streamwright.core.sdk.contract import check_arguments, check_connection, guide_text
from streamwright.core.sdk.messaging import SOURCE_CAPABILITIES
from streamwright.core.sdk.testing import certify
from streamwright.spark.io.messaging import CAPABILITIES

TestCertification = certify(CONNECTOR, (SOURCE_CAPABILITIES, CAPABILITIES))


def test_listing_does_not_import_the_vendor_or_engine():
    command = ("import sys, streamwright.connectors.kafka; "
               "print(sorted(x for x in ('confluent_kafka', 'pyspark', 'streamwright.connectors.kafka.client') "
               "if x in sys.modules))")
    assert subprocess.check_output([sys.executable, "-c", command], text=True).strip() == "[]"


def test_secret_and_sasl_rules():
    declared = {"bootstrap_servers": "broker:9092", "username": "{{ secrets.user }}",
                "password": "{{ secrets.password }}", "security_protocol": "SASL_SSL", "sasl_mechanism": "PLAIN"}
    assert check_connection(CONNECTOR, declared) == []
    assert [issue.code for issue in check_connection(CONNECTOR, dict(declared, password="plaintext"))] == [
        "literal-secret"]
    assert CONNECTOR.describe()["secret_keys"] == ["username", "password"]


def test_source_and_spark_accept_only_their_operations():
    read = CONNECTOR.operation("message", "read")
    consume = CONNECTOR.operation("message", "consume")
    assert [operation.name for operation in CONNECTOR.operations_for(SOURCE_CAPABILITIES)] == ["message.read"]
    assert [operation.name for operation in CONNECTOR.operations_for(CAPABILITIES)] == [
        "message.consume", "message.publish"]
    assert check_arguments(CONNECTOR, read, {"destination": "arrivals", "max_wait": "2s"},
                           SOURCE_CAPABILITIES) == []
    assert [issue.code for issue in check_arguments(CONNECTOR, consume, {
        "destination": "arrivals", "group_id": "workers", "max_messages": 0}, CAPABILITIES)] == ["bad-value"]
    assert [issue.code for issue in check_arguments(CONNECTOR, consume, {
        "destination": "arrivals"}, CAPABILITIES)] == ["missing-key"]
    assert [issue.code for issue in check_arguments(CONNECTOR, consume, {
        "destination": "arrivals", "group_id": "workers", "max_wait": "soon"}, CAPABILITIES)] == [
        "bad-value"]


def test_guide_tables_come_from_the_definition():
    guide = guide_text(CONNECTOR, "streamwright.connectors.kafka", (SOURCE_CAPABILITIES, CAPABILITIES))
    assert [line for line in guide.splitlines() if line.startswith("## ")] == [
        "## Connection", "## Operations", "## Engines"]
    assert "`message.consume`" in guide and "`group_id`" in guide
