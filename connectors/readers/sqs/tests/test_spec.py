"""SQS's declared contract and its engine-specific operation support."""

import subprocess
import sys

from streamwright.connectors.sqs import CONNECTOR
from streamwright.core.sdk.contract import check_arguments, check_connection
from streamwright.core.sdk.messaging import SOURCE_CAPABILITIES
from streamwright.core.sdk.testing import certify
from streamwright.spark.io.messaging import CAPABILITIES

TestCertification = certify(CONNECTOR, (SOURCE_CAPABILITIES, CAPABILITIES))
URL = "https://sqs.us-east-1.amazonaws.com/123456789012/arrivals"


def test_listing_does_not_import_vendor_or_engine():
    command = ("import sys, streamwright.connectors.sqs; "
               "print(sorted(x for x in ('boto3', 'pyspark', 'streamwright.connectors.sqs.client') "
               "if x in sys.modules))")
    assert subprocess.check_output([sys.executable, "-c", command], text=True).strip() == "[]"


def test_secret_references_and_connection_shape():
    assert check_connection(CONNECTOR, {"region": "us-east-1", "key_id": "{{ secrets.key }}",
                                        "secret": "{{ secrets.secret }}"}) == []
    assert [issue.code for issue in check_connection(CONNECTOR, {
        "region": "us-east-1", "key_id": "plaintext"})] == ["literal-secret"]
    assert CONNECTOR.describe()["secret_keys"] == ["key_id", "secret", "session_token"]


def test_one_vocabulary_across_source_and_spark():
    read = CONNECTOR.operation("message", "read")
    consume = CONNECTOR.operation("message", "consume")
    assert check_arguments(CONNECTOR, read, {"destination": URL, "max_messages": 10},
                           SOURCE_CAPABILITIES) == []
    assert [operation.name for operation in CONNECTOR.operations_for(CAPABILITIES)] == [
        "message.consume", "message.publish"]
    assert check_arguments(CONNECTOR, consume, {"destination": URL, "visibility_timeout": 300},
                           CAPABILITIES) == []
    assert [issue.code for issue in check_arguments(CONNECTOR, consume,
        {"destination": "not-a-url"}, CAPABILITIES)] == ["bad-value"]
    assert [issue.code for issue in check_arguments(CONNECTOR, consume, {}, CAPABILITIES)] == ["missing-key"]
