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

"""The PyMySQL connections of the state stores and Spark's merges: sslmode and the ssl_* files as MySQL means them."""

import datetime
import ssl
from types import SimpleNamespace

import pytest

from streamwright.connectors.mysql import CONNECTOR
from streamwright.connectors.mysql.engines import driver
from streamwright.core.sdk.contract import resolve_values

pymysql = pytest.importorskip("pymysql")
x509 = pytest.importorskip("cryptography.x509")


@pytest.fixture
def certificate(tmp_path):
    """(cert, key) files of a self-signed CA "streamwright-test-ca": the CA, and a client key pair."""
    serialization = pytest.importorskip("cryptography.hazmat.primitives.serialization")
    rsa = pytest.importorskip("cryptography.hazmat.primitives.asymmetric.rsa")
    hashes = pytest.importorskip("cryptography.hazmat.primitives.hashes")
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(x509.oid.NameOID.COMMON_NAME, "streamwright-test-ca")])
    now = datetime.datetime.now(datetime.UTC)
    cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
            .serial_number(1).not_valid_before(now).not_valid_after(now + datetime.timedelta(days=1))
            .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True).sign(key, hashes.SHA256()))
    cert_file, key_file = tmp_path / "ca.pem", tmp_path / "key.pem"
    cert_file.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_file.write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.TraditionalOpenSSL,
                                           serialization.NoEncryption()))
    return str(cert_file), str(key_file)


def _pymysql(**connection):
    """The PyMySQL connection (not connected) of a resolved connection."""
    resolved = resolve_values(CONNECTOR, dict({"host": "db", "database": "shop", "user": "u"}, **connection))
    return pymysql.connect(**driver.connect_arguments(resolved), defer_connect=True)


def test_required_tls_is_required_without_verifying_the_certificate():
    connection = _pymysql(sslmode="required")
    assert connection._ssl_required and connection.ctx.verify_mode == ssl.CERT_NONE


@pytest.mark.parametrize(("mode", "check_hostname"), [("verify_ca", False), ("verify_identity", True)])
def test_verification_keeps_the_configured_ca_and_client_key_pair(certificate, mode, check_hostname, monkeypatch):
    loaded = []
    monkeypatch.setattr(ssl.SSLContext, "load_cert_chain",
                        lambda self, *args, **kwargs: loaded.append((*args, kwargs.get("keyfile"))))
    cert, key = certificate
    connection = _pymysql(sslmode=mode, options={"ssl_ca": cert, "ssl_cert": cert, "ssl_key": key})
    assert connection._ssl_required and connection.ctx.verify_mode == ssl.CERT_REQUIRED
    assert connection.ctx.check_hostname is check_hostname and loaded == [(cert, key)]
    assert any(dict(item[0] for item in ca["subject"]).get("commonName") == "streamwright-test-ca"
               for ca in connection.ctx.get_ca_certs())


def test_disabled_and_preferred_tls():
    assert not _pymysql(sslmode="disabled").ssl
    preferred = _pymysql()
    assert preferred.ssl and not preferred._ssl_required


def test_sparks_merge_connection_has_the_connections_tls_and_socket(monkeypatch):
    spark = pytest.importorskip("streamwright.connectors.mysql.engines.spark")
    captured = {}
    monkeypatch.setattr(spark.jdbc, "write_table", lambda *args, connect, **kwargs: connect())
    monkeypatch.setattr(driver.pymysql, "connect", lambda **kwargs: captured.update(kwargs))
    connection = resolve_values(CONNECTOR, {"host": "db", "database": "shop", "user": "u", "sslmode": "required",
                                            "options": {"socket": "/run/mysqld.sock"}})
    spark.MySQLSparkAdapter().write(SimpleNamespace(connection=connection), None,
                                    CONNECTOR.operation("database", "write"), {})
    assert isinstance(captured["ssl"], ssl.SSLContext) and captured["unix_socket"] == "/run/mysqld.sock"
