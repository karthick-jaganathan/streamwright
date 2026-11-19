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

"""The PostgreSQL facts every engine shares: URLs, DSNs, timeouts and Spark's statements."""

from streamwright.connectors.postgres import client


def test_the_url_is_percent_encoded_so_any_password_survives():
    url = client.postgres_url([("host", "db"), ("port", 5433), ("database", "my db"), ("user", "u@x"),
                               ("password", "p'@:/ss word"), ("sslmode", "require")])
    assert url == ("postgresql:///?host=db&port=5433&dbname=my%20db&user=u%40x&password=p%27%40%3A%2Fss%20word"
                   "&sslmode=require")
    assert client.postgres_url([("host", "/var/run/postgresql"), ("database", "d"), ("user", None)]) == (
        "postgresql:///?host=%2Fvar%2Frun%2Fpostgresql&dbname=d")


def test_every_dsn_form_becomes_the_same_connection_url():
    forms = ["postgresql://u%40x:p%20w@db:5433/shop?sslmode=require",
             "host=db port=5433 dbname=shop user=u@x password='p w' sslmode=require"]
    assert [dict(client.parameters({"dsn": dsn})) for dsn in forms] == [
        {"host": "db", "port": "5433", "database": "shop", "user": "u@x", "password": "p w", "sslmode": "require"}] * 2


def test_the_structured_keys_and_options_become_parameters_with_the_default_port():
    connection = {"host": "db", "database": "shop", "user": "r", "password": "pw",
                  "options": {"application_name": "sw"}}
    assert client.parameters(connection) == [("host", "db"), ("port", 5432), ("database", "shop"), ("user", "r"),
                                             ("password", "pw"), ("application_name", "sw")]


def test_the_server_applies_the_statement_timeout_through_libpq_options():
    assert client.DIALECT.server_timeout([("host", "h"), ("options", "-c search_path=x")], 1500) == [
        ("host", "h"), ("options", "-c search_path=x -c statement_timeout=1500")]
    assert client.DIALECT.server_timeout([("host", "h")], 30000) == [
        ("host", "h"), ("options", "-c statement_timeout=30000")]


def test_jdbc_values_come_from_a_dsn_too_and_the_spark_driver_requires_tls_by_default():
    values = client.jdbc_values({"dsn": "postgresql://u:pw@db:5433/shop"})
    assert (values["host"], values["port"], values["database"], values["user"]) == ("db", "5433", "shop", "u")
    assert values["properties"] == {}
    assert client.driver_url({"dsn": "postgresql://u:pw@db/shop"}, default_sslmode="require").endswith(
        "&sslmode=require")
    assert client.driver_url({"dsn": "postgresql://u:pw@db/shop?sslmode=disable"},
                             default_sslmode="require").endswith("sslmode=disable")


def test_structured_options_and_the_statement_timeout_reach_jdbc_and_the_driver():
    connection = {"host": "db", "database": "shop", "user": "u", "sslmode": "verify-full", "statement_timeout": "5s",
                  "options": {"connect_timeout": "10", "sslrootcert": "/ca.pem", "options": "-c search_path=x"}}
    assert client.jdbc_values(connection)["properties"] == {
        "sslmode": "verify-full", "connectTimeout": "10", "sslrootcert": "/ca.pem",
        "options": "-c search_path=x -c statement_timeout=5000"}
    assert "options=-c%20search_path%3Dx%20-c%20statement_timeout%3D5000" in client.driver_url(connection)
    assert "options" not in client.driver_url(dict(connection, statement_timeout=None, options={}))


def test_spark_statements():
    assert client.staging_statement('"s"."t"', '"s"."stg"') == (
        'CREATE UNLOGGED TABLE "s"."stg" (LIKE "s"."t" INCLUDING DEFAULTS EXCLUDING CONSTRAINTS EXCLUDING INDEXES)')
    assert client.merge_statement('"t"', '"stg"', ['"id"', '"v"'], ['"id"'], ['"v"']) == (
        'INSERT INTO "t" ("id", "v") SELECT "id", "v" FROM "stg" ON CONFLICT ("id") DO UPDATE SET "v" = EXCLUDED."v"')
    assert client.merge_statement('"t"', '"stg"', ['"id"'], ['"id"'], []).endswith("DO NOTHING")
    assert client.delete_statement('"t"', '"stg"', ['"a"', '"b"']) == (
        'DELETE FROM "t" USING "stg" WHERE "t"."a" = "stg"."a" AND "t"."b" = "stg"."b"')
