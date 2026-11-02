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


def test_spark_statements():
    assert client.staging_statement('"s"."t"', '"s"."stg"') == (
        'CREATE UNLOGGED TABLE "s"."stg" (LIKE "s"."t" INCLUDING DEFAULTS EXCLUDING CONSTRAINTS EXCLUDING INDEXES)')
    assert client.merge_statement('"t"', '"stg"', ['"id"', '"v"'], ['"id"'], ['"v"']) == (
        'INSERT INTO "t" ("id", "v") SELECT "id", "v" FROM "stg" ON CONFLICT ("id") DO UPDATE SET "v" = EXCLUDED."v"')
    assert client.merge_statement('"t"', '"stg"', ['"id"'], ['"id"'], []).endswith("DO NOTHING")
    assert client.delete_statement('"t"', '"stg"', ['"a"', '"b"']) == (
        'DELETE FROM "t" USING "stg" WHERE "t"."a" = "stg"."a" AND "t"."b" = "stg"."b"')
