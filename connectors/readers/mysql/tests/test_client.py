"""The MySQL facts every engine shares: secret parameters, DSNs, policy and Spark's statements."""

import pytest

from streamwright.connectors.mysql import client
from streamwright.core.io.queries import check_query
from streamwright.core.io.sandbox import ConnectError


def test_parameters_become_the_secret_in_its_order_and_unknown_ones_are_refused_by_name():
    parameters = client.parameters({"dsn": "mysql://u:p%40w@db:3307/shop?ssl_mode=required&ssl_ca=/ca.pem"})
    assert client.secret_options(parameters) == [("HOST", "db"), ("PORT", 3307), ("DATABASE", "shop"),
                                                 ("USER", "u"), ("PASSWORD", "p@w"), ("SSL_MODE", "required"),
                                                 ("SSL_CA", "/ca.pem")]
    with pytest.raises(ConnectError, match=r"does not take: compress$"):
        client.secret_options([("compress", "secret-value")])


@pytest.mark.parametrize("schema", ["mysql", "performance_schema", "sys", "information_schema"])
def test_mysql_system_schemas_are_no_tables(schema):
    assert "system catalog or schema" in check_query("SELECT * FROM %s.users" % schema, {},
                                                     client.DIALECT.policy)[1][0]


def test_literals_double_backslashes_and_spark_statements():
    assert client.literal("a\\b'c") == "'a\\\\b''c'"
    assert client.staging_statement("`t`", "`stg`") == "CREATE TABLE `stg` LIKE `t`"
    assert client.merge_statement("`t`", "`stg`", ["`id`", "`v`"], ["`id`"], ["`v`"]) == (
        "INSERT INTO `t` (`id`, `v`) SELECT `id`, `v` FROM `stg` ON DUPLICATE KEY UPDATE `v` = VALUES(`v`)")
    assert client.merge_statement("`t`", "`stg`", ["`id`"], ["`id`"], []).endswith("UPDATE `id` = VALUES(`id`)")
    assert client.delete_statement("`t`", "`stg`", ["`id`"]) == (
        "DELETE t FROM `t` t INNER JOIN `stg` s ON t.`id` = s.`id`")
    assert client.jdbc_values({"host": "db", "database": "d", "user": "u", "sslmode": "verify_ca"})["properties"] == {
        "sslMode": "VERIFY_CA"}
