import duckdb
import pytest

from streamwright.core.sql.quote import quote_ident, quote_literal, quote_qualified


def test_quote_literal_doubles_single_quotes_and_round_trips():
    value = "it's; DROP TABLE x; --"
    assert quote_literal(value) == "'it''s; DROP TABLE x; --'"
    assert duckdb.connect().execute("SELECT %s" % quote_literal(value)).fetchone()[0] == value


def test_quote_ident_doubles_double_quotes_and_round_trips():
    name = 'my "odd" col'
    assert quote_ident(name) == '"my ""odd"" col"'
    con = duckdb.connect()
    assert [d[0] for d in con.execute("SELECT 1 AS %s" % quote_ident(name)).description] == [name]


def test_quote_qualified_quotes_each_part():
    assert quote_qualified("main.t") == '"main"."t"'


@pytest.mark.parametrize("bad", ["a\x00b", None, True])
def test_rejects_nul_and_non_values(bad):
    with pytest.raises(ValueError):
        quote_literal(bad)
    with pytest.raises(ValueError):
        quote_ident(bad)


def test_rejects_empty_identifier():
    with pytest.raises(ValueError):
        quote_ident("")
