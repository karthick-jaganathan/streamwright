"""The files source adapter (streamwright run): formats, globs, pages, roots, match, and the locked reader."""

import datetime
import decimal
import glob
import gzip
import json
import logging
import os
from types import MappingProxyType

import duckdb
import pytest
import yaml

from streamwright.connectors.files import CONNECTOR
from streamwright.core.io.file_formats import AccessError, ReadError, format_of
from streamwright.core.io.local import LocalLocation
from streamwright.core.io.store_reader import open_local_reader
from streamwright.core.sdk import ConnectorContext, ConnectorError, Redactor
from streamwright.core.sdk.contract import resolve_values
from streamwright.core.sdk.testing import SourceAdapterContract, run_cli

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", ".."))
EXAMPLE = os.path.join(REPO_ROOT, "examples", "sources", "readers", "files_demo")
DATA = os.path.join(EXAMPLE, "data")
BIG = "12345678901234567890.123456"
DAILY = r"orders_\d{4}-\d{2}-\d{2}\.csv"
READ = CONNECTOR.operation("object", "read")


def write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return str(path)


def open_session(*roots, page_size=None):
    connection = resolve_values(CONNECTOR, {"roots": [str(root) for root in roots]})
    session = CONNECTOR.load_adapter("source").open(connection, ConnectorContext("files", Redactor()))
    if page_size:
        session.reader = type(session.reader)(session.reader.sandbox, session.reader.location, page_size)
    return session


def read(root, arguments, *, roots=None):
    session = open_session(*(roots or [root]))
    try:
        return [record for page in session.read(READ, arguments, ConnectorContext("files", Redactor()))
                for record in page]
    finally:
        session.close()


def ids(records):
    return [int(record["id"]) for record in records]


class TestSourceAdapter(SourceAdapterContract):
    __test__ = True
    definition = CONNECTOR
    connection = MappingProxyType({"roots": [DATA]})
    read_call = (("object", "read"), MappingProxyType({"path": "orders/east.csv"}))
    expected_records = (
        {"order_id": "1001", "product_id": "1", "quantity": "2", "amount": "39.98", "ordered_on": "2026-10-01"},
        {"order_id": "1002", "product_id": "3", "quantity": "1", "amount": "149.00", "ordered_on": "2026-10-01"},
        {"order_id": "1003", "product_id": "2", "quantity": "5", "amount": "12.50", "ordered_on": "2026-10-02"})


def test_csv_values_are_the_files_text_and_empty_cells_null(tmp_path):
    write(tmp_path / "a.csv", "id,amount,day,name\n1,%s,2026-10-01,a\n2,,2026-10-02,\"b,c\"\n" % BIG)
    assert read(tmp_path, {"path": "a.csv"}) == [
        {"id": "1", "amount": BIG, "day": "2026-10-01", "name": "a"},
        {"id": "2", "amount": None, "day": "2026-10-02", "name": "b,c"}]
    typed = read(tmp_path, {"path": "a.csv", "options": {"columns": {
        "id": "BIGINT", "amount": "DECIMAL(38,6)", "day": "DATE", "name": "VARCHAR"}}})
    assert typed[0] == {"id": 1, "amount": decimal.Decimal(BIG), "day": "2026-10-01", "name": "a"}


def test_tsv_and_csv_options(tmp_path):
    write(tmp_path / "a.tsv", "id\tname\n1\tx\n")
    assert read(tmp_path, {"path": "a.tsv"}) == [{"id": "1", "name": "x"}]
    write(tmp_path / "b.txt", "# a comment line\nid;name\n1;NA\n")
    assert read(tmp_path, {"path": "b.txt", "format": "csv", "options": {
        "delimiter": ";", "skip": 1, "nullstr": "NA", "header": True}}) == [{"id": "1", "name": None}]
    write(tmp_path / "c.csv", "1,x\n")
    assert read(tmp_path, {"path": "c.csv", "options": {"header": False}}) == [{"column0": "1", "column1": "x"}]


def test_json_array_document_and_jsonl_records_are_kept_as_written(tmp_path):
    write(tmp_path / "a.json", '[{"id": 1, "v": %s}, {"id": 2, "v": 0.1, "n": {"x": [1, 2]}}]' % BIG)
    assert read(tmp_path, {"path": "a.json"}) == [
        {"id": 1, "v": decimal.Decimal(BIG)}, {"id": 2, "v": decimal.Decimal("0.1"), "n": {"x": [1, 2]}}]
    write(tmp_path / "d.json", '{"data": [{"id": 1}], "next": null}')
    assert read(tmp_path, {"path": "d.json"}) == [{"data": [{"id": 1}], "next": None}]
    write(tmp_path / "l.jsonl", '{"id": 1, "v": 1.50}\n\n{"id": 2}\n')
    assert read(tmp_path, {"path": "l.jsonl"}) == [{"id": 1, "v": decimal.Decimal("1.50")}, {"id": 2}]
    assert read(tmp_path, {"path": "l.jsonl", "options": {"filename": True}})[1] == {"id": 2, "filename": "l.jsonl"}


def test_compressed_files_take_their_format_from_the_name_before_the_compression(tmp_path):
    with gzip.open(str(tmp_path / "a.jsonl.gz"), "wt") as stream:
        stream.write('{"id": 1}\n')
    assert format_of(str(tmp_path / "a.jsonl.gz")) == "jsonl"
    assert read(tmp_path, {"path": "a.jsonl.gz"}) == [{"id": 1}]
    assert read(tmp_path, {"path": "a.jsonl.gz", "options": {"compression": "gzip"}}) == [{"id": 1}]


def test_parquet_columns_become_exact_json_values(tmp_path):
    connection = duckdb.connect()
    try:
        connection.execute("COPY (SELECT 1::BIGINT AS id, %s::DECIMAL(38,6) AS amount, DATE '2026-10-01' AS day, "
                           "[1, 2] AS l) TO '%s' (FORMAT parquet)" % (BIG, tmp_path / "p.parquet"))
    finally:
        connection.close()
    assert read(tmp_path, {"path": "p.parquet"}) == [
        {"id": 1, "amount": decimal.Decimal(BIG), "day": "2026-10-01", "l": [1, 2]}]


def test_bad_formats_and_unreadable_files_are_connector_errors(tmp_path):
    write(tmp_path / "a.xml", "<a/>")
    with pytest.raises(ConnectorError, match=r"cannot tell the format of .*a\.xml from its extension: set `format`"):
        read(tmp_path, {"path": "a.xml"})
    write(tmp_path / "a.csv", "id\n1\n")
    with pytest.raises(ConnectorError, match=r"cannot read .*a\.csv as parquet") as caught:
        read(tmp_path, {"path": "a.csv", "format": "parquet"})
    assert caught.value.code == "READ_ERROR"


def test_globs_read_every_matching_file_in_name_order(tmp_path):
    write(tmp_path / "data" / "b.csv", "id\n2\n")
    write(tmp_path / "data" / "a.csv", "id\n1\n")
    write(tmp_path / "data" / "sub" / "c.csv", "id\n3\n")
    write(tmp_path / "data" / "skip.jsonl", '{"id": 9}\n')
    assert read(tmp_path, {"path": "data/*.csv"}) == [{"id": "1"}, {"id": "2"}]
    assert read(tmp_path, {"path": "data/**/*.csv"}) == [{"id": "1"}, {"id": "2"}, {"id": "3"}]
    assert read(tmp_path, {"path": "data/*", "format": "csv", "options": {"filename": True}})[:2] == [
        {"id": "1", "filename": os.path.join("data", "a.csv")}, {"id": "2", "filename": os.path.join("data", "b.csv")}]
    # a list, absolute paths inside a root, each file read once
    assert read(tmp_path, {"path": [str(tmp_path / "data" / "b.csv"), "data/*.csv"]}) == [{"id": "2"}, {"id": "1"}]


def test_files_are_read_in_pages_of_at_most_page_size_rows(tmp_path):
    write(tmp_path / "a.csv", "id\n" + "".join("%d\n" % number for number in range(5)))
    session = open_session(tmp_path, page_size=2)
    try:
        pages = session.read(READ, {"path": "a.csv"}, ConnectorContext("files", Redactor()))
        assert [len(page) for page in pages] == [2, 2, 1]
    finally:
        session.close()


# * ------------------------
# * roots: what can be read
# * ------------------------

@pytest.fixture
def guarded(tmp_path, monkeypatch):
    """(root, outside secret file, reads): a root with links leading outside it; `reads` records every file read."""
    root, outside = tmp_path / "root", tmp_path / "outside"
    secret = write(outside / "secret.csv", "id\n1\n")
    write(root / "data" / "east.csv", "id,amount\n1,1\n")
    os.symlink(secret, str(root / "link.csv"))
    os.symlink(str(outside), str(root / "linked"))
    reads = []
    pages = type(open_local_reader((str(root),), provider="files")).pages

    def spy(self, path, *args, **kwargs):
        reads.append(path)
        return pages(self, path, *args, **kwargs)
    monkeypatch.setattr(type(open_local_reader((str(root),), provider="files")), "pages", spy)
    return root, secret, reads


@pytest.mark.parametrize(("path", "message"), [
    ("../outside/secret.csv", "goes up a folder"),
    ("data/../../outside/secret.csv", "goes up a folder"),
    ("SECRET", "is outside the roots"),
    ("link.csv", "is outside the roots"),
    ("*.csv", "leads outside the roots files can be read from .* through a symbolic link"),
    ("linked/*.csv", "is outside the roots"),
    ("s3://bucket/a.csv", "is a URL: local files only"),
    (["data/east.csv", "../outside/secret.csv"], "goes up a folder"),
])
def test_paths_outside_the_roots_are_refused_before_any_read(guarded, path, message):
    root, secret, reads = guarded
    path = [item.replace("SECRET", secret) for item in path] if isinstance(path, list) else path.replace(
        "SECRET", secret)
    with pytest.raises(ConnectorError, match=message) as caught:
        read(root, {"path": path})
    assert reads == [] and caught.value.code == "ACCESS_DENIED"


def test_the_reader_reads_only_inside_its_roots_and_stays_locked(guarded):
    root, _, _ = guarded
    reader = open_local_reader((str(root),), provider="files")
    try:
        east = os.path.realpath(str(root / "data" / "east.csv"))
        assert list(reader.pages(east, "csv")) == [[{"id": "1", "amount": "1"}]]
        with pytest.raises(ReadError, match="Permission Error"):  # DuckDB refuses a link leaving the roots too
            list(reader.pages(str(root / "link.csv"), "csv"))
        with pytest.raises(duckdb.Error):
            reader.sandbox.connection.execute("SET enable_external_access = true")
        folder = reader.sandbox.folder
    finally:
        reader.close()
    assert reader.closed and not os.path.exists(folder)
    with pytest.raises(ReadError, match="closed"):
        list(reader.pages(east, "csv"))


def test_the_reader_never_enables_external_access_nor_loads_an_extension(monkeypatch, tmp_path):
    statements = []

    class Recorder:
        def __init__(self, connection):
            self.connection = connection

        def execute(self, query, *args, **kwargs):
            statements.append(query)
            return self.connection.execute(query, *args, **kwargs)

        def __getattr__(self, name):
            return getattr(self.connection, name)

    connect = duckdb.connect
    monkeypatch.setattr(duckdb, "connect", lambda *args, **kwargs: Recorder(connect(*args, **kwargs)))
    write(tmp_path / "a.csv", "id\n1\n")
    assert read(tmp_path, {"path": "a.csv"}) == [{"id": "1"}]
    assert statements[:4] == ["SET TimeZone = 'UTC'",
                              "SET allowed_directories = ['%s']" % (os.path.realpath(str(tmp_path)) + os.sep),
                              "SET enable_external_access = false", "SET lock_configuration = true"]
    text = "\n".join(statements).lower()
    for word in ("httpfs", "install", "load ", "secret", "enable_external_access = true", "s3://", "http://"):
        assert word not in text, word


def test_roots_are_existing_folders_inside_which_relative_paths_start_at_the_first(tmp_path):
    link = tmp_path / "link"
    os.symlink(str(tmp_path), str(link))
    assert LocalLocation((str(link), str(tmp_path))).roots == (os.path.realpath(str(tmp_path)),)
    with pytest.raises(ConnectorError, match="no such folder"):
        open_session(tmp_path / "missing")
    with pytest.raises(AccessError, match="no folder"):
        LocalLocation(())
    with pytest.raises(AccessError, match="roots are local folders"):
        LocalLocation(("s3://bucket/",))
    write(tmp_path / "one" / "a.csv", "id\n1\n")
    write(tmp_path / "two" / "a.csv", "id\n2\n")
    roots = [tmp_path / "one", tmp_path / "two"]
    assert read(tmp_path, {"path": "a.csv"}, roots=roots) == [{"id": "1"}]
    assert read(tmp_path, {"path": str(tmp_path / "two" / "a.csv")}, roots=roots) == [{"id": "2"}]
    with pytest.raises(ConnectorError, match="outside the roots"):
        read(tmp_path, {"path": str(tmp_path / "two" / "a.csv")}, roots=roots[:1])
    (tmp_path / "data").mkdir()
    with pytest.raises(ConnectorError, match="is a folder: name its files"):
        read(tmp_path, {"path": "data"})


def test_a_missing_file_is_skipped_with_a_warning_or_fails_with_on_missing_error(tmp_path, caplog):
    caplog.set_level(logging.WARNING)
    assert read(tmp_path, {"path": "data/*.parquet"}) == []
    assert read(tmp_path, {"path": "north.csv"}) == []
    assert "files: no file matches 'north.csv'; skipping it (on_missing: skip)" in caplog.text
    with pytest.raises(ConnectorError, match=r"no file matches 'north\.csv'") as caught:
        read(tmp_path, {"path": "north.csv", "on_missing": "error"})
    assert caught.value.code == "NOT_FOUND"


# * ---------------------
# * match and recursive
# * ---------------------

@pytest.fixture
def tree(tmp_path):
    """A root with daily order files in data/, its sub-folders, and files the daily regex must not select."""
    root = tmp_path / "root"
    for name, number in (("orders_2026-10-02.csv", 2), ("orders_2026-10-01.csv", 1), ("orders_latest.csv", 9),
                         ("orders_2026-10-02.csv.bak", 8), ("2026/orders_2026-09-30.csv", 0),
                         ("2026/deep/orders_2026-09-29.csv", -1), ("2026/notes.txt", 7)):
        write(root / "data" / name, "id\n%d\n" % number)
    return root


def test_match_is_a_fullmatch_on_the_path_below_the_folder_and_recursive_goes_deeper(tree):
    assert ids(read(tree, {"path": "data", "match": DAILY})) == [1, 2]
    assert ids(read(tree, {"path": "data", "match": r"orders_2026"})) == []
    assert ids(read(tree, {"path": "data", "match": r"orders_\d{4}-\d{2}-\d{2}\.csv", "recursive": False})) == [1, 2]
    assert ids(read(tree, {"path": "data", "match": r"(.*/)?" + DAILY, "recursive": True})) == [-1, 0, 1, 2]
    assert ids(read(tree, {"path": "data", "match": r"2026/[^/]+\.csv", "recursive": True})) == [0]
    assert ids(read(tree, {"path": "data/orders_*.csv"})) == [1, 2, 9]
    assert ids(read(tree, {"path": "data/**/orders_2026-??-??.csv"})) == [-1, 0, 1, 2]


def test_a_matched_symbolic_link_leaving_the_root_is_refused_before_any_read(tree, tmp_path):
    secret = write(tmp_path / "outside" / "secret.csv", "id\n666\n")
    os.symlink(secret, str(tree / "data" / "orders_2026-10-03.csv"))
    with pytest.raises(ConnectorError, match=r"leads outside the roots files can be read from .* through a symbolic "
                                             r"link") as caught:
        read(tree, {"path": "data", "match": DAILY})
    assert caught.value.code == "ACCESS_DENIED"
    assert ids(read(tree, {"path": "data", "match": r"orders_2026-10-0[12]\.csv"})) == [1, 2]  # not selected: fine
    write(tmp_path / "outside" / "orders_2026-01-01.csv", "id\n666\n")
    os.symlink(str(tmp_path / "outside"), str(tree / "data" / "linked"))  # links to folders are not followed
    os.remove(str(tree / "data" / "orders_2026-10-03.csv"))
    assert ids(read(tree, {"path": "data", "match": r"(.*/)?" + DAILY, "recursive": True})) == [-1, 0, 1, 2]


@pytest.mark.parametrize(("path", "message"), [
    ("../outside", "goes up a folder"), ("data/../..", "goes up a folder"), ("OUTSIDE", "is outside the roots"),
    ("linked", "is outside the roots")])
def test_a_folder_outside_the_roots_is_refused(tree, tmp_path, path, message):
    write(tmp_path / "outside" / "orders_2026-01-01.csv", "id\n666\n")
    os.symlink(str(tmp_path / "outside"), str(tree / "linked"))
    with pytest.raises(ConnectorError, match=message) as caught:
        read(tree, {"path": path.replace("OUTSIDE", str(tmp_path / "outside")), "match": r".*\.csv"})
    assert caught.value.code == "ACCESS_DENIED"


@pytest.mark.parametrize(("name", "sibling", "regex", "pattern"), [
    ("orders_*.csv", "orders_payroll.csv", r"orders_\*\.csv", "orders_*.csv"),
    ("a[1].csv", "a1.csv", r"a\[1\]\.csv", "a*.csv"),
    ("b?.csv", "bx.csv", r"b\?\.csv", "b*.csv"),
])
def test_a_file_named_with_a_glob_character_is_refused_before_any_read(tmp_path, name, sibling, regex, pattern):
    # DuckDB reads each path it is given as a glob: the file would be read as its siblings
    root = tmp_path / "root"
    write(root / name, "id\n666\n")
    write(root / sibling, "id\n1\n")
    for arguments in ({"path": ".", "match": regex}, {"path": pattern}, {"path": glob.escape(name)}):
        with pytest.raises(ConnectorError, match=r"has a glob character \(\*, \? or \[\) in its path: it cannot be "
                                                 r"read by name") as caught:
            read(root, arguments)
        assert caught.value.code == "ACCESS_DENIED"
    assert ids(read(root, {"path": sibling})) == [1]


def test_with_match_path_must_be_a_folder_that_exists_or_follows_on_missing(tree, caplog):
    caplog.set_level(logging.WARNING)
    with pytest.raises(ConnectorError, match="is not a folder: with `match`, `path` is the folder") as caught:
        read(tree, {"path": "data/orders_latest.csv", "match": DAILY})
    assert caught.value.code == "READ_ERROR"
    with pytest.raises(ConnectorError, match="is a glob: with `match`, `path` is the folder to look in"):
        read(tree, {"path": "data/*", "match": DAILY})
    assert read(tree, {"path": "data", "match": r"orders_1999-.*\.csv"}) == []
    assert "no file in 'data' matches" in caplog.text
    assert read(tree, {"path": "nope", "match": DAILY}) == []
    with pytest.raises(ConnectorError, match="no file in 'nope' matches") as caught:
        read(tree, {"path": "nope", "match": DAILY, "on_missing": "error"})
    assert caught.value.code == "NOT_FOUND"
    location = LocalLocation((str(tree),))
    with pytest.raises(ReadError, match="is not a valid regex"):
        location.match_files("data", "(")
    with pytest.raises(AccessError, match="goes up a folder"):
        location.match_files("..", DAILY)


# * ---------------------------------
# * the example and the CLI
# * ---------------------------------

def test_the_files_demo_runs_offline_through_the_installed_definition(tmp_path):
    out, summary = tmp_path / "out", tmp_path / "summary.json"
    assert run_cli(["run", EXAMPLE, "--set", "data_root=%s" % DATA, "--allow-connector", "files",
                    "--output", "jsonl:%s" % out, "--summary", str(summary)]) == 0
    # the orders export names its file in the YAML (`file_name`); the others keep the default names
    orders_file = out / "files_demo" / "orders" / ("%s.jsonl" % datetime.date.today().isoformat())
    written = {}
    others = [out / name for name in os.listdir(str(out)) if name not in ("state.json", "files_demo")]
    for path in [orders_file, *others]:
        with open(str(path)) as stream:
            export = "orders" if path == orders_file else path.name.split(".")[0]
            written[export] = [json.loads(line) for line in stream]
    assert sorted(written) == ["customers", "orders", "products"]
    assert [(record["order_id"], record["region"]) for record in written["orders"]] == [
        (1001, "east"), (1002, "east"), (1003, "east"), (2001, "west"), (2002, "west")]
    with open(str(summary)) as stream:
        streams = {item["name"]: item for item in json.load(stream)["streams"]}
    assert streams["orders"]["records_read"] == 5 and streams["products"]["records_read"] == 3


def test_the_demo_needs_the_connector_allowed_and_an_existing_root(tmp_path, caplog):
    out = "jsonl:%s" % (tmp_path / "out")
    assert run_cli(["run", EXAMPLE, "--set", "data_root=%s" % DATA, "--allow-connector", "google_ads",
                    "--output", out]) == 2
    assert "connector 'files' is not in the allowed list" in caplog.text
    assert run_cli(["run", EXAMPLE, "--set", "data_root=%s" % os.path.join(DATA, "missing"), "--allow-connector",
                    "files", "--stream", "products", "--output", out]) != 0
    assert "roots: %s: no such folder" % os.path.join(DATA, "missing") in caplog.text


def test_validate_refuses_urls_secrets_and_unknown_formats(capsys, tmp_path):
    assert run_cli(["validate", EXAMPLE]) == 0
    with open(os.path.join(EXAMPLE, "source.yaml")) as stream:
        document = yaml.safe_load(stream)
    with open(os.path.join(EXAMPLE, "streams", "products.yaml")) as stream:
        products = yaml.safe_load(stream)
    products["requests"][0]["arguments"]["format"] = "xlsx"
    document["streams"] = [dict(products, name="products")]
    document["auth"]["roots"] = ["s3://bucket/prefix/"]
    bad = tmp_path / "source.yaml"
    bad.write_text(yaml.safe_dump(document, sort_keys=False))
    assert run_cli(["validate", str(bad)]) == 1
    output = capsys.readouterr().out
    assert "auth.roots" in output and "arguments.format" in output
