"""The MongoDB facts every engine shares: URIs, masked passwords, find pipelines, BSON as JSON."""

import datetime

import bson

from streamwright.connectors.mongodb import client


def test_the_uri_of_the_keys_encodes_the_credentials():
    connection = {"host": "db", "port": 27018, "database": "prod", "username": "a@b", "password": "p:w/d"}
    assert client.uri(connection) == "mongodb://a%40b:p%3Aw%2Fd@db:27018/prod?authSource=admin"
    assert client.uri({"host": "db", "database": "prod"}) == "mongodb://db:27017/prod"
    assert client.uri({"uri": " mongodb://h/d "}) == "mongodb://h/d"
    assert client.database_name({"uri": "mongodb://h:1/shop?x=1"}) == "shop"


def test_the_password_inside_a_uri_is_masked_as_written_and_decoded():
    assert client.masked_values({"uri": "mongodb://u:p%40ss@h/d"}) == ["p%40ss", "p@ss"]
    assert client.masked_values({"host": "h"}) == []


def test_a_find_becomes_a_pipeline():
    assert client.find_pipeline({"filter": {"a": 1}, "projection": ["x"], "sort": [["x", -1], "y"], "limit": 5}) == [
        {"$match": {"a": 1}}, {"$project": {"x": 1}}, {"$sort": {"x": -1, "y": 1}}, {"$limit": 5}]


def test_bson_values_become_json():
    document = {"_id": bson.ObjectId("65f000000000000000000001"), "at": datetime.datetime(2026, 1, 2, 3, 4),
                "n": bson.Decimal128("1.5"), "b": b"\x00\x01", "list": [{"d": datetime.date(2026, 1, 2)}]}
    assert client.serialize(document) == {"_id": "65f000000000000000000001", "at": "2026-01-02T03:04:00", "n": 1.5,
                                          "b": "AAE=", "list": [{"d": "2026-01-02"}]}
