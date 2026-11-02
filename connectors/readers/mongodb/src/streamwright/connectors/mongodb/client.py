# Copyright 2025 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""
What the mongodb connector knows about MongoDB, shared by every engine adapter: the connection URI (credentials
percent-encoded into it: only ever handed to a driver, never logged), the passwords to mask, the rules a resolved
connection must meet, a find call as an aggregation pipeline, and BSON values as JSON. No engine library is imported.
"""

import base64
import datetime
from collections.abc import Mapping
from typing import Any
from urllib.parse import quote_plus, unquote, urlsplit

__all__ = ["database_name", "find_pipeline", "masked_values", "serialize", "uri", "validate_connection"]


def uri(connection: Mapping[str, Any]) -> str:
    """The connection's URI: its `uri`, or one built from host, port, database and credentials."""
    if connection.get("uri"):
        return str(connection["uri"]).strip()
    address = "%s:%s/%s" % (connection["host"], connection.get("port") or 27017, connection["database"])
    if connection.get("username") and connection.get("password"):
        return "mongodb://%s:%s@%s?authSource=%s" % (
            quote_plus(str(connection["username"])), quote_plus(str(connection["password"])), address,
            quote_plus(str(connection.get("auth_source") or "admin")))
    return "mongodb://" + address


def database_name(connection: Mapping[str, Any]) -> str | None:
    """The connection's database: `database`, or the path of its `uri`."""
    if connection.get("database"):
        return str(connection["database"])
    return urlsplit(str(connection.get("uri") or "")).path.lstrip("/") or None


def masked_values(connection: Mapping[str, Any]) -> list[str]:
    """The password inside a connection URI, as written and decoded: to mask on its own."""
    try:
        password = urlsplit(str(connection.get("uri") or "")).password
    except ValueError:
        return []
    return [password, unquote(password)] if password else []


def validate_connection(connection: Mapping[str, Any]) -> list[str]:
    """Either `uri` or `host` and `database`; a password goes with a username; a URI's scheme is mongodb."""
    problems = []
    if connection.get("uri"):
        if not str(connection["uri"]).strip().startswith(("mongodb://", "mongodb+srv://")):
            problems.append("`uri` must be a mongodb:// or mongodb+srv:// URI")
        given = [key for key in ("host", "username", "password") if connection.get(key)]
        if given:
            problems.append("use either `uri` or the connection keys, not both: %s" % ", ".join(given))
    elif not (connection.get("host") and connection.get("database")):
        problems.append("needs either `uri` or `host` and `database`")
    if bool(connection.get("username")) != bool(connection.get("password")) and not connection.get("uri"):
        problems.append("`username` and `password` go together")
    return problems


def find_pipeline(arguments: Mapping[str, Any]) -> list[dict[str, Any]]:
    """A find call (filter, projection, sort, limit) as an aggregation pipeline."""
    pipeline: list[dict[str, Any]] = []
    if arguments.get("filter"):
        pipeline.append({"$match": dict(arguments["filter"])})
    projection = arguments.get("projection")
    if projection:
        pipeline.append({"$project": dict.fromkeys(projection, 1) if isinstance(projection, list) else
                         dict(projection)})
    sort = {str(item[0]): item[1] for item in arguments.get("sort") or () if isinstance(item, (list, tuple))}
    sort.update((item, 1) for item in arguments.get("sort") or () if isinstance(item, str))
    if sort:
        pipeline.append({"$sort": sort})
    if arguments.get("limit"):
        pipeline.append({"$limit": int(arguments["limit"])})
    return pipeline


def serialize(value: Any) -> Any:
    """A BSON value as JSON: ObjectId as text, dates as ISO 8601, Decimal128 as a number, bytes as base64."""
    kind = type(value).__name__
    if kind == "ObjectId":
        return str(value)
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    if kind == "Decimal128":
        return float(str(value))
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, dict):
        return {key: serialize(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [serialize(item) for item in value]
    return value
