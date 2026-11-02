#!/usr/bin/env python
# /*************************************************************************
# * Copyright 2025 Karthick Jaganathan
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

"""
DuckLake catalogs of the warehouse outputs (streamwright.core.outputs.warehouse): extensions, settings, secrets
and ATTACH.

DuckLake (`--output ducklake:CATALOG[:SCHEMA]`) reads these environment variables (all optional):

- STREAMWRIGHT_DUCKLAKE_DATA_PATH: where DuckLake writes its data (Parquet) files, a local folder or `s3://bucket/prefix/`
  (DuckLake's default: next to the catalog).
- Object storage, set up (httpfs loaded, a temporary DuckDB `s3` secret) when the data path is `s3://...` or any
  STREAMWRIGHT_DUCKLAKE_S3_* variable is set; values are bound parameters, never in a query's text, and credentials are
  never logged or shown in a message:
  - STREAMWRIGHT_DUCKLAKE_S3_KEY_ID, STREAMWRIGHT_DUCKLAKE_S3_SECRET (both or neither),
    STREAMWRIGHT_DUCKLAKE_S3_SESSION_TOKEN: credentials;
  - STREAMWRIGHT_DUCKLAKE_S3_REGION: e.g. us-east-1;
  - STREAMWRIGHT_DUCKLAKE_S3_ENDPOINT: host[:port] of an S3-compatible store, no scheme (e.g. localhost:4566 for
    LocalStack);
  - STREAMWRIGHT_DUCKLAKE_S3_URL_STYLE: path or vhost;
  - STREAMWRIGHT_DUCKLAKE_S3_USE_SSL: true or false.
- STREAMWRIGHT_DUCKLAKE_DATA_INLINING_ROW_LIMIT: DuckLake's data inlining (inserts of fewer rows are kept in the catalog
  instead of a data file); 0 turns it off. Defaults to 0 when the data path is on object storage (every load writes
  data files to the bucket), to DuckLake's default otherwise.
- A Postgres catalog: CATALOG `postgres:DSN`, a libpq connection string such as
  `postgres:dbname=lake host=localhost user=streamwright` (DuckDB's postgres extension is loaded; Postgres catalogs take
  concurrent writers). The DSN cannot hold a password: STREAMWRIGHT_DUCKLAKE_CATALOG_PASSWORD gives it, as a temporary
  DuckDB postgres secret (a bound parameter, never logged).
- STREAMWRIGHT_DUCKLAKE_CATALOG_SCHEMA: the schema of the catalog's own tables (DuckLake's METADATA_SCHEMA), e.g. to
  keep a Postgres catalog in a schema of its own.
"""

import os
import re
from types import MappingProxyType

from streamwright.core.outputs.staging import checked_name
from streamwright.core.outputs.staging import quote as _quote
from streamwright.core.outputs.staging import sql_string as _sql_string

POSTGRES_PREFIX = "postgres:"
DATA_PATH_ENV = "STREAMWRIGHT_DUCKLAKE_DATA_PATH"
INLINING_ENV = "STREAMWRIGHT_DUCKLAKE_DATA_INLINING_ROW_LIMIT"
CATALOG_PASSWORD_ENV = "STREAMWRIGHT_DUCKLAKE_CATALOG_PASSWORD"
CATALOG_SCHEMA_ENV = "STREAMWRIGHT_DUCKLAKE_CATALOG_SCHEMA"
S3_SECRET_NAME = "streamwright_ducklake_s3"
# environment variable -> the DuckDB s3 secret's parameter
S3_CREDENTIALS = MappingProxyType({"STREAMWRIGHT_DUCKLAKE_S3_KEY_ID": "KEY_ID",
                                   "STREAMWRIGHT_DUCKLAKE_S3_SECRET": "SECRET",
                                   "STREAMWRIGHT_DUCKLAKE_S3_SESSION_TOKEN": "SESSION_TOKEN"})
S3_SETTINGS = MappingProxyType({"STREAMWRIGHT_DUCKLAKE_S3_REGION": "REGION",
                                "STREAMWRIGHT_DUCKLAKE_S3_ENDPOINT": "ENDPOINT",
                                "STREAMWRIGHT_DUCKLAKE_S3_URL_STYLE": "URL_STYLE",
                                "STREAMWRIGHT_DUCKLAKE_S3_USE_SSL": "USE_SSL"})
_URL_STYLES = ("path", "vhost")
_ENDPOINT = re.compile(r"^[A-Za-z0-9._\-]+(:[0-9]+)?$")
_REGION = re.compile(r"^[A-Za-z0-9_\-]+$")
_BUCKET = re.compile(r"^s3://([A-Za-z0-9][A-Za-z0-9._\-]*)(/|$)")
_DSN_PASSWORD = re.compile(r"(^|[\s?&])(password|sslpassword)\s*=", re.IGNORECASE)
_URI_PASSWORD = re.compile(r"://[^/?#@:\s\"'<>]*:[^/?#@\s\"'<>]+@")


def load_extension(connection, name):
    """Loads a DuckDB extension, installing it first (once, from DuckDB's repository) when it is missing."""
    installed = connection.execute("SELECT bool_or(installed) FROM duckdb_extensions() "
                                   "WHERE extension_name = ? OR list_contains(aliases, ?)", [name, name]).fetchone()
    if not (installed and installed[0]):
        connection.execute("INSTALL %s" % name)
    connection.execute("LOAD %s" % name)


def load_ducklake(connection):
    """Loads DuckDB's ducklake extension, installing it first (once, from DuckDB's repository) when it is missing."""
    load_extension(connection, "ducklake")


def is_postgres(catalog):
    return catalog.startswith(POSTGRES_PREFIX)


def _flag(name, value):
    text = value.strip().lower()
    if text not in ("true", "false"):
        raise ValueError("%s: expected true or false" % name)
    return text == "true"


def _setting(environ, name):
    """A DuckLake environment variable's value, None when unset or blank."""
    text = environ.get(name)
    return text if text is not None and text.strip() != "" else None


def _s3_parameter(name, parameter, text):
    """The checked value of one s3 secret parameter given by environment variable `name`."""
    if name in S3_SETTINGS:
        text = text.strip()
    if parameter == "USE_SSL":
        return _flag(name, text)
    if parameter == "URL_STYLE" and text not in _URL_STYLES:
        raise ValueError("%s: expected one of: %s" % (name, ", ".join(_URL_STYLES)))
    if parameter == "ENDPOINT" and not _ENDPOINT.match(text):
        raise ValueError("%s: expected a host and an optional port, e.g. localhost:4566 or "
                         "storage.example.com (no scheme: %s picks https or http)" % (
                             name, "STREAMWRIGHT_DUCKLAKE_S3_USE_SSL"))
    if parameter == "REGION" and not _REGION.match(text):
        raise ValueError("%s: expected a region, e.g. us-east-1" % name)
    return text


def _s3_secret(environ, on_s3):
    """The s3 secret's parameters (None when the data is not on S3 and no s3 variable is set)."""
    given = [(name, parameter, _setting(environ, name))
             for name, parameter in list(S3_CREDENTIALS.items()) + list(S3_SETTINGS.items())]
    if not on_s3 and all(text is None for _, _, text in given):
        return None
    s3 = {parameter: _s3_parameter(name, parameter, text) for name, parameter, text in given
              if text is not None}
    if ("KEY_ID" in s3) != ("SECRET" in s3):
        raise ValueError("STREAMWRIGHT_DUCKLAKE_S3_KEY_ID and STREAMWRIGHT_DUCKLAKE_S3_SECRET go together: set both or "
                         "neither")
    if "SESSION_TOKEN" in s3 and "KEY_ID" not in s3:
        raise ValueError("STREAMWRIGHT_DUCKLAKE_S3_SESSION_TOKEN needs STREAMWRIGHT_DUCKLAKE_S3_KEY_ID and "
                         "STREAMWRIGHT_DUCKLAKE_S3_SECRET")
    return s3


def _inlining(environ, on_s3):
    inlining = _setting(environ, INLINING_ENV)
    if inlining is None:
        return 0 if on_s3 else None
    if not inlining.strip().isdigit():
        raise ValueError("%s: expected a whole number >= 0 (0 turns data inlining off)" % INLINING_ENV)
    return int(inlining.strip())


def _check_postgres_catalog(catalog):
    dsn = catalog[len(POSTGRES_PREFIX):].strip()
    if not dsn:
        raise ValueError("a postgres DuckLake catalog needs a connection string, e.g. "
                         "ducklake:postgres:dbname=lake host=localhost user=streamwright")
    if _DSN_PASSWORD.search(dsn) or _URI_PASSWORD.search(catalog):
        raise ValueError("the postgres DuckLake catalog's connection string holds a password: give it in %s "
                         "instead" % CATALOG_PASSWORD_ENV)


def ducklake_settings(environ, catalog):
    """
    The DuckLake connection settings of `environ` (see the module's doc) for `catalog`, checked: a dict of data_path,
    s3 (None, or the s3 secret's parameter -> value), s3_scope, inlining (None or a row limit), catalog_password and
    catalog_schema. ValueError (naming the variable, never a credential's value) when a setting is wrong.
    """
    data_path = _setting(environ, DATA_PATH_ENV)
    data_path = os.path.expanduser(data_path) if data_path is not None else None
    on_s3 = data_path is not None and data_path.lower().startswith("s3://")
    s3 = _s3_secret(environ, on_s3)
    scope = None
    if on_s3:
        bucket = _BUCKET.match(data_path)
        if not bucket:
            raise ValueError("%s: expected s3://BUCKET/PREFIX/" % DATA_PATH_ENV)
        scope = "s3://%s/" % bucket.group(1)
    inlining = _inlining(environ, on_s3)
    postgres = is_postgres(catalog)
    if postgres:
        _check_postgres_catalog(catalog)
    password = environ.get(CATALOG_PASSWORD_ENV) or None
    if password is not None and not postgres:
        raise ValueError("%s is for a postgres catalog (ducklake:postgres:DSN)" % CATALOG_PASSWORD_ENV)
    schema = _setting(environ, CATALOG_SCHEMA_ENV)
    if schema is not None:
        schema = schema.strip()
        checked_name(schema, CATALOG_SCHEMA_ENV)
    return {"data_path": data_path, "s3": s3, "s3_scope": scope, "inlining": inlining, "catalog_password": password,
            "catalog_schema": schema}


def s3_secret_query(s3, scope=None):
    """(query, parameters) that create the temporary DuckDB s3 secret; every value is a bound parameter."""
    parts, parameters = ["TYPE s3"], []
    for parameter in list(S3_CREDENTIALS.values()) + list(S3_SETTINGS.values()):
        if parameter in s3:
            parts.append("%s ?" % parameter)
            parameters.append(s3[parameter])
    if scope:
        parts.append("SCOPE ?")
        parameters.append(scope)
    return "CREATE OR REPLACE TEMPORARY SECRET %s (%s)" % (S3_SECRET_NAME, ", ".join(parts)), parameters


def postgres_secret_query(password):
    """
    (query, parameters) that create DuckDB's default postgres secret, which the postgres extension adds to every
    connection string (the DuckLake catalog's too): the password stays out of the ATTACH's text.
    """
    return "CREATE TEMPORARY SECRET (TYPE postgres, PASSWORD ?)", [password]


def attach_query(catalog, settings, read_only=False):
    """The ATTACH of the DuckLake catalog (its options are not secrets: they are in the query's text)."""
    options = []
    if settings["data_path"]:
        options.append("DATA_PATH %s" % _sql_string(settings["data_path"]))
    if settings["catalog_schema"]:
        options.append("METADATA_SCHEMA %s" % _sql_string(settings["catalog_schema"]))
    if settings["inlining"] is not None:
        options.append("DATA_INLINING_ROW_LIMIT %d" % settings["inlining"])
    if read_only:
        options.append("READ_ONLY")
    attach_options = " (%s)" % ", ".join(options) if options else ""
    return "ATTACH %s AS %s%s" % (_sql_string("ducklake:" + catalog), _quote("streamwright_ducklake"), attach_options)


def mask_values(text, values):
    for value in sorted((v for v in values if v), key=len, reverse=True):
        text = text.replace(value, "***")
    return text
