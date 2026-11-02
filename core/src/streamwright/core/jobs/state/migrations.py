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

"""
Ordered, idempotent migrations of the bookkeeping tables (§7c naming: plural snake_case tables, full-word columns,
named constraints, `*_at` timestamps, engine-specific data in `engine_metadata`).

Each migration is (version, build) where build(dialect, table, label) returns its statements: `table(name)` is the
qualified physical table name, `label(name)` the physical name used in constraint and index names.
"""

import uuid

__all__ = ["LATEST_VERSION", "MIGRATIONS"]


def _initial_schema(dialect, table, label):
    ts, js = dialect.timestamp_type, dialect.json_type
    return [
        "CREATE TABLE IF NOT EXISTS %s (job_name TEXT NOT NULL, owner_id TEXT NOT NULL, lease_epoch BIGINT NOT NULL, "
        "acquired_at %s NOT NULL, expires_at %s, CONSTRAINT pk_%s PRIMARY KEY (job_name))"
        % (table("job_leases"), ts, ts, label("job_leases")),

        "CREATE TABLE IF NOT EXISTS %s (job_name TEXT NOT NULL, batch_sequence BIGINT NOT NULL, "
        "CONSTRAINT pk_%s PRIMARY KEY (job_name))" % (table("job_sequences"), label("job_sequences")),

        "CREATE TABLE IF NOT EXISTS %s (job_name TEXT NOT NULL, batch_id TEXT NOT NULL, "
        "batch_sequence BIGINT NOT NULL, "
        "run_key TEXT, status TEXT NOT NULL, failed_from TEXT, manifest_json %s NOT NULL, error_message TEXT, "
        "engine_metadata %s, created_at %s NOT NULL, updated_at %s NOT NULL, "
        "CONSTRAINT pk_%s PRIMARY KEY (job_name, batch_id), "
        "CONSTRAINT uq_%s__job_name_batch_sequence UNIQUE (job_name, batch_sequence), "
        "CONSTRAINT uq_%s__job_name_run_key UNIQUE (job_name, run_key))"
        % (table("batches"), js, js, ts, ts, label("batches"), label("batches"), label("batches")),

        "CREATE INDEX IF NOT EXISTS ix_%s__job_name_status ON %s (job_name, status)"
        % (label("batches"), table("batches")),

        "CREATE TABLE IF NOT EXISTS %s (job_name TEXT NOT NULL, batch_id TEXT NOT NULL, output_name TEXT NOT NULL, "
        "receipt_json %s NOT NULL, recorded_at %s NOT NULL, "
        "CONSTRAINT pk_%s PRIMARY KEY (job_name, batch_id, output_name))"
        % (table("output_receipts"), js, ts, label("output_receipts")),

        "CREATE TABLE IF NOT EXISTS %s (job_name TEXT NOT NULL, batch_id TEXT NOT NULL, output_name TEXT NOT NULL, "
        "event_sequence BIGINT NOT NULL, destination TEXT NOT NULL, payload_json %s NOT NULL, status TEXT NOT NULL, "
        "created_at %s NOT NULL, sent_at %s, "
        "CONSTRAINT pk_%s PRIMARY KEY (job_name, batch_id, output_name, event_sequence))"
        % (table("outbox_events"), js, ts, ts, label("outbox_events")),

        "CREATE INDEX IF NOT EXISTS ix_%s__job_name_status ON %s (job_name, status)"
        % (label("outbox_events"), table("outbox_events")),

        "CREATE TABLE IF NOT EXISTS %s (job_name TEXT NOT NULL, path_prefix TEXT NOT NULL, header_hash TEXT NOT NULL, "
        "signature TEXT NOT NULL, updated_at %s NOT NULL, CONSTRAINT pk_%s PRIMARY KEY (job_name, path_prefix))"
        % (table("schema_signatures"), ts, label("schema_signatures")),
    ]


def _store_identity(dialect, table, label):
    """Creates the store-identity table and seeds this store a unique, persisted identity (once, at first migrate)."""
    ts = dialect.timestamp_type
    return [
        "CREATE TABLE IF NOT EXISTS %s (store_key TEXT NOT NULL, store_identity TEXT NOT NULL, created_at %s NOT NULL, "
        "CONSTRAINT pk_%s PRIMARY KEY (store_key))" % (table("store_metadata"), ts, label("store_metadata")),
        "INSERT INTO %s (store_key, store_identity, created_at) VALUES ('default', '%s', %s)" % (
            table("store_metadata"), uuid.uuid4(), dialect.now),
    ]


MIGRATIONS = ((1, _initial_schema), (2, _store_identity))
LATEST_VERSION = MIGRATIONS[-1][0]
