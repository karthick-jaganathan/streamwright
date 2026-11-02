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
Running and validating `kind: DuckDBJob` manifests.

`run_job` checks the job (credential-free), resolves its inputs, sets up a Session with an adapter per connection,
opens the bookkeeping store (runtime.state; an in-memory one when the job declares none, which keeps no state
between runs) and hands the batch to the shared Runner with a DuckDBExecutor. Secret values never leave in an
error, a log line of this engine or the state store: they are redacted.
"""

import contextlib
import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass, field

from streamwright.core.jobs import validate as job_validation
from streamwright.core.jobs.duckdb.executor import DuckDBExecutor
from streamwright.core.jobs.duckdb.loading import discover_adapters, load_adapters
from streamwright.core.jobs.duckdb.rules import DuckDBRules
from streamwright.core.jobs.duckdb.session import Session
from streamwright.core.jobs.inputs import effective_config, secret_values
from streamwright.core.jobs.manifest import load_job, manifest_directory
from streamwright.core.jobs.plan import build_plan
from streamwright.core.jobs.refs import resolve
from streamwright.core.jobs.runner import Runner
from streamwright.core.jobs.schema import json_schema as generate_schema
from streamwright.core.jobs.state import DEFAULT_SCHEMA, open_sqlite, open_store
from streamwright.core.jobs.types import written_files
from streamwright.core.runtime.redact import Redactor

__all__ = [
    "JobInvalidError",
    "RunResult",
    "duckdb_rules",
    "job_state",
    "json_schema",
    "open_job_state",
    "redactor",
    "run_job",
    "validate_job",
    "validate_job_text",
]

LOG = logging.getLogger("streamwright.duckdb")
_EPHEMERAL_STATE = "__streamwright_state"


class JobInvalidError(Exception):
    """The job has problems; `issues` lists them."""

    def __init__(self, issues):
        self.issues = issues
        super().__init__("the job has problems:\n  * " + "\n  * ".join(str(i) for i in issues))


@dataclass
class RunResult:
    exit_code: int
    job_name: str
    plan: str
    batches: list[str] = field(default_factory=list)
    outputs: dict[str, int | None] = field(default_factory=dict)      # output -> rows, from the receipts
    skipped: set[str] = field(default_factory=set)                    # outputs already committed in an earlier attempt
    error: str | None = None
    # output -> where it wrote: its files (a file_name template's, split_by's), else its destination
    locations: dict[str, list[str]] = field(default_factory=dict)


def _providers(job):
    connections = job.get("connections") if isinstance(job, dict) else None
    return [c["provider"] for c in (connections or {}).values()
            if isinstance(c, dict) and isinstance(c.get("provider"), str)]


def duckdb_rules(job, allowed=None, entry_points=None):
    """The DuckDBRules for `job`: the adapters of its connections' providers (installed ones authorized first)."""
    adapters, _ = load_adapters(_providers(job), allowed=allowed, entry_points=entry_points)
    return DuckDBRules(adapters)


def json_schema(entry_points=None):
    """The DuckDBJob JSON schema, generated from the rule tables and every installed adapter's connection fields."""
    adapters, _ = load_adapters(discover_adapters(entry_points), entry_points=entry_points)
    return generate_schema(DuckDBRules(adapters, connectors=()))


def validate_job(job, overlays=(), config=None, strict=False, allowed=None):
    """Validates a DuckDBJob (a mapping or a path) without secret values; returns a JobReport."""
    return job_validation.validate_job(job, lambda found: duckdb_rules(found, allowed), overlays, config, strict)


def validate_job_text(text, overlays=(), config=None, strict=False, allowed=None):
    """validate_job() for YAML text."""
    return job_validation.validate_job_text(text, lambda found: duckdb_rules(found, allowed), overlays, config,
                                            strict)


def redactor(secrets):
    """A Redactor for the secret values, also in the form they take inside a quoted SQL literal."""
    redact = Redactor()
    for value in (secrets or {}).values():
        if value is None or isinstance(value, bool):
            continue
        redact.add(value)
        redact.add(str(value).replace("'", "''"))
    return redact


class _RedactFilter(logging.Filter):
    """Redacts secrets from this engine's log records (messages and tracebacks)."""

    def __init__(self, redact):
        super().__init__()
        self.redact = redact

    def filter(self, record):
        message = record.getMessage()
        redacted = self.redact(message)
        if redacted != message:
            record.msg, record.args = redacted, ()
        if record.exc_info and not record.exc_text:
            record.exc_text = logging.Formatter().formatException(record.exc_info)
        if record.exc_text:
            record.exc_text = self.redact(record.exc_text)
        return True


def _used(job):
    """The connections a step, an output or the state store uses (only those are set up)."""
    names = {s["connection"] for s in job.get("steps") or () if s.get("connection")}
    return names | _writable(job)


def _writable(job):
    names = {o.get("connection") for o in job.get("outputs") or ()}
    state = (job.get("runtime") or {}).get("state") or {}
    if state.get("connection"):
        names.add(state["connection"])
    return names


def open_job_state(job, session, adapters, base_dir):
    """The job's StateStore: runtime.state.sqlite, runtime.state.connection, or an in-memory store."""
    state = (job.get("runtime") or {}).get("state") or {}
    if state.get("sqlite"):
        path = state["sqlite"]
        return open_sqlite(path if os.path.isabs(path) or base_dir is None else os.path.join(base_dir, path))
    if state.get("connection"):
        name = state["connection"]
        connection = job["connections"][name]
        return adapters[connection["provider"]].open_state(session, name, connection,
                                                           state.get("schema", DEFAULT_SCHEMA))
    session.attach(_EPHEMERAL_STATE, ":memory:")
    return open_store(session.connection_factory(), "duckdb", DEFAULT_SCHEMA, catalog=_EPHEMERAL_STATE)


def _load(job, overlays):
    if isinstance(job, Mapping):
        return dict(job), None
    return load_job(job, overlays=overlays), manifest_directory(job)


def run_job(job, *, config=None, secrets=None, overlays=(), allowed_connectors=None, dry_run=False, environ=None,
            base_dir=None, resume_only=False, strict=False):
    """
    Runs a DuckDBJob (a mapping or a path) once: unfinished batches are resumed first (`resume_only`: and nothing
    else). `config`/`secrets` are provided values (spec definitions apply; secrets also come from
    STREAMWRIGHT_SECRET_* in `environ`).
    Raises JobInvalidError before anything runs; returns a RunResult (exit_code 0 on success).
    """
    raw, found_dir = _load(job, list(overlays))
    secret_map = secret_values(raw, secrets, environ)
    redact = redactor(secret_map)
    log_filter = _RedactFilter(redact)
    LOG.addFilter(log_filter)
    try:
        return _run(raw, config=config, secret_map=secret_map, allowed_connectors=allowed_connectors, dry_run=dry_run,
                    base_dir=base_dir or found_dir, redact=redact, resume_only=resume_only, strict=strict)
    except JobInvalidError:
        raise
    except Exception as exc:
        text = redact(str(exc))
        if text == str(exc):
            raise
        try:
            redacted = type(exc)(text)
        except Exception:
            redacted = RuntimeError(text)
        raise redacted from None                  # the unredacted error must not appear as the context
    finally:
        LOG.removeFilter(log_filter)


def _run(raw, *, config, secret_map, allowed_connectors, dry_run, base_dir, redact, resume_only, strict):
    report = job_validation.validate_job(raw, lambda found: duckdb_rules(found, allowed_connectors), config=config,
                                         strict=strict)
    if not report.valid:
        raise JobInvalidError(report.problems)
    values = effective_config(raw, config)
    job, missing = resolve(raw, values, secret_map)
    if missing:
        raise JobInvalidError(missing)
    rules = duckdb_rules(job, allowed_connectors)
    plan = build_plan(job, rules.dependencies_of)
    if dry_run:
        return RunResult(0, job["name"], plan.describe())
    session = Session(job.get("runtime"), writable=_writable(job), base_dir=base_dir)
    try:
        for name in sorted(_used(job)):
            connection = job["connections"][name]
            adapter = rules.adapters.get(connection["provider"])
            if adapter is not None:
                adapter.configure(session, name, connection)
        store = open_job_state(job, session, rules.adapters, base_dir)
        store.initialize()
        executor = DuckDBExecutor(job, plan, session, rules.adapters, secrets=secret_map,
                                  allowed_connectors=allowed_connectors, base_dir=base_dir,
                                  previous_signatures=store.get_schema_signatures(job["name"]),
                                  declared_connections=raw["connections"], config=values)
        state = (job.get("runtime") or {}).get("state") or {}
        runner = Runner(job["name"], store, executor, redact=redact, lease_seconds=state.get("ttl_seconds", 300),
                        engine_metadata={"engine": "duckdb"})
        session.start_clock()
        code = runner.run(mode="once", run_key=state.get("run_key"), max_messages=0 if resume_only else 500)
        outputs, locations = {}, {}
        skipped = set()
        for batch_id in runner.processed:
            for name, receipt in store.get_receipts(job["name"], batch_id).items():
                outputs[name] = receipt.get("rows")
                locations[name] = written_files(receipt) or [receipt.get("destination")]
                if receipt.get("already_committed"):
                    skipped.add(name)
        return RunResult(code, job["name"], plan.describe(), list(runner.processed), outputs, skipped, runner.error,
                         locations)
    finally:
        session.close()


@contextlib.contextmanager
def job_state(job, *, config=None, secrets=None, overlays=(), allowed_connectors=None, environ=None):
    """(job name, StateStore) of a job's runtime.state, for listing, retrying or abandoning batches."""
    raw, base_dir = _load(job, list(overlays))
    if not ((raw.get("runtime") or {}).get("state")):
        raise JobInvalidError(["the job declares no runtime.state: its batches are not kept between runs"])
    resolved, missing = resolve(raw, effective_config(raw, config), secret_values(raw, secrets, environ))
    if missing:
        raise JobInvalidError(missing)
    rules = duckdb_rules(resolved, allowed_connectors)
    state = resolved["runtime"]["state"]
    session = Session(writable=[state["connection"]] if state.get("connection") else (), base_dir=base_dir)
    try:
        if state.get("connection"):
            connection = resolved["connections"][state["connection"]]
            rules.adapters[connection["provider"]].configure(session, state["connection"], connection)
        store = open_job_state(resolved, session, rules.adapters, base_dir)
        store.initialize()
        yield resolved["name"], store
    finally:
        session.close()
