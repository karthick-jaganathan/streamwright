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
between runs) and hands the batches to the shared Runner with a DuckDBExecutor. A job with a consume step gets its
broker as the Runner's consumer (and its publish outputs' brokers as publishers), so `mode` can drain, follow or
process what is available now. Secret values never leave in an
error, a log line of this engine or the state store: they are redacted.
"""

import contextlib
import logging
import os
from collections.abc import Mapping

from streamwright.core.config.inputs import secrets_from_env
from streamwright.core.jobs import validate as job_validation
from streamwright.core.jobs.duckdb.executor import DuckDBExecutor
from streamwright.core.jobs.duckdb.loading import discover_adapters, load_adapters
from streamwright.core.jobs.duckdb.results import RunResult, combine_passes
from streamwright.core.jobs.duckdb.rules import DuckDBRules
from streamwright.core.jobs.duckdb.session import Session
from streamwright.core.jobs.duckdb.steps import message_kind
from streamwright.core.jobs.inputs import InputError, declared_inputs, effective_config, secret_values, state_secrets
from streamwright.core.jobs.locking import applies_migrations, lock_plan, resolve_failed_job_policy
from streamwright.core.jobs.log_labels import job_log_context
from streamwright.core.jobs.lookups import Lookups, has_lookups, lookup_optional, state_inputs
from streamwright.core.jobs.lookups.passes import run_passes
from streamwright.core.jobs.manifest import load_job, manifest_directory
from streamwright.core.jobs.plan import build_plan
from streamwright.core.jobs.refs import resolve
from streamwright.core.jobs.runner import DEFAULT_MAX_MESSAGES, BrokerCall, Runner
from streamwright.core.jobs.schema import json_schema as generate_schema
from streamwright.core.jobs.state import DEFAULT_SCHEMA, open_sqlite, open_store
from streamwright.core.jobs.state.target import describe_connection
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
        _mask(redact, value)
    return redact


def _mask(redact, value):
    if value is None or isinstance(value, bool) or value == "":
        return
    redact.add(value)
    redact.add(str(value).replace("'", "''"))


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
        schema = state.get("schema", DEFAULT_SCHEMA)
        store = adapters[connection["provider"]].open_state(session, name, connection, schema)
        store.target = describe_connection(name, connection["provider"], connection, schema)
        return store
    session.attach(_EPHEMERAL_STATE, ":memory:")
    return open_store(session.connection_factory(), "duckdb", DEFAULT_SCHEMA, catalog=_EPHEMERAL_STATE,
                      target="duckdb in memory (no runtime.state)")


def _load(job, overlays):
    if isinstance(job, Mapping):
        return dict(job), None
    return load_job(job, overlays=overlays), manifest_directory(job)


def run_job(job, *, config=None, secrets=None, overlays=(), allowed_connectors=None, dry_run=False, environ=None,
            base_dir=None, resume_only=False, strict=False, state_lock_policy=None, state_lock_timeout=None,
            job_key=None, mode="once", interval=10.0, max_messages=DEFAULT_MAX_MESSAGES,
            apply_state_migrations=False, failed_job_policy=None, max_job_attempts=None, allow_lookup_exec=False,
            fan_out_concurrency=None, fan_out_on_failure=None):
    """
    Runs a DuckDBJob (a mapping or a path) once: unfinished batches are resumed first (`resume_only`: and nothing
    else). `config`/`secrets` are provided values (spec definitions apply; secrets also come from
    STREAMWRIGHT_SECRET_* in `environ`); `job_key` overrides runtime.state.job_key, `state_lock_policy` and
    `state_lock_timeout` its lock's policy and wait. `mode` (once, drain, available_now, follow; all but once need a
    consume step), `interval` (seconds, follow) and `max_messages` (per batch; 0: resume and relay only) are the
    Runner's. A pending state migration fails the run (MigrationsPendingError) unless `apply_state_migrations`
    (--apply-state-migrations) or runtime.state.apply_migrations: true apply it. `failed_job_policy` (retry, block)
    and `max_job_attempts` override runtime.state's: what the run does with a failed batch of its scope.
    spec.lookups fill the inputs not given explicitly (command and function lookups need `allow_lookup_exec`);
    runtime.fan_out runs the job once per item (`fan_out_concurrency`, `fan_out_on_failure` override it), and the
    result then has one `passes` entry per item.
    Raises JobInvalidError before anything runs; returns a RunResult (exit_code 0 on success).
    """
    raw, found_dir = _load(job, list(overlays))
    redact = Redactor()
    log_filter = _RedactFilter(redact)
    LOG.addFilter(log_filter)
    options = {"allowed_connectors": allowed_connectors, "dry_run": dry_run, "base_dir": base_dir or found_dir,
               "redact": redact, "resume_only": resume_only, "strict": strict, "state_lock_policy": state_lock_policy,
               "state_lock_timeout": state_lock_timeout, "job_key": job_key, "mode": mode, "interval": interval,
               "max_messages": max_messages, "apply_state_migrations": apply_state_migrations,
               "failed_job_policy": failed_job_policy, "max_job_attempts": max_job_attempts}
    try:
        if not has_lookups(raw):
            secret_map = secret_values(raw, secrets, environ)
            for value in secret_map.values():
                _mask(redact, value)
            with job_log_context(raw, config):
                return _run(raw, config=config, secret_map=secret_map, **options)
        return _run_lookups(raw, config, secrets, environ, options, allow_exec=allow_lookup_exec,
                            fan_out=(fan_out_concurrency, fan_out_on_failure))
    except (JobInvalidError, InputError):
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


def _run_lookups(raw, config, secrets, environ, options, *, allow_exec, fan_out):
    """A run whose inputs come from spec.lookups: once, or once per runtime.fan_out item (a RunResult per pass)."""
    declared = declared_inputs(raw, "secrets")
    if options["dry_run"]:                        # connects to nothing: the lookups do not run
        bound = {s: {n for n, d in declared_inputs(raw, s).items() if (d or {}).get("lookup")}
                 for s in ("config", "secrets")}
        config = {**dict.fromkeys(bound["config"], "<lookup>"), **(config or {})}
        secrets = {**dict.fromkeys(bound["secrets"], "<lookup>"), **(secrets or {})}
        return _run(raw, config=config, secret_map=secret_values(raw, secrets, environ), **options)
    report = job_validation.validate_job(raw, lambda found: duckdb_rules(
        found, options["allowed_connectors"]), config=config, strict=options["strict"])
    if not report.valid:
        raise JobInvalidError(report.problems)
    explicit = {**secrets_from_env(list(declared), environ), **(secrets or {})}
    lookups = Lookups(raw, config, explicit, allow_exec=allow_exec, enrol=lambda value: _mask(options["redact"], value),
                      base_dir=options["base_dir"])

    def run_one(label, pass_config, pass_secrets):
        secret_map = secret_values(raw, pass_secrets, environ)
        for value in secret_map.values():
            _mask(options["redact"], value)
        with job_log_context(raw, pass_config):
            result = _run(raw, config=pass_config, secret_map=secret_map, **options)
        return result.exit_code, result

    concurrency = fan_out[0]
    shared = sorted(name for name, c in (raw.get("connections") or {}).items()
                    if isinstance(c, dict) and c.get("provider") == "duckdb"
                    and c.get("path") not in (None, ":memory:"))
    if lookups.fan_out and shared and int(concurrency or lookups.fan_out["concurrency"]) > 1:
        LOG.warning("fan_out: the passes share the DuckDB database of connection(s) %s: they run one at a time",
                    ", ".join(shared))
        concurrency = 1
    outcomes = run_passes(lookups, run_one, concurrency=concurrency, on_failure=fan_out[1])
    if lookups.fan_out is None:
        return outcomes[0].result
    return combine_passes(raw["name"], outcomes, lookups.fan_out, options["redact"])


def _broker_calls(job, rules):
    """(consumer, {output: publisher}): BrokerCalls of the consume step and the publish outputs, a broker per
    connection."""
    brokers = {}

    def call(item):
        name = item["connection"]
        adapter = rules.adapters[job["connections"][name]["provider"]]
        if name not in brokers:
            brokers[name] = adapter.open_broker(job["connections"][name])
        return BrokerCall(brokers[name], item.get("arguments") or {})

    connections = job["connections"]
    consumer = next((call(step) for step in job.get("steps") or ()
                     if message_kind(step, connections, rules.adapters) == "consume"), None)
    publishers = {output["name"]: call(output) for output in job.get("outputs") or ()
                  if message_kind(output, connections, rules.adapters) == "publish"}
    return consumer, publishers, list(brokers.values())


def _run(raw, *, config, secret_map, allowed_connectors, dry_run, base_dir, redact, resume_only, strict,
         state_lock_policy=None, state_lock_timeout=None, job_key=None, mode="once", interval=10.0,
         max_messages=DEFAULT_MAX_MESSAGES, apply_state_migrations=False, failed_job_policy=None,
         max_job_attempts=None):
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
    state = (job.get("runtime") or {}).get("state") or {}
    persistent = bool(state.get("connection") or state.get("sqlite"))
    lock = lock_plan(raw, job, values, policy=state_lock_policy, timeout=state_lock_timeout, persistent=persistent)
    failed_jobs = resolve_failed_job_policy(job, failed_job_policy, max_job_attempts)
    consumes = any(message_kind(step, job["connections"], rules.adapters) == "consume"
                   for step in job.get("steps") or ())
    if mode != "once" and not consumes and max_messages and not resume_only:
        raise InputError("--%s needs a job that consumes a broker (a step with method: consume): without one every "
                         "batch is new work and the run would not end" % mode.replace("_", "-"))
    if job_key is not None and not persistent:
        raise InputError("--job-key needs a persistent runtime.state (sqlite or connection): an in-memory store "
                         "forgets the key")
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
        store.prepare(migrate=applies_migrations(job, apply_state_migrations))
        signatures = store.get_schema_signatures(job["name"])

        def prepare(inputs):
            """The executor of a batch, from its pinned config values (a lock input narrowed to its items)."""
            pinned = effective_config(raw, inputs["config"]) if inputs else values
            pinned_job, problems = resolve(raw, pinned, secret_map)
            if problems:
                raise JobInvalidError(problems)
            return DuckDBExecutor(pinned_job, build_plan(pinned_job, rules.dependencies_of), session, rules.adapters,
                                  secrets=secret_map, allowed_connectors=allowed_connectors, base_dir=base_dir,
                                  previous_signatures=signatures, declared_connections=raw["connections"],
                                  config=pinned)

        consumer, publishers, brokers = _broker_calls(job, rules)
        try:
            runner = Runner(job["name"], store, None, consumer, publishers, redact=redact,
                            lease_seconds=state.get("ttl_seconds", 300), engine_metadata={"engine": "duckdb"},
                            scope=lock.scope, lock=lock, prepare=prepare, failed_jobs=failed_jobs)
            code = runner.run(mode=mode, interval=interval, max_messages=0 if resume_only else max_messages,
                              job_key=job_key if job_key is not None else state.get("job_key"))
        finally:
            for broker in brokers:
                if callable(getattr(broker, "close", None)):
                    broker.close()
        outputs, locations = {}, {}
        skipped = set()
        for batch_id in runner.processed:            # (drain, follow: many batches; their rows add up)
            for name, receipt in store.get_receipts(job["name"], batch_id).items():
                rows = receipt.get("rows")
                outputs[name] = None if rows is None or outputs.get(name, 0) is None else outputs.get(name, 0) + rows
                locations.setdefault(name, []).extend(written_files(receipt) or [receipt.get("destination")])
                if receipt.get("already_committed"):
                    skipped.add(name)
        return RunResult(code, job["name"], plan.describe(), list(runner.processed), outputs, skipped, runner.error,
                         locations, runner.skipped)
    finally:
        session.close()


@contextlib.contextmanager
def job_state(job, *, config=None, secrets=None, overlays=(), allowed_connectors=None, environ=None,
              initialize=True, allow_lookup_exec=False):
    """
    (job name, StateStore, scope) of a job's runtime.state, for listing, retrying or abandoning batches;
    `initialize`: create or upgrade the tables first (False to look at them as they are).
    """
    raw, base_dir = _load(job, list(overlays))
    if not ((raw.get("runtime") or {}).get("state")):
        raise JobInvalidError(["the job declares no runtime.state: its batches are not kept between runs"])
    explicit = {**secrets_from_env(list(declared_inputs(raw, "secrets")), environ), **(secrets or {})}
    secrets, bound = state_inputs(raw, config, explicit, allow_exec=allow_lookup_exec, base_dir=base_dir)
    raw = lookup_optional(raw)
    resolved, missing = resolve(raw, effective_config(raw, config), state_secrets(raw, secrets, environ))
    missing = [text for text in missing if text.split(" ", 1)[0] not in bound]   # e.g. a scope from a fan_out item
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
        if initialize:
            store.prepare(migrate=applies_migrations(resolved))
        yield resolved["name"], store, state.get("scope") or ""
    finally:
        session.close()
