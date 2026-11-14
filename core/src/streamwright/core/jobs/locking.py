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
runtime.state.scope, runtime.state.lock and the failed-job policy, shared by every engine.

A run's lease is a set of tokens in sw_leases:
  - the scope token: `scope:<scope>` (or "" without a scope), always needed: one active run per job and scope;
  - `lock.token` (without `each`): a fixed token, also needed;
  - with `each: "{{ config.NAME }}"`: one token per item of that input (`token` rendered with `{{ <as> }}` set to
    the item); they exclude each other across the job's scopes (leases are per job name), and are the only
    tokens `policy: prune` drops.
A batch pins its inputs (scope, config values, admitted item tokens) in its manifest; resuming it needs the same
tokens and rebuilds the work from the pinned values.
"""

import re
from dataclasses import dataclass, field
from types import MappingProxyType

from streamwright.core.config.inputs import InputError, parse_duration
from streamwright.core.jobs.fields import Field
from streamwright.core.jobs.state.tables.base import KEY_LEN

__all__ = ["DEFAULT_FAILED_JOB_POLICY", "DEFAULT_MAX_JOB_ATTEMPTS", "FAILED_JOB_POLICIES", "MAX_KEY_LENGTH",
           "MAX_SCOPE_LENGTH", "SCOPE_PREFIX", "STATE_FIELDS", "FailedJobPolicy", "LockPlan", "applies_migrations",
           "batch_tokens", "check_key", "check_lock_source", "check_state_lock", "lock_plan", "parse_lock_timeout",
           "resolve_failed_job_policy", "scope_token"]

SCOPE_PREFIX = "scope:"
MAX_KEY_LENGTH = KEY_LEN        # lock tokens and job keys are key columns of the state tables
MAX_SCOPE_LENGTH = MAX_KEY_LENGTH - len(SCOPE_PREFIX)   # its token is `scope:<scope>`
_LOCK_POLICIES = ("fail", "wait", "prune")
FAILED_JOB_POLICIES = ("abandon", "retry", "block")
DEFAULT_FAILED_JOB_POLICY = "abandon"
DEFAULT_MAX_JOB_ATTEMPTS = 3
_EACH = re.compile(r"^\{\{\s*config\.([A-Za-z_][A-Za-z0-9_]*)\s*\}\}$")
_SECONDS = re.compile(r"^[0-9]+(\.[0-9]+)?$")

STATE_FIELDS = MappingProxyType({
    "apply_migrations": Field("boolean", default=False,
                              description="true: a run or state action creates or upgrades the state tables when it "
                                          "opens them; default false: it fails while a migration is pending (apply "
                                          "them with `state migrate`)"),
    "job_key": Field("string", description="a batch with this key runs once per scope: a later run with it does "
                                           "nothing (e.g. daily-{{ config.day }}; --job-key overrides it)"),
    "failed_job_policy": Field("string", enum=FAILED_JOB_POLICIES, default=DEFAULT_FAILED_JOB_POLICY,
                               description="what a run does with a failed job (batch) of its scope, before new work: "
                                           "abandon (default) gives it up (status abandoned, its error kept) and "
                                           "runs on; retry resumes it, from the status it failed in and with its "
                                           "pinned inputs, while it has been started fewer than max_job_attempts "
                                           "times, then blocks; block stops the run until `state retry` or `state "
                                           "abandon` (--failed-job-policy overrides it)"),
    "max_job_attempts": Field("integer", minimum=1, default=DEFAULT_MAX_JOB_ATTEMPTS,
                              description="failed_job_policy retry only: how many times a failed job may be started "
                                          "in all (default %d, the first run included); then runs stop until `state "
                                          "retry` or `state abandon` (--max-job-attempts overrides it)"
                                          % DEFAULT_MAX_JOB_ATTEMPTS),
    "scope": Field("string", description="the state scope: batches, recovery, job keys, carried values, sequences "
                                         "and the outbox are kept per scope, and one run per scope is active; e.g. "
                                         "client:{{ config.client }}"),
    "lock": Field("object", fields={
        "each": Field(("string", "array"), description="{{ config.NAME }}: a lock token per item of that list input"),
        "as": Field("string", pattern=r"^[A-Za-z_][A-Za-z0-9_]*$",
                    description="the item's name in `token` (default: item)"),
        "token": Field("string", description="the lock token; with each, it names {{ <as> }}, e.g. "
                                             "account:{{ item }}"),
        "policy": Field("string", enum=_LOCK_POLICIES, default="fail",
                        description="when a token is held: fail (default), wait (up to timeout), prune (run the "
                                    "items that are free, skip the rest)"),
        "timeout": Field(("number", "string"), description="policy wait: how long, in seconds or as 30s, 5m"),
    }),
})


def applies_migrations(job, option=False):
    """
    Whether opening the job's state tables applies their pending migrations: with --apply-state-migrations (`option`)
    or runtime.state.apply_migrations: true, and always for a store that lives only in memory (no runtime.state, or
    sqlite: ":memory:"); otherwise a pending migration is an error (MigrationsPendingError).
    """
    state = (job.get("runtime") or {}).get("state") or {}
    in_memory = not (state.get("connection") or state.get("sqlite")) or state.get("sqlite") == ":memory:"
    return bool(option) or in_memory or state.get("apply_migrations") is True


@dataclass(frozen=True)
class FailedJobPolicy:
    """What a run does with its scope's failed job: abandon it, retry it (while attempts < max_attempts), or block."""
    policy: str = DEFAULT_FAILED_JOB_POLICY
    max_attempts: int = DEFAULT_MAX_JOB_ATTEMPTS

    def refusal(self, batch):
        """Why a run may not retry this failed batch (policy block, its attempts used up, ...); None: it may."""
        attempts = int(batch.get("attempts") or 1)
        if self.policy != "retry":
            return "failed job policy %s" % self.policy
        if attempts >= self.max_attempts:
            return "started %d time(s), max job attempts %d" % (attempts, self.max_attempts)
        return None if batch.get("failed_from") else "it has no status to resume from"


def resolve_failed_job_policy(job, policy=None, max_attempts=None):
    """
    The job's FailedJobPolicy: runtime.state.failed_job_policy and max_job_attempts, overridden by `policy` and
    `max_attempts` (--failed-job-policy, --max-job-attempts). InputError for a bad value.
    """
    state = (job.get("runtime") or {}).get("state") or {}
    chosen = policy if policy is not None else state.get("failed_job_policy") or DEFAULT_FAILED_JOB_POLICY
    if chosen not in FAILED_JOB_POLICIES:
        raise InputError("unknown failed job policy %r (%s)" % (chosen, ", ".join(FAILED_JOB_POLICIES)))
    attempts = max_attempts if max_attempts is not None else state.get("max_job_attempts", DEFAULT_MAX_JOB_ATTEMPTS)
    if isinstance(attempts, bool) or not isinstance(attempts, int) or attempts < 1:
        raise InputError("max job attempts must be a whole number of at least 1, not %r" % (attempts,))
    if max_attempts is not None and chosen != "retry":
        raise InputError("--max-job-attempts needs failed job policy retry (got %s)" % chosen)
    return FailedJobPolicy(chosen, attempts)


def scope_token(scope):
    """The lease token of a scope ("" without one)."""
    return SCOPE_PREFIX + scope if scope else ""


def batch_tokens(batch):
    """The tokens a batch needs to be retried, abandoned or resumed: its scope's and the ones it pinned."""
    return tuple(dict.fromkeys((scope_token(batch.get("scope") or ""),
                                *(((batch.get("tokens") or {}).get("accepted")) or ()))))


def parse_lock_timeout(value):
    """Seconds from a number, a numeric text or a duration (30s, 5m); None -> 0."""
    if value is None:
        return 0.0
    if isinstance(value, bool):
        raise InputError("invalid lock timeout %r" % (value,))
    if isinstance(value, (int, float)):
        seconds = float(value)
    elif _SECONDS.match(str(value).strip()):
        seconds = float(str(value).strip())
    else:
        seconds = parse_duration(str(value).strip()).total_seconds()
    if seconds < 0:
        raise InputError("the lock timeout cannot be negative")
    return seconds


def _token_pattern(name):
    return re.compile(r"\{\{\s*" + re.escape(name) + r"\s*\}\}")


def check_state_lock(state):
    """Problems of runtime.state's scope and lock, as (path below runtime.state, message)."""
    found = []
    if "run_key" in state:
        found.append((("run_key",), "run_key was renamed job_key"))
    if "max_job_attempts" in state and (state.get("failed_job_policy") or DEFAULT_FAILED_JOB_POLICY) != "retry":
        found.append((("max_job_attempts",), "max_job_attempts needs failed_job_policy: retry (the policy is %s)"
                      % (state.get("failed_job_policy") or DEFAULT_FAILED_JOB_POLICY)))
    scope = state.get("scope")
    if scope is not None and (not isinstance(scope, str) or len(scope) > MAX_SCOPE_LENGTH):
        found.append((("scope",), "scope must be text of at most %d characters" % MAX_SCOPE_LENGTH))
    lock = state.get("lock")
    if lock is None:
        return found
    if not isinstance(lock, dict):
        return [*found, (("lock",), "lock must be a mapping")]
    each, token, name = lock.get("each"), lock.get("token"), lock.get("as") or "item"
    if each is not None and not (isinstance(token, str) and _token_pattern(name).search(token)):
        found.append((("lock", "token"), "with each, token must contain {{ %s }}" % name))
    if each is None and not token:
        found.append((("lock",), "lock needs token, or each and token"))
    if isinstance(token, str) and token.startswith(SCOPE_PREFIX):
        found.append((("lock", "token"), "lock tokens cannot start with %r (scope tokens do)" % SCOPE_PREFIX))
    if lock.get("policy") == "prune" and each is None:
        found.append((("lock", "policy"), "policy prune needs each (only item tokens can be pruned)"))
    if lock.get("timeout") is not None:
        try:
            parse_lock_timeout(lock["timeout"])
        except InputError as exc:
            found.append((("lock", "timeout"), str(exc)))
    return found


def check_lock_source(raw):
    """Problems of runtime.state.lock as written (before inputs are resolved): `each` names one config input."""
    lock = ((raw.get("runtime") or {}).get("state") or {}).get("lock") if isinstance(raw, dict) else None
    each = lock.get("each") if isinstance(lock, dict) else None
    if each is not None and not (isinstance(each, str) and _EACH.match(each.strip())):
        return ["runtime.state.lock.each must be exactly {{ config.NAME }} (a list input), not %r" % (each,)]
    return []


def _items(value):
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [item for item in value if str(item).strip()]
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return [value]


@dataclass(frozen=True)
class LockPlan:
    """What a run locks: its scope, a fixed token, the items of a list input with their tokens, and the policy."""
    scope: str = ""
    policy: str = "fail"
    timeout: float = 0.0
    token: str | None = None
    config_name: str | None = None
    items: tuple = ()
    item_tokens: tuple[str, ...] = ()
    values: dict = field(default_factory=dict, compare=False)

    @property
    def tokens(self):
        """The tokens the run needs (never pruned)."""
        return (scope_token(self.scope),) + ((self.token,) if self.token else ())

    def inputs(self, lease):
        """
        The pinned inputs of a new batch under `lease`: scope, config values (the lock input narrowed to the admitted
        items) and the admitted item tokens; None when every item was pruned.
        """
        values = dict(self.values)
        admitted = [t for t in self.item_tokens if t in lease.tokens]
        if self.config_name is not None:
            if self.item_tokens and not admitted:
                return None
            kept = [item for item, token in zip(self.items, self.item_tokens, strict=True) if token in lease.tokens]
            original = values.get(self.config_name)
            values[self.config_name] = kept if not isinstance(original, str) else ",".join(map(str, kept))
        return {"scope": self.scope, "config": values, "tokens": admitted}


def check_key(what, value):
    """InputError when `value` (a lock token or job key) is longer than the state tables' key columns."""
    if len(str(value)) > MAX_KEY_LENGTH:
        raise InputError("%s %r... is %d characters: at most %d" % (what, str(value)[:40], len(str(value)),
                                                                   MAX_KEY_LENGTH))


def _check_tokens(tokens):
    for token in tokens:
        check_key("the lock token", token)


def lock_plan(raw, job, values, *, policy=None, timeout=None, persistent=True):
    """
    The LockPlan of a job (`raw`: as written, `job`: resolved, `values`: its effective config). `policy`/`timeout`
    override lock.policy and lock.timeout (the --state-lock-* options); they need a persistent state store, prune
    needs each and a timeout needs wait.
    """
    state = (job.get("runtime") or {}).get("state") or {}
    lock = state.get("lock") or {}
    if (policy is not None or timeout is not None) and not persistent:
        raise InputError("--state-lock-policy and --state-lock-timeout need a persistent runtime.state (sqlite or "
                         "connection)")
    effective = policy or lock.get("policy") or "fail"
    if effective not in _LOCK_POLICIES:
        raise InputError("unknown lock policy %r (fail, wait, prune)" % (effective,))
    if effective == "prune" and lock.get("each") is None:
        raise InputError("lock policy prune needs runtime.state.lock.each (only item tokens can be pruned)")
    if timeout is not None and effective != "wait":
        raise InputError("--state-lock-timeout needs lock policy wait (got %s)" % effective)
    scope = state.get("scope") or ""
    if not isinstance(scope, str) or len(scope) > MAX_SCOPE_LENGTH:
        raise InputError("runtime.state.scope must render to text of at most %d characters" % MAX_SCOPE_LENGTH)
    seconds = parse_lock_timeout(timeout if timeout is not None else lock.get("timeout"))
    token = lock.get("token")
    if lock.get("each") is None:
        _check_tokens([token] if token else [])
        return LockPlan(scope, effective, seconds, token, values=dict(values))
    raw_each = str((((raw.get("runtime") or {}).get("state") or {}).get("lock") or {}).get("each") or "")
    match = _EACH.match(raw_each.strip())
    if not match:
        raise InputError("runtime.state.lock.each must be exactly {{ config.NAME }}")
    name = match.group(1)
    items = _items(values.get(name))
    pattern = _token_pattern(lock.get("as") or "item")
    tokens = [pattern.sub(lambda _, item=item: str(item), token) for item in items]
    bad = [t for t in tokens if t.startswith(SCOPE_PREFIX)]
    if bad:
        raise InputError("lock tokens cannot start with %r: %s" % (SCOPE_PREFIX, ", ".join(bad)))
    _check_tokens(tokens)
    return LockPlan(scope, effective, seconds, None, name, tuple(items), tuple(tokens), dict(values))
