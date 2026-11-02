"""The shared execution policy (contract v1.2 §6a): one retry, timeout and on_error vocabulary for every engine."""

import time

import pytest

from streamwright.core.jobs import policy


def _policy(**item):
    return policy.ExecutionPolicy.of(item)


def test_defaults_are_the_shared_net_policy():
    resolved = policy.ExecutionPolicy.of({})
    assert resolved.retry == dict(policy.DEFAULTS["retry"])
    assert resolved.on_error == "fail" and resolved.timeout_seconds is None


def test_guard_retries_a_transient_failure_through_the_one_shared_retry():
    attempts = []

    def work():
        attempts.append(1)
        if len(attempts) < 3:
            raise policy.TransientError("try again")
        return "done"

    p = _policy(retry={"max_attempts": 3, "backoff": "constant"})
    assert policy.guard(p, work, classify=policy.classify_transient(p), on_retry=lambda *a: None) == "done"
    assert len(attempts) == 3


def test_guard_does_not_retry_a_non_transient_error():
    p = _policy(retry={"max_attempts": 3})

    def work():
        raise ValueError("permanent")

    with pytest.raises(ValueError):
        policy.guard(p, work, classify=policy.classify_transient(p))


def test_retry_codes_match_an_error_code():
    p = _policy(retry={"max_attempts": 2, "backoff": "constant", "codes": [503]})
    seen = []

    class ThrottledError(Exception):
        code = 503

    def work():
        seen.append(1)
        if len(seen) < 2:
            raise ThrottledError()
        return "ok"

    assert policy.guard(p, work, classify=policy.classify_transient(p)) == "ok"


def test_on_error_fail_reraises_but_skip_and_warn_tolerate():
    def boom():
        raise RuntimeError("nope")

    strict = _policy(on_error="fail")
    with pytest.raises(RuntimeError):
        policy.guard(strict, boom, classify=policy.classify_transient(strict))

    for mode in ("skip", "warn"):
        tolerated = []
        p = _policy(on_error=mode)
        assert policy.guard(p, boom, classify=policy.classify_transient(p),
                            on_tolerated=lambda exc, m, sink=tolerated: sink.append(m)) is policy.SKIPPED
        assert tolerated == [mode]


def test_timeout_bounds_each_attempt():
    p = _policy(timeout=0.05, on_error="fail")
    with pytest.raises(TimeoutError):
        policy.guard(p, lambda: time.sleep(1), classify=policy.classify_transient(p))
    tolerant = _policy(timeout=0.05, on_error="skip")
    assert policy.guard(tolerant, lambda: time.sleep(1), classify=policy.classify_transient(tolerant)) is policy.SKIPPED


def test_check_policy_names_the_engine_for_an_unsupported_key():
    problems = policy.check_policy({"paginator": {}, "retry": {}}, policy.OUTPUT_POLICY_KEYS, "duckdb",
                                   surface="outputs")
    assert problems == [("unsupported-argument", "`paginator` is not supported by the duckdb engine on outputs",
                         False)]


def test_check_policy_rejects_an_unknown_on_error():
    assert policy.check_policy({"on_error": "retry"}, policy.STEP_POLICY_KEYS, "spark", surface="steps") == [
        ("bad-value", "on_error is one of: fail, skip, warn", False)]
