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
The passes of a runtime.fan_out run: the job once per item, `concurrency` at a time, in item order; with on_failure
stop no pass starts after one failed. An engine gives `run_one(label, config, secrets) -> (exit code, result)`.
"""

import logging
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from typing import Any

from streamwright.core.jobs.lookups.providers import LookupExecNotAllowedError
from streamwright.core.runtime.logs import bind, fields, log_context

__all__ = ["PassOutcome", "run_passes"]

LOG = logging.getLogger("streamwright.lookups")


@dataclass
class PassOutcome:
    label: str | None
    exit_code: int | None = None     # None: not run (on_failure stop)
    result: Any = None
    error: BaseException | None = None

    @property
    def status(self):
        if self.exit_code is None and self.error is None:
            return "not_run"
        return "ok" if self.error is None and not self.exit_code else "failed"


def run_passes(lookups, run_one, *, concurrency=None, on_failure=None):
    """[PassOutcome] in item order; KeyboardInterrupt stops the run (the passes running finish their batch)."""
    items = lookups.items()
    if lookups.fan_out is None:
        config, secrets = lookups.inputs()
        code, result = run_one(None, config, secrets)
        return [PassOutcome(None, code, result)]
    settings = lookups.fan_out
    workers = int(concurrency or settings["concurrency"])
    stop_on_failure = (on_failure or settings["on_failure"]) == "stop"
    if not items:
        LOG.warning("fan_out: %s gave no item: nothing to run", settings["over"],
                    extra=fields(event="fan_out_empty"))
    outcomes = [PassOutcome(label) for label, _ in items]

    def one(index):
        label, item = items[index]
        with log_context(job=lookups.job.get("name"), item="%s=%s" % (settings["as"], label)):
            return _one(index, label, item)

    def _one(index, label, item):
        LOG.info("%s %s: starting (%d of %d)", settings["as"], label, index + 1, len(items),
                 extra=fields(event="pass_start"))
        try:
            config, secrets = lookups.inputs(item)
            outcomes[index].exit_code, outcomes[index].result = run_one(label, config, secrets)
        except LookupExecNotAllowedError:
            raise                                   # a usage error: no pass may run
        except Exception as exc:
            outcomes[index].error = exc
        return outcomes[index]

    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="sw-pass") as pool:
        running, failed = set(), False
        for index in range(len(items)):
            if failed and stop_on_failure:
                break
            running.add(pool.submit(bind(one), index))
            if len(running) >= workers:
                done, running = wait(running, return_when=FIRST_COMPLETED)
                failed = failed or any(future.result().status == "failed" for future in done)
        for future in running:
            future.result()
    return outcomes
