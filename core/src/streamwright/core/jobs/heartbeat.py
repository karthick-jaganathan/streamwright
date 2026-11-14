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

"""The lease heartbeat of a running batch (the batch runner's)."""

import threading
import time

from streamwright.core.jobs.state import StaleLeaseError
from streamwright.core.runtime.logs.context import bind

__all__ = ["Heartbeat"]


class Heartbeat:
    """
    Renews the lease in the background while a batch runs; remembers why renewal stopped. Only a lost lease (epoch or
    owner changed) or a lease that expired while renewals failed stops it: a failed renewal (e.g. a lock conflict
    with a contending acquire) is retried soon.
    """

    RETRY_SECONDS = 0.2

    def __init__(self, runner, interval):
        self.runner, self.interval = runner, interval
        self.lost = None
        self._stop = threading.Event()
        self._thread = threading.Thread(target=bind(self._beat), name="streamwright-lease-heartbeat", daemon=True)

    def _beat(self):
        runner = self.runner
        valid_until = time.monotonic() + runner.lease_seconds
        wait = self.interval
        while not self._stop.wait(wait):
            try:
                runner.lease = runner.state.renew_lease(runner.lease, runner.lease_seconds)
            except StaleLeaseError as exc:
                self.lost = exc
                return
            except Exception as exc:
                if time.monotonic() >= valid_until:
                    self.lost = exc
                    return
                runner.log("lease_renewal_retried", error=runner.redact(str(exc)))
                wait = min(self.interval, self.RETRY_SECONDS)
                continue
            valid_until = time.monotonic() + runner.lease_seconds
            wait = self.interval

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc_info):
        self._stop.set()
        self._thread.join()
        return False
