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

"""The log record factory that scrubs every record while LogSetups are in effect, and their stack."""


import contextvars
import logging

CONTEXT_SETUPS = contextvars.ContextVar("streamwright_log_setups", default=())


class RecordFactory:
    """
    The log record factory while LogSetups are in effect: it makes records with the factory it replaced, then scrubs
    them. Python's record factory is process-wide, so the stack of active setups lives here, with it: the first setup
    installs it, the last one to close puts the factory it replaced back (out-of-order closes unwind when the top
    closes).
    """

    def __init__(self, original):
        self.original = original
        self.setups = []

    def __call__(self, *args, **kwargs):
        record = self.original(*args, **kwargs)
        scrub_record(record, self)
        return record


def installed_factory():
    """The RecordFactory in effect, or None."""
    factory = logging.getLogRecordFactory()
    return factory if isinstance(factory, RecordFactory) else None


def top_setup():
    """The LogSetup at the top of the active stack, or None."""
    factory = installed_factory()
    return factory.setups[-1] if factory is not None and factory.setups else None


def _current_setup():
    return next((setup for setup in reversed(CONTEXT_SETUPS.get()) if not setup._closed), None)


def _record_setups(factory):
    current = _current_setup()
    return (current,) if current is not None else tuple(setup for setup in factory.setups if not setup._closed)


def scrub_record(record, factory):
    setups = _record_setups(factory)
    if setups:
        try:
            if record.exc_info and not record.exc_text:
                record.exc_text = logging.Formatter().formatException(record.exc_info)
            for setup in factory.setups:
                if not setup._closed and setup.redact is not None:
                    record.msg, record.args = setup.clean(record.getMessage()), ()
                    if record.exc_text:
                        record.exc_text = setup.clean(record.exc_text)
                    if record.stack_info:
                        record.stack_info = setup.clean(record.stack_info)
            for setup in setups:
                setup.scrub(record)
        except Exception:
            record.msg, record.args, record.exc_info, record.exc_text = \
                "(a log line that could not be redacted)", (), None, None
            record.streamwright_scrubbed = True
