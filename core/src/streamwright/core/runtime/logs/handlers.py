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

"""Loggers, handlers and filters as a LogSetup configures and restores them."""


import contextlib
import logging
import logging.config

from streamwright.core.runtime.logs.record_factory import scrub_record


def all_loggers():
    """The root logger and every logger made so far."""
    return [logging.getLogger()] + [logger for logger in list(logging.Logger.manager.loggerDict.values())
                                    if isinstance(logger, logging.Logger)]


def logger_settings():
    """Every logger's settings: level, handlers, filters, propagate, disabled."""
    return {logger: {"level": logger.level, "handlers": list(logger.handlers), "filters": list(logger.filters),
                          "propagate": logger.propagate, "disabled": logger.disabled} for logger in all_loggers()}


def restored_items(before, configured, current):
    """
    The handlers (or filters) to give back to a logger: the ones it had before, minus the ones the setup added, plus
    the ones added while the command ran (by libraries).
    """
    def among(item, items):
        return any(item is other for other in items)
    return [item for item in current if among(item, before) or not among(item, configured)] + \
        [item for item in before if not among(item, configured) and not among(item, current)]


def apply_dict_config(config):
    """
    Applies a logging.config.dictConfig mapping and returns the handlers it made (also those no logger uses), which
    the setup closes when it ends. The handlers that exist stay open: a full (non-incremental) configuration closes
    every handler (logging.config._clearExistingHandlers), and a closed handler that is put back on its logger
    afterwards loses lines (a FileHandler opened with mode 'w' drops them, a MemoryHandler its target).
    """
    configurator = logging.config.dictConfigClass(config)
    clear = getattr(logging.config, "_clearExistingHandlers", None)
    if clear is not None:
        logging.config._clearExistingHandlers = lambda: None
    try:
        configurator.configure()
    except BaseException:
        for handler in _made_handlers(configurator):
            close_handler(handler)
        raise
    finally:
        if clear is not None:
            logging.config._clearExistingHandlers = clear
    return _made_handlers(configurator)


def _made_handlers(configurator):
    """The handlers a configurator made (configure() puts each in place of its configuration)."""
    handlers = configurator.config.get("handlers") or {}
    return [handler for handler in dict.values(handlers) if isinstance(handler, logging.Handler)]


def close_handler(handler):
    with contextlib.suppress(Exception):  # (closing a file the config opened)
        handler.close()


def all_handlers():
    """The handlers of every logger, each once."""
    found = []
    for logger in all_loggers():
        found += [handler for handler in logger.handlers if not any(handler is other for other in found)]
    return found


class ScrubFilter(logging.Filter):
    """A handler filter: redacts and truncates the lines that were not made while the LogSetup was in effect."""

    def __init__(self, setup):
        super().__init__()
        self.setup = setup

    def filter(self, record):
        if not getattr(record, "streamwright_scrubbed", False):
            scrub_record(record, self.setup.records)
        return True


class ToRootHandler(logging.Handler):
    """
    logging.lastResort while a LogSetup is in effect: a line that reaches no handler, because its logger is below one
    that does not propagate (google-api-core makes the `google` logger so), goes to the root logger's handlers, as if
    it had propagated: `--log google.ads.googleads.client=DEBUG` shows the client's lines.
    """

    def __init__(self, fallback):
        super().__init__(logging.NOTSET)
        self.fallback = fallback

    def handle(self, record):
        handlers = logging.getLogger().handlers or ([self.fallback] if self.fallback is not None else [])
        for handler in handlers:
            if record.levelno >= handler.level:
                handler.handle(record)
        return True

    def emit(self, record):  # (handle() passes the lines on)
        pass
