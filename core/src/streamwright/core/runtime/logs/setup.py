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

"""LogSetup: the logging of one streamwright command."""


import logging
import sys
from types import MappingProxyType

from streamwright.core.runtime.logs.formatting import JsonFormatter
from streamwright.core.runtime.logs.handlers import (
    ScrubFilter,
    ToRootHandler,
    all_handlers,
    all_loggers,
    apply_dict_config,
    close_handler,
    logger_settings,
    restored_items,
)
from streamwright.core.runtime.logs.lines import DEFAULT_MAX_CHARS, NETWORK, TEXT_FORMAT, level_number
from streamwright.core.runtime.logs.record_factory import CONTEXT_SETUPS, RecordFactory, installed_factory
from streamwright.core.runtime.logs.redaction import mask_text

_NEW_LOGGER = MappingProxyType({"level": logging.NOTSET, "handlers": (), "filters": (), "propagate": True,
                                "disabled": False})


class LogSetup:
    """
    The logging of one streamwright command, on Python's standard model (named loggers, standard levels):

    - handlers: streamwright's own, on the root logger, writing `log_format` (text or json) lines to stderr (or
      `stream`); or, with `config` (a logging.config.dictConfig mapping: --log-config), the config's handlers instead.
      The handlers that exist stay open (dictConfig would close them all), and existing loggers stay enabled unless the
      config says `disable_existing_loggers: true` - except the `streamwright` loggers and the ones `loggers` names,
      which always log.
    - levels: `level` for the `streamwright` loggers (--log-level; default INFO, or the config's);
      `streamwright.network` WARNING (quiet) unless the config sets it, or `level` when that is higher; then `loggers`,
      (name, level) pairs (--log NAME=LEVEL, in order; `root` is the root logger). The root logger is at WARNING when
      streamwright's handler is its first, as with logging.basicConfig.
    - redaction and truncation: every line of every logger, for every handler, is redacted - with `redact`, the run's
      Redactor once run() knows the secrets, then credentials masked by name (mask_text) - and its message cut to
      `max_chars` (0: never). Lines are scrubbed when they are made (a log record factory), and every handler gets a
      filter that scrubs lines made otherwise.
    - lines that reach no handler go to the root logger's handlers (ToRootHandler).

    close() puts logging back as it was - what the setup changed: levels, handlers (the ones the config made are
    closed), filters, propagation, disabled loggers - and leaves what libraries changed while the command ran.
    """

    def __init__(self, level=None, log_format="text", loggers=(), config=None, *, max_chars=DEFAULT_MAX_CHARS,
                 stream=None):
        self.max_chars = max_chars
        self.redact = None
        self.handler = None
        self.filter = ScrubFilter(self)
        self._before = logger_settings()  # every logger's settings before the setup ...
        self._configured = {}  # ... and after it
        self._made = []  # the config's handlers
        self._filtered = []
        self.records = installed_factory() or RecordFactory(logging.getLogRecordFactory())
        self._last_resort = logging.lastResort
        self._to_root = ToRootHandler(self._last_resort)
        self._closed = False
        self.records.setups.append(self)
        CONTEXT_SETUPS.set((*CONTEXT_SETUPS.get(), self))
        try:
            self._configure(level_number(level), log_format, loggers, config, stream)
        except BaseException:
            self._configured = logger_settings()
            self.close()
            raise
        self._configured = logger_settings()

    def _configure(self, level, log_format, loggers, config, stream):
        root = logging.getLogger()
        if config is not None:
            self._made = apply_dict_config(dict({"disable_existing_loggers": False}, **config))
        else:
            self.handler = logging.StreamHandler(sys.stderr if stream is None else stream)
            self.handler.setFormatter(JsonFormatter(self) if log_format == "json" else logging.Formatter(TEXT_FORMAT))
            if not root.handlers:  # as logging.basicConfig: other libraries log their warnings and errors
                root.setLevel(logging.WARNING)
            root.addHandler(self.handler)
        streamwright = logging.getLogger("streamwright")
        if level is not None or config is None or streamwright.level == logging.NOTSET:
            streamwright.setLevel(logging.INFO if level is None else level)
        if config is None or NETWORK.level == logging.NOTSET:
            NETWORK.setLevel(logging.WARNING)
        if level is not None and level > NETWORK.level:  # quieter than the network logger: that too
            NETWORK.setLevel(level)
        for name, value in loggers:
            (root if name == "root" else logging.getLogger(name)).setLevel(level_number(value))
        if config is not None:  # (`disable_existing_loggers: true` disables the loggers made before the config)
            for logger in all_loggers():
                if logger.name == "streamwright" or logger.name.startswith("streamwright.") or \
                        any(logger.name == name or (name == "root" and logger is root) for name, _ in loggers):
                    logger.disabled = False
        for handler in all_handlers():
            handler.addFilter(self.filter)
            self._filtered.append(handler)
        logging.lastResort = self._to_root
        logging.setLogRecordFactory(self.records)

    def clean(self, text):
        """The text redacted (the run's Redactor) and with credentials masked by name."""
        if self.redact is not None:
            text = self.redact(text)
        return mask_text(text)

    def message(self, text):
        """A message as log lines show it: clean, then cut to `max_chars`."""
        message = self.clean(text)
        if self.max_chars and len(message) > self.max_chars:
            message = "%s... [truncated %d chars]" % (message[:self.max_chars], len(message) - self.max_chars)
        return message

    def scrub(self, record):
        """Redacts a log record's message, traceback and stack, and truncates its message (once)."""
        try:
            message = record.getMessage()
        except Exception:  # arguments that do not fit the message: both are shown, redacted
            message = "%s %r" % (record.msg, record.args)
        record.msg, record.args = self.message(message), ()
        if record.exc_info and not record.exc_text:
            # formatters reuse exc_text, so the redacted traceback is what every handler writes
            record.exc_text = logging.Formatter().formatException(record.exc_info)
        if record.exc_text:
            record.exc_text = self.clean(record.exc_text)
        if record.stack_info:
            record.stack_info = self.clean(record.stack_info)
        record.streamwright_scrubbed = True

    def close(self):
        if self._closed:
            return
        self._closed = True
        CONTEXT_SETUPS.set(tuple(setup for setup in CONTEXT_SETUPS.get() if setup is not self))
        setups = self.records.setups
        while setups and setups[-1]._closed:
            setups.pop()._restore()

    def _restore(self):
        if not self.records.setups and logging.getLogRecordFactory() is self.records:
            logging.setLogRecordFactory(self.records.original)
        if logging.lastResort is self._to_root:
            logging.lastResort = self._last_resort
        for handler in self._filtered:
            handler.removeFilter(self.filter)
        current = logger_settings()
        for logger, configured in self._configured.items():
            before, now = self._before.get(logger, _NEW_LOGGER), current.get(logger)
            if now is None:
                continue
            if now["level"] == configured["level"]:  # (else a library set it while the command ran: it stays)
                logger.setLevel(before["level"])
            for name in ("propagate", "disabled"):
                if now[name] == configured[name]:
                    setattr(logger, name, before[name])
            logger.handlers = restored_items(before["handlers"], configured["handlers"], now["handlers"])
            logger.filters = restored_items(before["filters"], configured["filters"], now["filters"])
        for handler in self._made:
            close_handler(handler)
        for handler in [self.handler, *self._made]:
            try:
                if handler is not None:
                    handler.flush()
            except (OSError, ValueError):
                pass
        self._filtered, self._made = [], []
