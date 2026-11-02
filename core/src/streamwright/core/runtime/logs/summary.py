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

"""The run summary file (--summary FILE)."""


import json
import os
import tempfile

LOG_SUMMARY = "-"  # a bare --summary: the summary as a log line


def log_summary(logger, summary):
    """The run summary as one INFO log line of compact JSON (event `run_summary`), on the command's log handlers."""
    logger.info("run summary: %s", json.dumps(summary, separators=(",", ":"), default=str),
                extra={"event": "run_summary"})


def summary_problem(path):
    """Why --summary FILE cannot be written (checked before a run starts), or None."""
    if not path or path == LOG_SUMMARY:
        return None
    if os.path.isdir(path):
        return "--summary %s is a folder" % path
    folder = os.path.dirname(os.path.abspath(path))
    if not os.path.isdir(folder):
        return "--summary %s: the folder %s does not exist" % (path, folder)
    return None


def write_summary(path, summary):
    """Writes the run summary as JSON to `path`, atomically (a temporary file in its folder, then renamed)."""
    folder = os.path.dirname(os.path.abspath(path))
    descriptor, temporary = tempfile.mkstemp(prefix="." + os.path.basename(path) + ".", suffix=".part", dir=folder)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(summary, stream, indent=2, default=str)
            stream.write("\n")
        os.replace(temporary, path)
    except BaseException:
        if os.path.exists(temporary):
            os.remove(temporary)
        raise
