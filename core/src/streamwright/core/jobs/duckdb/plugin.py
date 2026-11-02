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

"""The DuckDBJob `JobEngine` registered in `streamwright.job_kinds` (`DuckDBJob = ...plugin:JOB_ENGINE`)."""

from importlib.resources import files

from streamwright.core.jobs.duckdb import cli, engine
from streamwright.core.jobs.duckdb.loading import load_adapters
from streamwright.core.jobs.duckdb.rules import KIND

__all__ = ["JOB_ENGINE", "DuckDBJobEngine"]


class DuckDBJobEngine:
    """The `streamwright.core.jobs.engine.JobEngine` of `kind: DuckDBJob`."""

    kind = KIND

    def main(self, argv=None, prog="streamwright"):
        return cli.main(argv, prog=prog)

    def validate_job(self, job, *, overlays=(), config=None, strict=False, allowed=None):
        return engine.validate_job(job, overlays=overlays, config=config, strict=strict, allowed=allowed)

    def validate_job_text(self, text, *, overlays=(), config=None, strict=False, allowed=None):
        return engine.validate_job_text(text, overlays=overlays, config=config, strict=strict, allowed=allowed)

    def json_schema(self):
        return engine.json_schema()

    def guide(self, section=None):
        text = files("streamwright.core.jobs.duckdb").joinpath("guide.md").read_text(encoding="utf-8")
        if section is None:
            return text
        title = str(section).strip().lower()
        for block in text.split("\n## ")[1:]:
            if block.partition("\n")[0].strip().lower() == title:
                return "## " + block.rstrip("\n") + "\n"
        raise KeyError("no DuckDBJob guide section %r" % section)

    def describe_connector(self, name):
        adapters, _ = load_adapters([name])
        return adapters[name].describe() if name in adapters else None


JOB_ENGINE = DuckDBJobEngine()
