"""
`streamwright run`: preparation, execution and reporting of a source run (RunCommand), and the run with its
metrics, summary and output handling (run).
"""

import datetime
import json
import logging
import os
import zoneinfo

import yaml

from streamwright.core.config import loader
from streamwright.core.config.inputs import (
    InputError,
    read_config_file,
    read_values_file,
    resolve_inputs,
    secrets_from_env,
)
from streamwright.core.engine import sql
from streamwright.core.engine.runner import SourceRunner
from streamwright.core.net.http import Redactor
from streamwright.core.outputs.output import FILE_FORMATS, FILE_NAME_OUTPUTS, export_files, open_output
from streamwright.core.runtime import components, logs
from streamwright.core.validation import engine, schema

LOG = logging.getLogger("streamwright.source")


class RunCommand:
    def __init__(self, args, fail):
        self.args = args
        self.fail = fail

    def prepare(self):
        """Validate and compile once before any output is opened or request is made."""
        try:
            layout = loader.find_source(self.args.source)
        except loader.SourceFilesError as exc:
            self.fail("%s", exc)
            return None
        name = layout.stream_of(self.args.source)
        if os.path.isfile(self.args.source) and name is not None:
            self.fail("%s is a stream of the source folder %s; run the folder: streamwright run %s --stream %s",
                      self.args.source, layout.folder, layout.folder, name)
            return None
        issues = engine.validate_source(layout, kind=schema.KIND,
                                        allowed_connectors=self.args.allowed_connectors)
        for issue in issues:
            if issue.severity == engine.ERROR:
                self.fail("%s", issue)
            else:
                LOG.warning("%s", issue)
        if any(issue.severity == engine.ERROR for issue in issues):
            self.fail("%s is not a valid source; fix the errors above (streamwright validate %s)",
                      self.args.source, self.args.source)
            return None
        source = loader.load_source(layout)
        if source.get("kind") != schema.KIND:
            self.fail("%s is not a `kind: source` file", self.args.source)
            return None
        plan = sql.plan_source(source)
        problems = components.check_source(source, self.args.allowed_connectors) + plan.messages()
        if problems:
            raise InputError("; ".join(problems))
        return source, plan

    def execute(self, source, config, secrets, *, state, output, today, redact, metrics, selected, plan):
        """Execute the already compiled plan; output commit is owned by the caller."""
        runner = SourceRunner(source, config, secrets, state=state, output=output, today=today, redact=redact,
                              allowed_connectors=self.args.allowed_connectors, metrics=metrics, plan=plan)
        runner.run(selected)

    def report(self, output):
        """Report completed output metadata, preserving the run's original error on failure."""
        try:
            return list(output.summary())
        except Exception as exc:
            LOG.warning("could not summarize the output: %s", exc)
            return []


def _discard(output):
    """Throws away a failed run's output; a failure here must not hide the run's own error."""
    try:
        output.close(failed=True)
    except Exception as exc:
        LOG.warning("could not discard the output: %s", exc)


def _load_state(path):
    if not path:
        return None
    if not os.path.exists(path):
        LOG.warning("state file %s does not exist; starting from each stream's `start`", path)
        return None
    with open(path, encoding="utf-8") as stream:
        state = json.load(stream)
    # accept a Singer STATE message as well as its value
    return state.get("value", state) if state.get("type") == "STATE" else state


def _summary_problem(path):
    """Why --summary FILE cannot be written (checked before the run starts), or None."""
    if not path:
        return None
    if os.path.isdir(path):
        return "--summary %s is a folder" % path
    folder = os.path.dirname(os.path.abspath(path))
    if not os.path.isdir(folder):
        return "--summary %s: the folder %s does not exist" % (path, folder)
    return None


def run(args, logging_setup=None):
    """
    streamwright run. Whatever the outcome, a run that started logs its end (RunMetrics.finish), and --summary FILE is
    written, with the error of a failed or interrupted run: the errors _run reports (_failed) - not log lines, which
    the loggers' levels and handlers can drop - redacted and cut as log lines are. `logging_setup`: the command's
    logs.LogSetup, which gets the run's Redactor and the network loggers of the source's connector.
    """
    problem = _summary_problem(getattr(args, "summary", None))
    if problem:
        LOG.error("%s", problem)
        return 2
    redact = Redactor()
    if logging_setup is not None:  # every log line from now on, as the secrets become known
        logging_setup.redact = redact
    metrics = logs.RunMetrics()
    errors = []  # the run's errors (_failed), for the summary
    code = None
    try:
        code = _run(args, metrics, redact, logging_setup, errors)
    except BaseException as exc:
        if metrics.status is None:  # it stopped before the run did
            metrics.status = "failed" if isinstance(exc, Exception) else "interrupted"
            if isinstance(exc, Exception):
                errors.append("%s: %s" % (type(exc).__name__, exc))
        raise
    finally:
        if getattr(args, "summary", None):
            shown = logging_setup.message if logging_setup is not None else \
                (lambda text: logs.mask_text(redact(text)))
            messages = []
            for error in errors:
                message = shown(redact(error))
                if message not in messages:
                    messages.append(message)
            metrics.error = "; ".join(messages) or None
            try:
                logs.write_summary(args.summary, metrics.summary())
            except (OSError, TypeError, ValueError) as exc:
                LOG.error("could not write the summary %s: %s", args.summary, exc)
                code = 1 if code == 0 else code
    return code


def _failed(errors, message, *args):
    """Logs an error of `streamwright run` and keeps it in `errors`, for the run summary."""
    LOG.error(message, *args)
    errors.append(message % args if args else message)


def _today_in_zone(started, timezone):
    """The client's local date (for `today` and relative dates/windows): --timezone (an IANA name) or the machine's."""
    try:
        zone = zoneinfo.ZoneInfo(timezone) if timezone else None
    except (zoneinfo.ZoneInfoNotFoundError, ValueError):
        raise InputError("unknown --timezone %r: use an IANA name such as America/New_York" % timezone) from None
    return started.astimezone(zone).date() if zone else datetime.date.today()


def _resolve_config_and_secrets(spec, provided_config, provided_secrets, today):
    """(config, secrets) resolved from the spec's declared inputs, reporting every section's problems together."""
    problems, values = [], {}
    for section, label, provided in (("config", "config", provided_config),
                                     ("secrets", "secret", provided_secrets)):
        try:
            values[section] = resolve_inputs(spec.get(section), provided, label, today=today,
                                             hide_values=section == "secrets")
        except InputError as exc:
            problems.append(str(exc))
    if problems:
        raise InputError("; ".join(problems))
    # secrets from files and Kubernetes mounts often end with a newline
    secrets = {k: v.strip() if isinstance(v, str) else v for k, v in values["secrets"].items()}
    return values["config"], secrets


def _selection(args, source):
    """(provided config, selected streams or exports): --config file values under --set, --stream or the file's."""
    provided_config, file_streams = read_config_file(args.config) if args.config else ({}, None)
    provided_config.update(dict(args.set))
    selected = args.streams or file_streams
    names = [s["name"] for s in source["streams"]]
    exports = [export for stream in source["streams"] for export in sql.exports_of(stream)]
    unknown = [name for name in selected or () if name not in names and name not in exports]
    if unknown:
        raise InputError("unknown stream(s): %s (streams/exports: %s)" % (
            ", ".join(unknown), ", ".join(sorted(set(names) | set(exports)))))
    return provided_config, selected


def _provided_secrets(args, spec, redact):
    provided = secrets_from_env(list((spec.get("secrets") or {}).keys()))
    if args.secrets:
        provided.update(read_values_file(args.secrets, warn=LOG.warning))
    for value in provided.values():
        redact.add(value)
    return provided


def _file_names(args, source, plan, selected, values):
    """
    {export: path} of the exports that run and are named, or None: the run's --file-name, else the export's
    `file_name`, else the source's `export.file_name`. Names apply to the file outputs only: --file-name with another
    output is an error, a `file_name` in the YAML is not used there.
    """
    kind, _, directory = (args.output or "").partition(":")
    if kind not in FILE_FORMATS or not directory:
        if args.file_name is not None:
            raise InputError(FILE_NAME_OUTPUTS)
        return None
    default = (source.get("export") or {}).get("file_name") if isinstance(source.get("export"), dict) else None
    # the selected streams run with their `from_stream` parents, and all of them write their exports
    running = plan.closure(selected or [s["name"] for s in source["streams"]])
    templates = {}
    for name in running:
        stream = plan.streams[name].stream
        for export in sql.exports_of(stream):
            definition = stream["export"][export] if isinstance(stream["export"][export], dict) else {}
            template = args.file_name or definition.get("file_name") or default
            if template:
                templates[export] = template
    return export_files(templates, dict(values, format=kind), directory) if templates else None


def _execute(command, args, run_inputs, *, state, output, metrics, errors):
    """Runs the prepared command into `output`; the exit code. Every failure is reported (redacted)."""
    source, config, secrets, today, redact, selected, plan = run_inputs
    closing = False
    try:
        if args.state is None:  # e.g. dlt keeps the state in the destination
            state = output.initial_state()
        command.execute(source, config, secrets, state=state, output=output, today=today, redact=redact,
                        metrics=metrics, selected=selected, plan=plan)
        closing = True  # closing writes the files or loads the data, and can fail too
        output.close()
        metrics.outputs = command.report(output)
        metrics.finish("ok")
        return 0
    except KeyboardInterrupt:
        _abandon(output, closing, command, metrics)
        _failed(errors, "interrupted")
        metrics.finish("interrupted")
        return 130
    except Exception as exc:  # report every failure (redacted) instead of a traceback with secrets
        _abandon(output, closing, command, metrics)
        _failed(errors, "%s", redact(exc))
        LOG.debug("details", exc_info=True)
        metrics.finish("failed")
        return 1
    except BaseException:
        _abandon(output, closing, command, metrics)
        metrics.finish("interrupted")
        raise


def _abandon(output, closing, command, metrics):
    """A run that failed: its output is discarded unless it was being closed; what it wrote is reported."""
    if not closing:
        _discard(output)
    metrics.outputs = command.report(output)


def _run(args, metrics, redact, logging_setup, errors):
    command = RunCommand(args, lambda message, *values: _failed(errors, message, *values))
    try:
        prepared = command.prepare()
    except (InputError, ValueError, yaml.YAMLError, OSError) as exc:
        _failed(errors, "%s", exc)
        return 2
    if prepared is None:
        return 2
    source, plan = prepared
    try:
        metrics.source = source.get("name")
        spec = source.get("spec") or {}
        provided_config, selected = _selection(args, source)
        provided_secrets = _provided_secrets(args, spec, redact)
        started = datetime.datetime.now(datetime.UTC)
        today = _today_in_zone(started, args.timezone)
        config, secrets = _resolve_config_and_secrets(spec, provided_config, provided_secrets, today)
        for value in secrets.values():
            redact.add(value)
        file_names = _file_names(args, source, plan, selected, {
            "source": source["name"], "today": today, "config": config,
            "timestamp": started.strftime("%Y%m%dT%H%M%SZ")})
        state = _load_state(args.state)
        output = open_output(args.output, source=source, file_names=file_names)
    except (InputError, ValueError, yaml.YAMLError, OSError) as exc:
        _failed(errors, "%s", exc)
        return 2
    return _execute(command, args, (source, config, secrets, today, redact, selected, plan), state=state, output=output,
                    metrics=metrics, errors=errors)
