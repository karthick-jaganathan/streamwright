"""Job-kind and command plug-ins are discovered from metadata, not imported by the core CLI."""

import argparse
import importlib
import subprocess
import sys
from types import SimpleNamespace

import pytest

from streamwright.core import cli
from streamwright.core.cli.jobs import parse_set
from streamwright.core.cli.parser import build_parser, build_validate_parser
from streamwright.core.jobs.duckdb import cli as duckdb_cli
from streamwright.core.runtime import plugins


def entry(name, group, callback):
    calls = []

    def load():
        calls.append(name)
        return callback

    ep = SimpleNamespace(
        name=name, group=group, dist=SimpleNamespace(name="streamwright-demo", version="0.1",
                                                      metadata={"StreamWright-SDK-Version": "1.0"}),
        load=load,
    )
    return ep, calls


def test_module_help_and_public_parser_api():
    result = subprocess.run([sys.executable, "-m", "streamwright.core.cli", "--help"],
                            capture_output=True, text=True, check=False)
    assert result.returncode == 0
    assert "run" in result.stdout and "duckdb" in result.stdout
    assert {"run", "validate", "plan", "job"} <= set(cli.command_parsers())


class FakeEngine:
    """A JobEngine of kind `kind` that records the arguments of main()."""

    def __init__(self, kind, received):
        self.kind, self.received = kind, received

    def main(self, argv=None):
        self.received.append(argv)
        return 0

    def validate_job(self, job, *, overlays=(), config=None, strict=False):
        raise NotImplementedError

    def validate_job_text(self, text, *, overlays=(), config=None, strict=False):
        raise NotImplementedError

    def json_schema(self):
        return {}

    def guide(self, section=None):
        return ""

    def describe_connector(self, name):
        return None


def test_job_kind_routes_by_manifest_without_engine_import(tmp_path, monkeypatch):
    manifest = tmp_path / "job.yaml"
    manifest.write_text("kind: NewJob\n")
    received = []
    ep, calls = entry("NewJob", plugins.JOB_KINDS, FakeEngine("NewJob", received))
    monkeypatch.setattr(plugins, "entry_point_source", lambda: {plugins.JOB_KINDS: [ep]})
    for verb in ("run", "plan", "validate"):
        assert cli.main([verb, "--set", "x=1", str(manifest)]) == 0
        assert received[-1] == [verb, "--set", "x=1", str(manifest)]
    assert len(calls) == 3
    assert cli.detect_kind(str(manifest)) == "NewJob"


def test_job_option_routes_like_the_job_path(tmp_path, capsys, monkeypatch):
    manifest = tmp_path / "job.yaml"
    manifest.write_text("kind: NewJob\n")
    received = []
    ep, _ = entry("NewJob", plugins.JOB_KINDS, FakeEngine("NewJob", received))
    monkeypatch.setattr(plugins, "entry_point_source", lambda: {plugins.JOB_KINDS: [ep]})
    for argv in (["run", "--job", str(manifest)], ["validate", "-j", str(manifest)],
                 ["plan", "--job=%s" % manifest], ["run", str(manifest), "--job", str(manifest)]):
        assert cli.main(argv) == 0 and received[-1] == argv
    for argv in (["run", "--summary", "--job", str(manifest)], ["run", "--job", str(manifest), "--summary"]):
        assert cli.main(argv) == 0 and received[-1] == argv  # a bare --summary takes no path
    source = tmp_path / "source.yaml"
    source.write_text("kind: source\n")
    assert cli.main(["run", "--job", str(source)]) == 2
    assert cli.main(["run"]) == 2
    error = capsys.readouterr().err
    assert "is not a job manifest" in error and "run needs a SOURCE, or --job JOB" in error


def test_a_job_kind_must_target_a_job_engine_of_that_kind(tmp_path, capsys, monkeypatch):
    manifest = tmp_path / "job.yaml"
    manifest.write_text("kind: NewJob\n")
    for target in (lambda argv: 0, FakeEngine("OtherJob", [])):
        ep, _ = entry("NewJob", plugins.JOB_KINDS, target)
        monkeypatch.setattr(plugins, "entry_point_source", lambda ep=ep: {plugins.JOB_KINDS: [ep]})
        assert cli.main(["validate", str(manifest)]) == 2
    error = capsys.readouterr().err
    assert "invalid type" in error and "targets an engine of kind 'OtherJob'" in error


def test_uninstalled_job_kind_fails_with_install_hint(tmp_path, capsys, monkeypatch):
    manifest = tmp_path / "job.yaml"
    manifest.write_text("kind: MissingJob\n")
    monkeypatch.setattr(plugins, "entry_point_source", dict)
    assert cli.main(["run", str(manifest)]) == 2
    assert "pip install streamwright-MissingJob" in capsys.readouterr().err


def test_command_entry_is_registered_from_metadata(monkeypatch, capsys):
    def register(parsers):
        parser = parsers.add_parser("example")
        parser.set_defaults(command_handler=lambda args: print("example invoked") or 0)

    ep, calls = entry("example", plugins.COMMANDS, register)
    monkeypatch.setattr(plugins, "entry_point_source", lambda: {plugins.COMMANDS: [ep]})
    assert cli.main(["example"]) == 0
    assert calls == ["example"]
    assert "example invoked" in capsys.readouterr().out


def test_builtin_job_shorthand_uses_plugin_route(monkeypatch):
    jobs_module = importlib.import_module("streamwright.core.cli.commands.jobs")
    calls = []
    monkeypatch.setattr(jobs_module, "run_job_kind",
                        lambda kind, argv, prog=None: calls.append((kind, argv, prog)) or 0)
    assert cli.main(["duckdb", "run", "job.yaml"]) == 0
    assert calls == [("DuckDBJob", ["run", "job.yaml"], "streamwright duckdb")]  # its usage lines name the command


def test_shared_job_set_parser_uses_jobs_input_rules():
    assert parse_set("tenant = north") == ("tenant", "north")
    try:
        parse_set("missing")
    except argparse.ArgumentTypeError as exc:
        assert "NAME=VALUE" in str(exc)
    else:
        raise AssertionError("an invalid --set must be rejected")


def _without_help(parser, path):
    missing = []
    for action in parser._actions:
        if isinstance(action, argparse._HelpAction) or action.help == argparse.SUPPRESS:
            continue
        if isinstance(action, argparse._SubParsersAction):
            helps = {choice.dest: choice.help for choice in action._choices_actions}
            for name, sub in action.choices.items():
                missing += [] if helps.get(name) else ["%s %s" % (path, name)]
                missing += _without_help(sub, "%s %s" % (path, name))
        elif not action.help and action.nargs != argparse.REMAINDER:  # (REMAINDER: another parser's arguments)
            missing.append("%s %s" % (path, "/".join(action.option_strings) or action.dest))
    return missing


def test_every_command_and_option_has_help(capsys):
    plugin_commands = {item["name"] for item in plugins.discover(plugins.COMMANDS)} - {"duckdb"}
    missing = [item for item in _without_help(build_parser(), "streamwright")
               if item.split()[1] not in plugin_commands]  # (installed plug-ins check their own)
    assert missing == []
    assert _without_help(build_validate_parser(), "streamwright validate") == []
    assert _without_help(duckdb_cli._build_parser("streamwright duckdb"), "streamwright duckdb") == []
    assert cli.main(["plan", "-h"]) == 0
    with pytest.raises(SystemExit) as shown:
        cli.main(["duckdb", "-h"])  # forwarded to the engine's own parser
    assert shown.value.code == 0
    out = capsys.readouterr().out
    assert "usage: streamwright plan" in out and "usage: streamwright duckdb" in out
