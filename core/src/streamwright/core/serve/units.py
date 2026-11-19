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
`streamwright systemd install`: puts the systemd units of a service - serve, worker, and a daily `queue purge` -
where systemd reads them, with this installation's `streamwright` in ExecStart; and, when they are missing, the
environment files they read (mode 0600: they hold secrets). pip never writes outside its environment, so this is the
installation's step that does, as root. Units that exist are kept unless --force; environment files are never
replaced.
"""

import importlib.resources
import os
import shutil
import sys

from streamwright.core.cli.help_format import DescriptionFormatter

__all__ = ["UNITS", "install_units", "register_systemd"]

SUCCESS, FAILURE = 0, 1
UNITS = ("streamwright-serve.service", "streamwright-worker.service", "streamwright-purge.service",
         "streamwright-purge.timer")
ENV_FILES = ("common.env", "worker.env")
DEFAULT_UNIT_DIR = "/etc/systemd/system"
DEFAULT_CONFIG_DIR = "/etc/streamwright"
_DESCRIPTION = """\
Install the systemd units of `streamwright serve`: streamwright-serve.service, streamwright-worker.service and
streamwright-purge.service with its daily timer, into --unit-dir, with this installation's `streamwright` in
ExecStart and --config-dir (serve.yaml, common.env, worker.env) in their paths. The environment files are created
(mode 0600) when missing, never replaced. Run it as root:

  sudo /opt/streamwright/venv/bin/streamwright systemd install
  sudo systemctl daemon-reload
  sudo systemctl enable --now streamwright-serve streamwright-worker streamwright-purge.timer"""


def register_systemd(subparsers):
    parser = subparsers.add_parser("systemd", help="install the systemd units of streamwright serve and worker",
                                   description=_DESCRIPTION, formatter_class=DescriptionFormatter)
    actions = parser.add_subparsers(dest="systemd_action", metavar="ACTION", required=True)
    install = actions.add_parser("install", help="write the units (and missing environment files)",
                                 description=_DESCRIPTION, formatter_class=DescriptionFormatter)
    install.add_argument("--unit-dir", default=DEFAULT_UNIT_DIR, metavar="DIR",
                         help="where systemd reads the units (default %s)" % DEFAULT_UNIT_DIR)
    install.add_argument("--config-dir", default=DEFAULT_CONFIG_DIR, metavar="DIR",
                         help="the folder of serve.yaml and the environment files (default %s)" % DEFAULT_CONFIG_DIR)
    install.add_argument("--force", action="store_true", help="replace units that exist and differ")
    install.add_argument("--dry-run", action="store_true", help="print the units; write nothing")
    install.set_defaults(command_handler=cmd_systemd)


def executable():
    """This installation's `streamwright` (beside its Python, e.g. a virtualenv's bin), or the one on PATH."""
    beside = os.path.join(os.path.dirname(sys.executable), "streamwright")
    return beside if os.access(beside, os.X_OK) else shutil.which("streamwright")


def _template(name):
    return importlib.resources.files("streamwright.core.serve").joinpath("systemd", name).read_text(encoding="utf-8")


def render(name, program, config_dir):
    """A unit (or environment file) with this installation's paths."""
    return _template(name).replace("@STREAMWRIGHT@", program).replace("@CONFIG_DIR@", config_dir.rstrip("/"))


def _write(path, text, mode):
    with open(path, "w", encoding="utf-8") as stream:
        stream.write(text)
    os.chmod(path, mode)


def install_units(unit_dir, config_dir, program, *, force=False, out=sys.stdout):
    """Writes the units and the missing environment files; returns {path: what was done}."""
    done = {}
    for name in UNITS:
        path, text = os.path.join(unit_dir, name), render(name, program, config_dir)
        if os.path.exists(path):
            with open(path, encoding="utf-8") as stream:
                same = stream.read() == text
            if same or not force:
                done[path] = "unchanged" if same else "kept (it differs; --force replaces it)"
                continue
        _write(path, text, 0o644)
        done[path] = "written"
    os.makedirs(config_dir, mode=0o755, exist_ok=True)
    for name in ENV_FILES:
        path = os.path.join(config_dir, name)
        if os.path.exists(path):
            done[path] = "kept"
        else:
            _write(path, render(name, program, config_dir), 0o600)
            done[path] = "created (mode 0600): fill it in"
    out.writelines("%-48s %s\n" % (path, what) for path, what in done.items())
    return done


def _next_steps(config_dir):
    return ("\nNext:\n"
            "  useradd --system --no-create-home --home-dir /var/lib/streamwright --shell /usr/sbin/nologin "
            "streamwright\n"
            "  put serve.yaml in %s and the secrets in its common.env and worker.env\n"
            "  systemctl daemon-reload\n"
            "  systemctl enable --now streamwright-serve streamwright-worker streamwright-purge.timer\n" % config_dir)


def cmd_systemd(args):
    program = executable()
    if program is None:
        sys.stderr.write("streamwright systemd install: cannot find this installation's `streamwright` program\n")
        return FAILURE
    config_dir = os.path.abspath(args.config_dir)
    if args.dry_run:
        for name in UNITS:
            sys.stdout.write("# %s\n%s\n" % (os.path.join(args.unit_dir, name), render(name, program, config_dir)))
        return SUCCESS
    if not os.path.isdir(args.unit_dir):
        sys.stderr.write("streamwright systemd install: %s is not a folder (is this a systemd host?)\n" % args.unit_dir)
        return FAILURE
    try:
        install_units(args.unit_dir, config_dir, program, force=args.force)
    except PermissionError as exc:
        sys.stderr.write("streamwright systemd install: %s: run it as root (sudo %s systemd install)\n" % (
            exc, program))
        return FAILURE
    sys.stdout.write(_next_steps(config_dir))
    return SUCCESS
