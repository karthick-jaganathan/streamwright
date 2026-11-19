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
"""`streamwright systemd install`: the packaged units, with this installation's paths, and their environment files."""

import io
import os
import stat

from streamwright.core import cli
from streamwright.core.serve import units

PROGRAM = "/opt/streamwright/venv/bin/streamwright"


def test_the_units_name_this_installation_and_its_config_folder(tmp_path):
    unit_dir, config_dir = tmp_path / "system", tmp_path / "etc" / "sw"
    unit_dir.mkdir()
    done = units.install_units(str(unit_dir), str(config_dir), PROGRAM, out=io.StringIO())
    assert sorted(os.listdir(unit_dir)) == sorted(units.UNITS)
    worker = (unit_dir / "streamwright-worker.service").read_text()
    assert "ExecStart=%s worker %s/serve.yaml --processes 8 --log-format json" % (PROGRAM, config_dir) in worker
    assert "EnvironmentFile=%s/worker.env" % config_dir in worker and "@" not in worker
    assert "ExecStart=%s queue purge %s/serve.yaml" % (PROGRAM, config_dir) in (
        unit_dir / "streamwright-purge.service").read_text()
    for name in units.ENV_FILES:                  # created for the secrets, readable by root only
        path = config_dir / name
        assert stat.S_IMODE(path.stat().st_mode) == 0o600 and done[str(path)].startswith("created")
        assert all(line.startswith("#") for line in path.read_text().splitlines() if line)


def test_installed_files_are_kept_unless_forced_and_secrets_never_replaced(tmp_path):
    unit_dir, config_dir = tmp_path / "system", tmp_path / "sw"
    unit_dir.mkdir()
    units.install_units(str(unit_dir), str(config_dir), PROGRAM, out=io.StringIO())
    serve, secrets = unit_dir / "streamwright-serve.service", config_dir / "common.env"
    serve.write_text(serve.read_text().replace("RestartSec=5", "RestartSec=9"))
    secrets.write_text("STREAMWRIGHT_SECRET_STATE_PASSWORD=pw\n")
    again = units.install_units(str(unit_dir), str(config_dir), PROGRAM, out=io.StringIO())
    assert again[str(serve)].startswith("kept") and again[str(unit_dir / "streamwright-purge.timer")] == "unchanged"
    assert "RestartSec=9" in serve.read_text()
    forced = units.install_units(str(unit_dir), str(config_dir), PROGRAM, force=True, out=io.StringIO())
    assert forced[str(serve)] == "written" and "RestartSec=5" in serve.read_text()
    assert forced[str(secrets)] == "kept" and secrets.read_text() == "STREAMWRIGHT_SECRET_STATE_PASSWORD=pw\n"


def test_the_command_prints_a_dry_run_and_needs_a_systemd_folder(tmp_path, capsys):
    unit_dir = tmp_path / "system"
    unit_dir.mkdir()
    assert cli.main(["systemd", "install", "--unit-dir", str(unit_dir), "--config-dir", "/etc/sw", "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "# %s/streamwright-serve.service" % unit_dir in out and "serve /etc/sw/serve.yaml" in out
    assert os.listdir(unit_dir) == []
    assert cli.main(["systemd", "install", "--unit-dir", str(tmp_path / "missing"),
                     "--config-dir", str(tmp_path / "sw")]) == 1
    assert "is not a folder" in capsys.readouterr().err
    assert cli.main(["systemd", "install", "--unit-dir", str(unit_dir), "--config-dir", str(tmp_path / "sw")]) == 0
    assert "systemctl enable --now streamwright-serve" in capsys.readouterr().out
    assert units.executable()                     # this test environment's own `streamwright`
