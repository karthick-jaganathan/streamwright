"""The dependency-free AST gate catches both executable vendor literals and private imports."""

import importlib.util
from pathlib import Path


def test_contract_gate_distinguishes_sdk_imports_and_vendor_literals(tmp_path, monkeypatch):
    module_path = Path(__file__).resolve().parents[2] / "scripts/check_plugin_contract.py"
    spec = importlib.util.spec_from_file_location("plugin_contract_gate", module_path)
    checker = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(checker)
    core = tmp_path / "core/src/streamwright/core"
    engine = core / "engine"
    engine.mkdir(parents=True)
    connector = tmp_path / "connectors/readers/vendor_x"
    package = connector / "src/streamwright/connectors/vendor_x"
    package.mkdir(parents=True)
    (connector / "pyproject.toml").write_text("[project]\nname = 'vendor-x'\n")
    (engine / "runner.py").write_text('NAME = "vendor_x"\n')
    (package / "connector.py").write_text("from streamwright.core.runtime.components import Connector\n")
    allowlist = tmp_path / "scripts/plugin_contract_allowlist.txt"
    allowlist.parent.mkdir()
    allowlist.write_text("# none\n")
    monkeypatch.setattr(checker, "ROOT", tmp_path)
    monkeypatch.setattr(checker, "CORE", core)
    monkeypatch.setattr(checker, "CONNECTORS", tmp_path / "connectors")
    monkeypatch.setattr(checker, "SIBLINGS", (tmp_path,))
    monkeypatch.setattr(checker, "ALLOWLIST", allowlist)
    violations = checker.violations()
    assert len(violations) == 2
    assert any("G1" in problem and "vendor_x" in problem for problem in violations)
    assert any("G2" in problem and "runtime.components" in problem for problem in violations)
    (engine / "runner.py").write_text('"""Example vendor_x is documentation."""\nNAME = "generic"\n')
    (package / "connector.py").write_text("from streamwright.core.sdk import Connector\n")
    assert checker.violations() == []
