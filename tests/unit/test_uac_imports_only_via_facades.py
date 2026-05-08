"""Cat B.4 — UAC import discipline: deployment-api uses only facade imports.

Plan: ``data_status_comprehensive_test_coverage_2026_05_07.plan.md`` Phase 1.

Per the workspace Citadel UAC import rules + ``SUB_AGENT_MANDATORY_RULES.md``
§ 3b:

* Allowed: ``from unified_api_contracts import X`` and
  ``from unified_api_contracts.{domain} import X`` where ``{domain}`` is
  a public sub-package facade (``sports``, ``features``, ``internal``,
  ``registry``, ``market``, ``execution``, etc.).
* Banned: ``from unified_api_contracts.canonical.*`` and
  ``from unified_api_contracts.normalize_utils.*`` — those are
  UAC-internal implementation detail. Reaching into them couples
  consumers to refactor-fragile depths.

This test AST-walks every ``deployment_api/services/data_status_*.py``
module + ``deploy_missing.py`` and asserts no banned-prefix imports
appear. Catches a deep-import slipping past code review.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

# Repo root (where the test file lives is .../deployment-api/tests/unit/)
_REPO_ROOT = Path(__file__).resolve().parents[2]
_SERVICES_DIR = _REPO_ROOT / "deployment_api" / "services"

# Banned prefixes — these sub-packages are UAC-internal implementation
# detail and consumers must use the public facades instead.
_BANNED_PREFIXES: tuple[str, ...] = (
    "unified_api_contracts.canonical",
    "unified_api_contracts.normalize_utils",
)

# Files in scope for this test — data_status pipeline + deploy_missing.
# The full repo audit is the responsibility of a workspace-wide gate;
# this test pins the data-status surface.
_FILES_TO_AUDIT: tuple[str, ...] = (
    "data_status_service.py",
    "data_status_drilldown.py",
    "data_status_hierarchical.py",
    "data_status_mock.py",
    "deploy_missing.py",
)


def _collect_uac_imports(source: str) -> list[tuple[str, int]]:
    """Walk the AST and return every ``from unified_api_contracts...
    import X`` module path + line number. Plain
    ``import unified_api_contracts`` would also trigger if found, but
    every current consumer uses the from-import form."""
    tree = ast.parse(source)
    imports: list[tuple[str, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            if node.module == "unified_api_contracts" or node.module.startswith(
                "unified_api_contracts."
            ):
                imports.append((node.module, node.lineno))
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "unified_api_contracts" or alias.name.startswith(
                    "unified_api_contracts."
                ):
                    imports.append((alias.name, node.lineno))
    return imports


@pytest.mark.parametrize("filename", _FILES_TO_AUDIT)
def test_no_banned_uac_internal_imports(filename: str) -> None:
    """Each data-status module must not import from
    ``unified_api_contracts.canonical.*`` or
    ``unified_api_contracts.normalize_utils.*``."""
    path = _SERVICES_DIR / filename
    if not path.exists():
        pytest.skip(f"file {path} not present in this checkout")

    source = path.read_text(encoding="utf-8")
    imports = _collect_uac_imports(source)
    violations = [
        (mod, lineno)
        for mod, lineno in imports
        if any(mod.startswith(prefix) for prefix in _BANNED_PREFIXES)
    ]
    assert not violations, (
        f"{filename} imports UAC-internal modules — replace with public facades:\n"
        + "\n".join(f"  line {lineno}: from {mod} ..." for mod, lineno in violations)
    )


def test_at_least_one_facade_import_present_when_uac_used() -> None:
    """Sanity check the test itself — at least one of the audited files
    actually imports from UAC, so the banned-prefix scan isn't trivially
    passing on a no-import surface."""
    found_facade = False
    for filename in _FILES_TO_AUDIT:
        path = _SERVICES_DIR / filename
        if not path.exists():
            continue
        for module, _lineno in _collect_uac_imports(path.read_text(encoding="utf-8")):
            if module == "unified_api_contracts" or (
                module.startswith("unified_api_contracts.")
                and not any(module.startswith(p) for p in _BANNED_PREFIXES)
            ):
                found_facade = True
                break
        if found_facade:
            break
    assert found_facade, (
        "Sanity-check failure: no UAC facade imports found across audited files. "
        "Either the file list is wrong or the imports moved — investigate before trusting "
        "the banned-prefix test results."
    )
