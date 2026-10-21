"""A tiny test runner, for environments without pytest.

The test files are written as plain ``test_*`` functions, so they run under
pytest. This runner lets them also run with only the standard library:

    python tests/run_tests.py

It imports each test module, calls every ``test_*`` function, and reports the
results, exiting non-zero if any test fails. Using pytest is recommended where
it is available (``pytest``); this runner is the portable fallback.
"""

from __future__ import annotations

import importlib
import sys
import traceback
from pathlib import Path


def main() -> int:
    """Discover and run every test function, returning an exit code.

    Returns:
        0 if all tests passed, 1 otherwise.
    """
    # Make the project root importable so ``import umf`` and ``import tests.*``
    # work when this runner is launched directly.
    root = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(root))

    test_modules = [
        "tests.test_core_and_typing",
        "tests.test_engine_and_recon",
        "tests.test_control_runner_ui",
    ]

    passed = 0
    failed = 0
    failures = []

    for module_name in test_modules:
        module = importlib.import_module(module_name)
        for name in sorted(dir(module)):
            if not name.startswith("test_"):
                continue
            func = getattr(module, name)
            if not callable(func):
                continue
            try:
                func()
                passed += 1
            except Exception as exc:  # collect, do not stop
                failed += 1
                failures.append((f"{module_name}.{name}", exc, traceback.format_exc()))

    print(f"\n{passed} passed, {failed} failed")
    for qualified_name, exc, tb in failures:
        print(f"\nFAILED {qualified_name}: {exc}")
        print(tb)

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
