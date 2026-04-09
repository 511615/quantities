from __future__ import annotations

from pathlib import Path


def test_mainline_tests_do_not_depend_on_legacy_src_train_imports() -> None:
    root = Path("tests")
    offenders: list[str] = []
    for path in root.rglob("test_*.py"):
        if "compatibility" in path.parts or "static" in path.parts:
            continue
        text = path.read_text(encoding="utf-8")
        if "from src.train" in text or "import src.train" in text:
            offenders.append(str(path))
    assert offenders == []
