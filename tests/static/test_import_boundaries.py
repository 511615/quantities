from __future__ import annotations

from pathlib import Path


def test_main_quant_platform_tree_does_not_import_legacy_src_train() -> None:
    root = Path("src/quant_platform")
    offenders: list[str] = []
    for path in root.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        if "from src.train" in text or "import src.train" in text:
            offenders.append(str(path))
    assert offenders == []
