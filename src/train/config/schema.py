"""Typed experiment config schema."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict


@dataclass(slots=True)
class ExperimentConfig:
    experiment: Dict[str, Any]
    data: Dict[str, Any]
    dataset: Dict[str, Any]
    model: Dict[str, Any]
    objective: Dict[str, Any]
    trainer: Dict[str, Any]
    eval: Dict[str, Any]
    tracking: Dict[str, Any]
    search: Dict[str, Any]
    runtime: Dict[str, Any]
    extras: Dict[str, Any] = field(default_factory=dict)
