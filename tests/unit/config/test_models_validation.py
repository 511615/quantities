from __future__ import annotations

import pytest
from pydantic import ValidationError

from quant_platform.common.config.models import AppConfig
from tests.fixtures.config_fixtures import clone_payload


def test_app_config_rejects_missing_required_field() -> None:
    payload = clone_payload()
    del payload["project"]
    with pytest.raises(ValidationError):
        AppConfig.model_validate(payload)


def test_app_config_rejects_wrong_field_type() -> None:
    payload = clone_payload()
    payload["train"]["epochs"] = "not-an-int"
    with pytest.raises(ValidationError):
        AppConfig.model_validate(payload)


def test_app_config_rejects_extra_fields() -> None:
    payload = clone_payload()
    payload["project"]["unknown"] = "boom"
    with pytest.raises(ValidationError):
        AppConfig.model_validate(payload)
