from __future__ import annotations

from collections.abc import Sequence

import numpy as np

_EPSILON = 1e-9
_CLIP_Z = 3.0


def robust_location_scale(values: Sequence[float]) -> tuple[float, float]:
    array = np.asarray(list(values), dtype=float)
    if array.size == 0:
        return 0.0, 1.0
    median = float(np.median(array))
    mad = float(np.median(np.abs(array - median))) * 1.4826
    if mad > _EPSILON:
        return median, mad
    mean = float(np.mean(array))
    std = float(np.std(array))
    if std > _EPSILON:
        return mean, std
    max_abs = float(np.max(np.abs(array))) if array.size else 1.0
    return 0.0, max(max_abs, 1.0)


def robust_normalize(values: Sequence[float]) -> tuple[list[float], dict[str, float | str]]:
    center, scale = robust_location_scale(values)
    array = np.asarray(list(values), dtype=float)
    if array.size == 0:
        return [], {
            "method": "robust_zscore_clip",
            "center": center,
            "scale": scale,
            "clip_z": _CLIP_Z,
        }
    if array.size == 1:
        clipped = float(np.clip(array[0] / max(scale, _EPSILON), -1.0, 1.0))
        return [clipped], {
            "method": "single_value_clip",
            "center": center,
            "scale": max(scale, _EPSILON),
            "clip_z": 1.0,
        }
    z_scores = (array - center) / max(scale, _EPSILON)
    clipped = np.clip(z_scores, -_CLIP_Z, _CLIP_Z) / _CLIP_Z
    return clipped.astype(float).tolist(), {
        "method": "robust_zscore_clip",
        "center": center,
        "scale": max(scale, _EPSILON),
        "clip_z": _CLIP_Z,
    }
