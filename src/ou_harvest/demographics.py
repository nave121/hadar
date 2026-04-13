from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any

from .models import DemographicEstimate


DEFAULT_ACTIONS = ["gender", "race", "age", "emotion"]


def analyze_photo(
    image_path: str | Path,
    *,
    detector_backend: str = "retinaface",
) -> DemographicEstimate | None:
    try:
        deepface_module = importlib.import_module("deepface")
        exceptions_module = importlib.import_module("deepface.modules.exceptions")
    except ImportError:
        return None

    deepface = getattr(deepface_module, "DeepFace", None)
    face_not_detected = getattr(exceptions_module, "FaceNotDetected", Exception)
    if deepface is None:
        return None

    try:
        raw_result = deepface.analyze(
            img_path=str(image_path),
            actions=DEFAULT_ACTIONS,
            detector_backend=detector_backend,
            enforce_detection=True,
            silent=True,
        )
    except face_not_detected:
        return None

    result = _pick_primary_result(_normalize_results(raw_result))
    if result is None:
        return None

    region = result.get("region", {})
    return DemographicEstimate(
        dominant_gender=_as_optional_str(result.get("dominant_gender")),
        gender_scores=_coerce_scores(result.get("gender")),
        dominant_race=_as_optional_str(result.get("dominant_race")),
        race_scores=_coerce_scores(result.get("race")),
        estimated_age=_coerce_int(result.get("age")),
        dominant_emotion=_as_optional_str(result.get("dominant_emotion")),
        emotion_scores=_coerce_scores(result.get("emotion")),
        face_confidence=_coerce_float(result.get("face_confidence")),
        face_region={
            key: _coerce_int(region.get(key), default=0) or 0
            for key in ("x", "y", "w", "h")
        },
        detector_backend=detector_backend,
    )


def _normalize_results(raw_result: Any) -> list[dict[str, Any]]:
    if isinstance(raw_result, list):
        return [item for item in raw_result if isinstance(item, dict)]
    if isinstance(raw_result, dict):
        return [raw_result]
    return []


def _pick_primary_result(results: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not results:
        return None
    return max(results, key=_region_area)


def _region_area(item: dict[str, Any]) -> int:
    region = item.get("region", {})
    width = _coerce_int(region.get("w"), default=0) or 0
    height = _coerce_int(region.get("h"), default=0) or 0
    return width * height


def _coerce_scores(value: Any) -> dict[str, float]:
    if not isinstance(value, dict):
        return {}
    scores: dict[str, float] = {}
    for key, raw in value.items():
        number = _coerce_float(raw)
        if number is not None:
            scores[str(key)] = number
    return scores


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any, *, default: int | None = None) -> int | None:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
