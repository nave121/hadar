from pathlib import Path
from types import SimpleNamespace

from ou_harvest.demographics import analyze_photo
from ou_harvest.models import DemographicEstimate


def test_analyze_photo_returns_none_when_deepface_not_installed(monkeypatch):
    def fake_import(name: str):
        raise ImportError(name)

    monkeypatch.setattr("ou_harvest.demographics.importlib.import_module", fake_import)

    assert analyze_photo(Path("photo.jpg")) is None


def test_analyze_photo_returns_none_when_no_face(monkeypatch):
    class FaceNotDetected(Exception):
        pass

    def analyze(**kwargs):
        raise FaceNotDetected("no face")

    deepface_module = SimpleNamespace(DeepFace=SimpleNamespace(analyze=analyze))
    exceptions_module = SimpleNamespace(FaceNotDetected=FaceNotDetected)

    def fake_import(name: str):
        if name == "deepface":
            return deepface_module
        if name == "deepface.modules.exceptions":
            return exceptions_module
        raise ImportError(name)

    monkeypatch.setattr("ou_harvest.demographics.importlib.import_module", fake_import)

    assert analyze_photo(Path("photo.jpg")) is None


def test_analyze_photo_picks_largest_face(monkeypatch):
    calls: list[dict] = []

    def analyze(**kwargs):
        calls.append(kwargs)
        return [
            {
                "dominant_gender": "Woman",
                "gender": {"Woman": 99.0},
                "dominant_race": "white",
                "race": {"white": 88.0},
                "age": 41,
                "dominant_emotion": "happy",
                "emotion": {"happy": 90.0},
                "face_confidence": 0.81,
                "region": {"x": 10, "y": 20, "w": 12, "h": 18},
            },
            {
                "dominant_gender": "Man",
                "gender": {"Man": 80.0},
                "dominant_race": "asian",
                "race": {"asian": 92.5},
                "age": 29,
                "dominant_emotion": "neutral",
                "emotion": {"neutral": 65.0},
                "face_confidence": 0.93,
                "region": {"x": 5, "y": 7, "w": 30, "h": 25},
            },
        ]

    class FaceNotDetected(Exception):
        pass

    deepface_module = SimpleNamespace(DeepFace=SimpleNamespace(analyze=analyze))
    exceptions_module = SimpleNamespace(FaceNotDetected=FaceNotDetected)

    def fake_import(name: str):
        if name == "deepface":
            return deepface_module
        if name == "deepface.modules.exceptions":
            return exceptions_module
        raise ImportError(name)

    monkeypatch.setattr("ou_harvest.demographics.importlib.import_module", fake_import)

    estimate = analyze_photo(Path("photo.jpg"), detector_backend="mtcnn")

    assert calls[0]["img_path"] == "photo.jpg"
    assert calls[0]["detector_backend"] == "mtcnn"
    assert estimate is not None
    assert estimate.dominant_gender == "Man"
    assert estimate.dominant_race == "asian"
    assert estimate.estimated_age == 29
    assert estimate.dominant_emotion == "neutral"
    assert estimate.face_confidence == 0.93
    assert estimate.face_region == {"x": 5, "y": 7, "w": 30, "h": 25}


def test_demographic_estimate_serialization():
    estimate = DemographicEstimate(
        dominant_gender="Woman",
        gender_scores={"Woman": 99.0},
        dominant_race="white",
        race_scores={"white": 88.0},
        estimated_age=41,
        dominant_emotion="happy",
        emotion_scores={"happy": 90.0},
        face_confidence=0.81,
        face_region={"x": 1, "y": 2, "w": 3, "h": 4},
        detector_backend="mtcnn",
        analyzed_at="2024-01-01T00:00:00+00:00",
        source_artifact_id="artifact-123",
    )

    round_tripped = DemographicEstimate.model_validate_json(estimate.model_dump_json())

    assert round_tripped == estimate
