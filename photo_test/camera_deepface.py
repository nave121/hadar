#!/usr/bin/env python3

from __future__ import annotations

import argparse
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any

import cv2
from deepface import DeepFace
from deepface.modules.exceptions import FaceNotDetected


DEFAULT_ACTIONS = ["gender", "race", "age", "emotion"]
DEFAULT_DETECTOR_BACKEND = "retinaface"


@dataclass
class AnalysisState:
    summary_lines: list[str]
    raw_result: dict[str, Any] | None
    region: tuple[int, int, int, int] | None
    face_confidence: float | None
    error: str | None = None
    updated_at: float = 0.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Open the webcam, detect a face, and analyze it with DeepFace."
    )
    parser.add_argument(
        "--camera-index",
        type=int,
        default=0,
        help="Webcam index passed to OpenCV. Default: 0",
    )
    parser.add_argument(
        "--min-interval",
        type=float,
        default=2.0,
        help="Seconds between DeepFace analyses. Default: 2.0",
    )
    parser.add_argument(
        "--detector-backend",
        default=DEFAULT_DETECTOR_BACKEND,
        help=(
            "DeepFace detector backend. Example values: retinaface, mtcnn, "
            "mediapipe, opencv, yunet. Default: retinaface"
        ),
    )
    parser.add_argument(
        "--expand-percentage",
        type=int,
        default=12,
        help="Expand the detected face region before analysis. Default: 12",
    )
    parser.add_argument(
        "--no-align",
        action="store_true",
        help="Disable DeepFace alignment. Alignment is enabled by default.",
    )
    parser.add_argument(
        "--anti-spoofing",
        action="store_true",
        help="Enable DeepFace anti-spoofing checks.",
    )
    return parser.parse_args()


def pick_primary_result(results: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not results:
        return None

    def region_area(item: dict[str, Any]) -> int:
        region = item.get("region", {})
        width = int(region.get("w", 0) or 0)
        height = int(region.get("h", 0) or 0)
        return width * height

    return max(results, key=region_area)


def normalize_results(raw_result: Any) -> list[dict[str, Any]]:
    if isinstance(raw_result, list):
        return [item for item in raw_result if isinstance(item, dict)]
    if isinstance(raw_result, dict):
        return [raw_result]
    return []


def analyze_frame(
    frame_bgr: Any,
    detector_backend: str,
    align: bool,
    expand_percentage: int,
    anti_spoofing: bool,
) -> AnalysisState | None:
    try:
        raw_result = DeepFace.analyze(
            img_path=frame_bgr,
            actions=DEFAULT_ACTIONS,
            detector_backend=detector_backend,
            align=align,
            expand_percentage=expand_percentage,
            enforce_detection=True,
            anti_spoofing=anti_spoofing,
            silent=True,
        )
    except FaceNotDetected:
        return None

    results = normalize_results(raw_result)
    result = pick_primary_result(results)
    if result is None:
        return None

    region = result.get("region", {})
    region_tuple = (
        int(region.get("x", 0)),
        int(region.get("y", 0)),
        int(region.get("w", 0)),
        int(region.get("h", 0)),
    )

    face_confidence = result.get("face_confidence")
    dominant_gender = result.get("dominant_gender", "unknown")
    dominant_race = result.get("dominant_race", "unknown")
    dominant_emotion = result.get("dominant_emotion", "unknown")
    age = result.get("age", "unknown")

    summary_lines = [
        f"Age: {age}",
        f"Gender: {dominant_gender}",
        f"Race: {dominant_race}",
        f"Emotion: {dominant_emotion}",
    ]

    if face_confidence is not None:
        summary_lines.append(f"Face conf: {float(face_confidence):.2f}")

    return AnalysisState(
        summary_lines=summary_lines,
        raw_result=result,
        region=region_tuple,
        face_confidence=float(face_confidence) if face_confidence is not None else None,
        updated_at=time.time(),
    )


def log_analysis(state: AnalysisState) -> None:
    if state.raw_result is None:
        return

    result = state.raw_result
    print("\n=== DeepFace analysis ===")
    print(f"Age: {result.get('age', 'unknown')}")
    print(f"Dominant gender: {result.get('dominant_gender', 'unknown')}")
    print(f"Gender scores: {result.get('gender', {})}")
    print(f"Dominant race: {result.get('dominant_race', 'unknown')}")
    print(f"Race scores: {result.get('race', {})}")
    print(f"Dominant emotion: {result.get('dominant_emotion', 'unknown')}")
    print(f"Emotion scores: {result.get('emotion', {})}")
    print(f"Face confidence: {result.get('face_confidence', 'unknown')}")


def draw_overlay(frame: Any, state: AnalysisState | None, detector_backend: str) -> None:
    if state is not None and state.region is not None:
        x, y, w, h = state.region
        cv2.rectangle(frame, (x, y), (x + w, y + h), (40, 220, 120), 2)

    lines = [f"DeepFace live analysis ({detector_backend})"]

    if state is None:
        lines.append("Searching for a face...")
    elif state.error:
        lines.append(f"Error: {state.error}")
    else:
        lines.extend(state.summary_lines)

    x = 16
    y = 28
    for index, line in enumerate(lines):
        line_y = y + index * 26
        cv2.putText(
            frame,
            line,
            (x, line_y),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.7,
            (20, 20, 20),
            5,
            cv2.LINE_AA,
        )
        cv2.putText(
            frame,
            line,
            (x, line_y),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.7,
            (255, 255, 255),
            2,
            cv2.LINE_AA,
        )


def main() -> None:
    args = parse_args()
    align = not args.no_align

    capture = cv2.VideoCapture(args.camera_index)
    if not capture.isOpened():
        raise RuntimeError(
            f"Could not open camera index {args.camera_index}. "
            "Check camera permissions and whether another app is using the webcam."
        )

    analysis_executor = ThreadPoolExecutor(max_workers=1)
    pending_analysis: Future[AnalysisState | None] | None = None
    current_state: AnalysisState | None = None
    last_analysis_started_at = 0.0

    try:
        while True:
            ok, frame = capture.read()
            if not ok:
                raise RuntimeError("Failed to read a frame from the camera.")

            now = time.time()
            if pending_analysis is None and now - last_analysis_started_at >= args.min_interval:
                pending_analysis = analysis_executor.submit(
                    analyze_frame,
                    frame.copy(),
                    args.detector_backend,
                    align,
                    args.expand_percentage,
                    args.anti_spoofing,
                )
                last_analysis_started_at = now

            if pending_analysis is not None and pending_analysis.done():
                try:
                    current_state = pending_analysis.result()
                    if current_state is not None:
                        log_analysis(current_state)
                except Exception as exc:
                    current_state = AnalysisState(
                        summary_lines=[],
                        raw_result=None,
                        region=None,
                        face_confidence=None,
                        error=str(exc),
                        updated_at=time.time(),
                    )
                finally:
                    pending_analysis = None

            draw_overlay(frame, current_state, args.detector_backend)

            cv2.putText(
                frame,
                "Press q to quit",
                (16, frame.shape[0] - 16),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.6,
                (255, 255, 255),
                2,
                cv2.LINE_AA,
            )

            cv2.imshow("DeepFace Live Camera", frame)
            key = cv2.waitKey(1) & 0xFF
            if key == ord("q"):
                break
    finally:
        if pending_analysis is not None:
            pending_analysis.cancel()
        analysis_executor.shutdown(wait=False, cancel_futures=True)
        capture.release()
        cv2.destroyAllWindows()


if __name__ == "__main__":
    main()
