# DeepFace camera test

This small script opens your webcam, shows the live feed, and runs DeepFace analysis for:

- gender
- race
- age
- emotion

The script now uses DeepFace's own detector and alignment path instead of an OpenCV Haar cascade crop. By default it uses the `retinaface` detector, which is slower but usually more accurate than the old setup.

The video window shows the latest dominant values and the latest face box, and the terminal prints the full DeepFace dictionaries for gender, race, and emotion after each analysis pass.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

On macOS, the first run may ask for camera permission.

## Run

```bash
python3 camera_deepface.py
```

Optional flags:

```bash
python3 camera_deepface.py --camera-index 0 --min-interval 2.0 --detector-backend retinaface --expand-percentage 12
```

Press `q` to quit.

If `retinaface` is too slow on your machine, try:

```bash
python3 camera_deepface.py --detector-backend mediapipe
python3 camera_deepface.py --detector-backend yunet
```
