from __future__ import annotations

from pathlib import Path
import subprocess


def extract_pdf_text(path: str | Path) -> str:
    file_path = Path(path)
    try:
        import pdfplumber  # type: ignore
    except ImportError:
        pdfplumber = None

    if pdfplumber is not None:
        text_chunks: list[str] = []
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ""
                if text:
                    text_chunks.append(text)
        if text_chunks:
            return "\n\n".join(text_chunks)

    try:
        from pypdf import PdfReader  # type: ignore
    except ImportError:
        return _extract_with_pdftotext(file_path)

    try:
        reader = PdfReader(str(file_path))
        text = "\n\n".join(filter(None, (page.extract_text() for page in reader.pages)))
        if text:
            return text
    except Exception:
        pass

    return _extract_with_pdftotext(file_path)


def _extract_with_pdftotext(path: Path) -> str:
    try:
        result = subprocess.run(
            ["pdftotext", "-layout", str(path), "-"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        return ""
    return result.stdout.strip()
