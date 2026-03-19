from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple


@dataclass(frozen=True)
class ExtractConfig:
    max_chars: int = 200_000          # общий лимит текста на файл
    max_rows_per_sheet: int = 3000    # для Excel
    max_sheets: int = 10              # для Excel
    max_pdf_pages: int = 30           # для PDF


def _clip(s: str, n: int) -> str:
    if len(s) <= n:
        return s
    return s[: n - 200] + "\n...\n[TRUNCATED]\n"


def extract_text(path: str | Path, cfg: ExtractConfig = ExtractConfig()) -> Tuple[str, dict]:
    p = Path(path)
    ext = p.suffix.lower()

    try:
        if ext in (".xlsx", ".xlsm", ".xltx", ".xltm"):
            return _extract_xlsx(p, cfg)
        if ext == ".pdf":
            return _extract_pdf(p, cfg)
        if ext == ".docx":
            return _extract_docx(p, cfg)
        if ext == ".pptx":
            return _extract_pptx(p, cfg)
        if ext in (".txt", ".md", ".csv"):
            txt = p.read_text(encoding="utf-8", errors="ignore")
            return _clip(txt, cfg.max_chars), {"type": ext[1:]}
    except Exception as e:
        return "", {"error": str(e), "type": ext[1:]}

    # неизвестный/бинарный формат
    return "", {"type": ext[1:], "note": "unsupported format (indexed by filename/meta only)"}


def _extract_xlsx(p: Path, cfg: ExtractConfig) -> Tuple[str, dict]:
    try:
        import openpyxl  # type: ignore
    except Exception:
        return "", {"type": "xlsx", "note": "openpyxl not installed"}

    wb = openpyxl.load_workbook(p, read_only=True, data_only=True)
    sheet_names = wb.sheetnames[: cfg.max_sheets]

    out_parts: list[str] = []
    total_chars = 0
    total_rows = 0

    for sname in sheet_names:
        ws = wb[sname]
        out_parts.append(f"\n\n## SHEET: {sname}\n")
        row_i = 0

        for row in ws.iter_rows(values_only=True):
            row_i += 1
            if row_i > cfg.max_rows_per_sheet:
                out_parts.append("[TRUNCATED ROWS]\n")
                break

            vals = []
            for v in row:
                if v is None:
                    continue
                sv = str(v).strip()
                if sv:
                    vals.append(sv)
            if not vals:
                continue

            line = "\t".join(vals)
            out_parts.append(line + "\n")
            total_rows += 1

            total_chars += len(line)
            if total_chars >= cfg.max_chars:
                out_parts.append("\n[TRUNCATED]\n")
                break

        if total_chars >= cfg.max_chars:
            break

    wb.close()
    text = "".join(out_parts)
    text = _clip(text, cfg.max_chars)
    meta = {"type": "xlsx", "sheets": sheet_names, "rows_written": total_rows}
    return text, meta


def _extract_pdf(p: Path, cfg: ExtractConfig) -> Tuple[str, dict]:
    # лучший вариант — PyMuPDF (fitz). Если нет — просто вернём пусто.
    try:
        import fitz  # type: ignore
    except Exception:
        return "", {"type": "pdf", "note": "pymupdf not installed"}

    doc = fitz.open(str(p))
    pages = min(len(doc), cfg.max_pdf_pages)

    parts: list[str] = []
    for i in range(pages):
        page = doc[i]
        parts.append(f"\n\n## PAGE {i+1}\n")
        parts.append(page.get_text("text"))

        if sum(len(x) for x in parts) >= cfg.max_chars:
            parts.append("\n[TRUNCATED]\n")
            break

    doc.close()
    text = _clip("".join(parts), cfg.max_chars)
    meta = {"type": "pdf", "pages": pages}
    return text, meta


def _extract_docx(p: Path, cfg: ExtractConfig) -> Tuple[str, dict]:
    try:
        from docx import Document  # type: ignore
    except Exception:
        return "", {"type": "docx", "note": "python-docx not installed"}

    d = Document(str(p))
    parts: list[str] = []
    for para in d.paragraphs:
        t = (para.text or "").strip()
        if t:
            parts.append(t)

    text = _clip("\n".join(parts), cfg.max_chars)
    meta = {"type": "docx", "paras": len(parts)}
    return text, meta


def _extract_pptx(p: Path, cfg: ExtractConfig) -> Tuple[str, dict]:
    try:
        from pptx import Presentation  # type: ignore
    except Exception:
        return "", {"type": "pptx", "note": "python-pptx not installed"}

    prs = Presentation(str(p))
    parts: list[str] = []
    for si, slide in enumerate(prs.slides, start=1):
        parts.append(f"\n\n## SLIDE {si}\n")
        for shape in slide.shapes:
            if hasattr(shape, "text") and shape.text:
                parts.append(shape.text)

        if sum(len(x) for x in parts) >= cfg.max_chars:
            parts.append("\n[TRUNCATED]\n")
            break

    text = _clip("\n".join(parts), cfg.max_chars)
    meta = {"type": "pptx", "slides": len(prs.slides)}
    return text, meta
