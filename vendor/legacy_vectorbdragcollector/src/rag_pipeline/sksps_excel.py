# src/rag_pipeline/sksps_excel.py
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from openpyxl import load_workbook


_WS_RE = re.compile(r"\s+")


def norm_header(value: Any) -> str:
    """Normalize Excel header text for robust matching.

    - None -> ""
    - replace newlines with spaces
    - collapse whitespace
    - lowercase
    """
    if value is None:
        return ""
    s = str(value).replace("\r", " ").replace("\n", " ")
    s = _WS_RE.sub(" ", s.replace("\u00A0", " ")).strip()
    return s.casefold()


def norm_cell(value: Any) -> str:
    if value is None:
        return ""
    s = str(value)
    s = _WS_RE.sub(" ", s.replace("\u00A0", " ")).strip()
    return s


def to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = norm_cell(value).replace(" ", "").replace("\u202f", "").replace(",", ".")
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


@dataclass(frozen=True)
class TemplateSignature:
    """SkSp template signature derived from a template file."""
    headers_exact: List[str]
    headers_norm: List[str]
    required_norm: List[str]


def load_template_signature(
    template_path: str | Path,
    *,
    sheet_name: str = "СкСп",
    header_row: int = 1,
    stop_on_blank: bool = True,
) -> TemplateSignature:
    p = Path(template_path)
    wb = load_workbook(p, read_only=True, data_only=True)
    if sheet_name not in wb.sheetnames:
        raise ValueError(f"Template missing sheet {sheet_name!r}. Has: {wb.sheetnames}")
    ws = wb[sheet_name]

    exact: List[str] = []
    norm: List[str] = []
    for cell in ws[header_row]:
        v = cell.value
        if v is None and stop_on_blank:
            break
        ex = norm_cell(v)
        exact.append(ex)
        norm.append(norm_header(v))
    required = [h for h in norm if h]
    if not required:
        raise ValueError("Template signature is empty (no headers found).")
    return TemplateSignature(headers_exact=exact, headers_norm=norm, required_norm=required)


@dataclass(frozen=True)
class DetectedSheet:
    sheet_name: str
    header_row: int
    header_map: Dict[str, int]  # normalized header -> column index (1-based)
    score: int
    max_col: int


def detect_sksp_sheet(
    xlsx_path: str | Path,
    signature: TemplateSignature,
    *,
    scan_rows: int = 30,
    min_matches: int = 12,
) -> Optional[DetectedSheet]:
    """Detect a sheet+header row matching the SkSp signature."""
    p = Path(xlsx_path)
    wb = load_workbook(p, read_only=True, data_only=True)
    required = set(signature.required_norm)

    best: Optional[DetectedSheet] = None

    for sname in wb.sheetnames:
        ws = wb[sname]
        for r in range(1, scan_rows + 1):
            row = list(ws.iter_rows(min_row=r, max_row=r, values_only=False))
            if not row:
                continue
            cells = row[0]
            norms = [norm_header(c.value) for c in cells]
            if not any(norms):
                continue

            idx_map: Dict[str, int] = {}
            for idx, h in enumerate(norms, start=1):
                if h:
                    idx_map[h] = idx

            score = sum(1 for h in required if h in idx_map)
            if score >= min_matches:
                cand = DetectedSheet(
                    sheet_name=sname,
                    header_row=r,
                    header_map=idx_map,
                    score=score,
                    max_col=len(cells),
                )
                if best is None or cand.score > best.score or (cand.score == best.score and cand.header_row < best.header_row):
                    best = cand

    return best


@dataclass(frozen=True)
class SkspRow:
    """Parsed row from a SkSp-like sheet."""
    manufacturer: str
    article: str
    description: str
    unit_price: Optional[float]
    qty: Optional[float]
    supplier: str
    registration_status: str
    payment_terms: str
    delivery_term: str
    row_index: int  # excel row index


def _get_col(hmap: Dict[str, int], *names_norm: str) -> Optional[int]:
    for n in names_norm:
        if n in hmap:
            return hmap[n]
    return None


def extract_sksp_rows(
    xlsx_path: str | Path,
    detected: DetectedSheet,
    *,
    max_rows: int = 800,
    min_filled_cols: int = 2,
) -> List[SkspRow]:
    """Extract spec rows under the detected header."""
    p = Path(xlsx_path)
    wb = load_workbook(p, read_only=True, data_only=True)
    ws = wb[detected.sheet_name]

    h = detected.header_map
    # Common Russian template headers (normalized)
    c_man = _get_col(h, norm_header("Производитель"))
    c_art = _get_col(h, norm_header("Артикул"))
    c_desc = _get_col(h, norm_header("Описание"))
    c_price = _get_col(h, norm_header("Цена ₽, за шт"), norm_header("Цена"), norm_header("Цена, ₽"))
    c_qty = _get_col(h, norm_header("Кол-во, шт"), norm_header("Кол-во"), norm_header("Количество"))
    c_sup = _get_col(h, norm_header("Поставщик"))
    c_reg = _get_col(h, norm_header("Статус регистрации"), norm_header("Статус регистрации оборудования"))
    c_pay = _get_col(h, norm_header("Условия оплаты"))
    c_del = _get_col(h, norm_header("Условия поставки"), norm_header("Срок поставки"))

    start = detected.header_row + 1
    end = start + max_rows - 1

    out: List[SkspRow] = []
    empty_streak = 0

    for r in range(start, end + 1):
        values = []
        for col in range(1, detected.max_col + 1):
            v = ws.cell(row=r, column=col).value
            if v is not None and norm_cell(v):
                values.append((col, v))

        if not values:
            empty_streak += 1
            if empty_streak >= 10:
                break
            continue
        empty_streak = 0

        def val(col: Optional[int]) -> Any:
            if not col:
                return None
            return ws.cell(row=r, column=col).value

        manufacturer = norm_cell(val(c_man))
        article = norm_cell(val(c_art))
        description = norm_cell(val(c_desc))
        unit_price = to_float(val(c_price))
        qty = to_float(val(c_qty))
        supplier = norm_cell(val(c_sup))
        reg = norm_cell(val(c_reg))
        pay = norm_cell(val(c_pay))
        delivery = norm_cell(val(c_del))

        filled = sum(1 for x in (manufacturer, article, description, unit_price, qty, supplier) if (x is not None and str(x).strip() != ""))
        if filled < min_filled_cols:
            continue

        # Skip totals/footer lines
        low = f"{manufacturer} {article} {description}".casefold()
        if any(k in low for k in ("итого", "всего", "сумма", "итог")) and not article:
            continue

        out.append(
            SkspRow(
                manufacturer=manufacturer,
                article=article,
                description=description,
                unit_price=unit_price,
                qty=qty,
                supplier=supplier,
                registration_status=reg,
                payment_terms=pay,
                delivery_term=delivery,
                row_index=r,
            )
        )

    return out


def row_to_chunk_text(row: SkspRow) -> str:
    parts = [
        f"Производитель: {row.manufacturer}" if row.manufacturer else "",
        f"Артикул: {row.article}" if row.article else "",
        f"Описание: {row.description}" if row.description else "",
        f"Цена: {row.unit_price}" if row.unit_price is not None else "",
        f"Кол-во: {row.qty}" if row.qty is not None else "",
        f"Поставщик: {row.supplier}" if row.supplier else "",
        f"Статус регистрации: {row.registration_status}" if row.registration_status else "",
        f"Условия оплаты: {row.payment_terms}" if row.payment_terms else "",
        f"Срок поставки: {row.delivery_term}" if row.delivery_term else "",
    ]
    return "; ".join([p for p in parts if p]).strip()