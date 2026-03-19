# =========================
# File: src/rag_pipeline/rag/sksps_writer.py
# =========================
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from openpyxl import load_workbook
from openpyxl.styles import Alignment


@dataclass(frozen=True)
class SkspsItem:
    """One row in СкСп.

    Column mapping (template sheet: СкСп):
      1  N
      2  Manufacturer
      3  Article
      4  Description
      5  Unit price (RUB)                     -> unit_price_rub
      6  Qty                                   -> qty
      7  Sum (RUB)                             (formula)
      8  Delivery term (customer)              -> delivery_term
      9  Comment                               -> comment
      10 Link                                  -> link
      11 RRP unit price (RUB)                  -> rrp_unit_price_rub
      12 RRP sum (RUB)                          (formula)
      13 Cost unit price (RUB)                 -> cost_unit_price_rub
      14 Cost sum (RUB)                         (formula)
      15 Delivery term (supplier)              -> supplier_delivery_term
      16 Supplier                              -> supplier
      17 Registration status                   -> registration_status
      18 Payment terms                         -> payment_terms
    """

    manufacturer: str
    article: str
    description: str
    unit_price_rub: float
    qty: float
    delivery_term: str = ""
    comment: str = ""
    link: str = ""
    rrp_unit_price_rub: float = 0.0
    cost_unit_price_rub: float = 0.0
    supplier_delivery_term: str = ""
    supplier: str = ""
    registration_status: str = ""
    payment_terms: str = ""


def _to_float(x: Any, default: float = 0.0) -> float:
    if x is None:
        return default
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip().replace(" ", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return default


def _to_str(x: Any) -> str:
    return ("" if x is None else str(x)).strip()


def write_sksps_from_items(
    template_path: str,
    out_path: str,
    items: List[SkspsItem],
    *,
    sheet_name: str = "СкСп",
    start_row: int = 2,
) -> None:
    wb = load_workbook(template_path)
    if sheet_name not in wb.sheetnames:
        raise RuntimeError(f"Template missing sheet {sheet_name!r}. Has: {wb.sheetnames}")
    ws = wb[sheet_name]

    center = Alignment(vertical="center", wrap_text=True)

    row = start_row
    for i, it in enumerate(items, start=1):
        ws.cell(row=row, column=1, value=i).alignment = center
        ws.cell(row=row, column=2, value=it.manufacturer).alignment = center
        ws.cell(row=row, column=3, value=it.article).alignment = center
        ws.cell(row=row, column=4, value=it.description).alignment = center

        ws.cell(row=row, column=5, value=float(it.unit_price_rub)).alignment = center
        ws.cell(row=row, column=6, value=float(it.qty)).alignment = center

        ws.cell(row=row, column=7, value=f"=E{row}*F{row}").alignment = center

        ws.cell(row=row, column=8, value=it.delivery_term).alignment = center
        ws.cell(row=row, column=9, value=it.comment).alignment = center
        ws.cell(row=row, column=10, value=it.link).alignment = center

        ws.cell(row=row, column=11, value=float(it.rrp_unit_price_rub)).alignment = center
        ws.cell(row=row, column=12, value=f"=K{row}*F{row}").alignment = center

        ws.cell(row=row, column=13, value=float(it.cost_unit_price_rub)).alignment = center
        ws.cell(row=row, column=14, value=f"=M{row}*F{row}").alignment = center

        ws.cell(row=row, column=15, value=it.supplier_delivery_term or it.delivery_term).alignment = center
        ws.cell(row=row, column=16, value=it.supplier).alignment = center
        ws.cell(row=row, column=17, value=it.registration_status).alignment = center
        ws.cell(row=row, column=18, value=it.payment_terms).alignment = center

        row += 1

    wb.save(out_path)


def items_from_json(obj: Dict[str, Any]) -> List[SkspsItem]:
    """Accepts either:
      - {"items": [...]} (flat)
      - {"sections": [{"items": [...]}]} (nested)
    """
    items: List[Dict[str, Any]] = []

    if isinstance(obj.get("items"), list):
        items = [x for x in obj["items"] if isinstance(x, dict)]

    if not items and isinstance(obj.get("sections"), list):
        for sec in obj["sections"]:
            if isinstance(sec, dict) and isinstance(sec.get("items"), list):
                items.extend([x for x in sec["items"] if isinstance(x, dict)])

    out: List[SkspsItem] = []
    for x in items:
        out.append(
            SkspsItem(
                manufacturer=_to_str(x.get("manufacturer") or x.get("brand")),
                article=_to_str(x.get("article") or x.get("sku") or ""),
                description=_to_str(x.get("description") or x.get("name") or ""),
                unit_price_rub=_to_float(x.get("unit_price_rub") or x.get("price_rub") or x.get("price")),
                qty=_to_float(x.get("qty") or x.get("quantity") or 1),
                delivery_term=_to_str(x.get("delivery_term") or x.get("lead_time") or ""),
                comment=_to_str(x.get("comment") or ""),
                link=_to_str(x.get("link") or x.get("url") or ""),
                rrp_unit_price_rub=_to_float(x.get("rrp_unit_price_rub") or x.get("rrp_rub") or 0),
                cost_unit_price_rub=_to_float(x.get("cost_unit_price_rub") or x.get("cost_rub") or 0),
                supplier_delivery_term=_to_str(x.get("supplier_delivery_term") or ""),
                supplier=_to_str(x.get("supplier") or ""),
                registration_status=_to_str(x.get("registration_status") or ""),
                payment_terms=_to_str(x.get("payment_terms") or ""),
            )
        )
    return out

