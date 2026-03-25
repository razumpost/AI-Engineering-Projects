from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text


@dataclass(frozen=True)
class ChunkRow:
    id: int
    created_at: str | None
    activity_id: str | None
    deal_id: str | None
    path: str | None
    content: str | None
    meta: dict[str, Any]


@dataclass
class CallTranscript:
    deal_id: str | None
    activity_id: str | None
    path: str | None
    chunk_ids: list[int]
    created_at_min: str | None
    created_at_max: str | None
    raw_text: str
    cleaned_text: str
    score: float
    debug: dict[str, Any]


# High-signal engineering terms with weights (so "гибкий/размеры/пиксель" wins over "КП/цена")
TECH_WEIGHTS: list[tuple[str, float]] = [
    ("гибк", 3.0),
    ("радиус", 2.5),
    ("кривизн", 2.5),
    ("стык", 2.0),
    ("двух стен", 2.0),
    ("размер", 2.0),
    ("ширин", 1.5),
    ("высот", 1.5),
    ("шаг", 2.0),
    ("пиксел", 2.0),
    ("pixel", 2.0),
    ("pitch", 2.0),
    ("ярк", 1.5),
    ("нит", 1.5),
    ("монтаж", 1.5),
    ("креп", 1.2),
    ("каркас", 1.2),
    ("питание", 1.2),
    ("электр", 1.2),
    ("процессор", 1.2),
    ("контроллер", 1.2),
    ("управлен", 1.0),
    ("кабинет", 1.0),
    ("модуль", 1.0),
    ("стена", 0.8),
    ("экран", 0.6),
    ("led", 0.8),
    ("светодиод", 0.8),
]

# “Sales/admin” is not bad, but should not dominate when choosing best technical call.
SALES_WEIGHTS: list[tuple[str, float]] = [
    ("кп", 0.2),
    ("коммерческ", 0.2),
    ("цена", 0.2),
    ("прайс", 0.2),
    ("форма", 0.1),
    ("excel", 0.1),
]

NOISE_PATTERNS = [
    r"\bробот\b",
    r"\bчеловек\b",
    r"передам это абоненту",
    r"передам их абоненту",
    r"возможно, он перезвонит",
    r"что[- ]?либо еще",
    r"в течение какого времени",
    r"подскажите.*разговариваю",
]

_SENT_SPLIT = re.compile(r"(?<=[\.\!\?])\s+|\n+")
_WS = re.compile(r"\s+")

# Detect dimensions/spec numbers (very strong signal for engineering content)
_DIM_RE = re.compile(
    r"(\d{1,3}(?:[.,]\d{1,3})?)\s*(м|mm|см|cm|м\.)"
    r"(\s*[xх×]\s*(\d{1,3}(?:[.,]\d{1,3})?)\s*(м|mm|см|cm|м\.))?",
    re.IGNORECASE,
)


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _collector_root(system_root: str) -> Path | None:
    if not system_root:
        return None
    p = Path(system_root).expanduser().resolve()
    return p.parent if p.name.startswith(".cognee") else p


def _candidate_rel_paths(rel: str | None) -> list[str]:
    rel = (rel or "").strip()
    if not rel:
        return []
    out = [rel]
    if "calls_transcripts_dev" in rel:
        out.append(rel.replace("calls_transcripts_dev", "calls_transcripts"))
    return list(dict.fromkeys(out))


def _read_text_file(fp: Path) -> str:
    return fp.read_text(encoding="utf-8", errors="replace")


def _resolve_file_text(path: str | None, collector_root: Path | None) -> str | None:
    for rel in _candidate_rel_paths(path):
        p = Path(rel)
        if p.is_absolute() and p.exists():
            return _read_text_file(p)
        if collector_root:
            cand = (collector_root / rel).resolve()
            if cand.exists():
                return _read_text_file(cand)
        cand2 = (Path.cwd() / rel).resolve()
        if cand2.exists():
            return _read_text_file(cand2)
    return None


def _as_dict_meta(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            v = json.loads(raw)
            return v if isinstance(v, dict) else {}
        except Exception:
            return {}
    return {}


def _chunk_sort_key(row: ChunkRow) -> tuple:
    m = row.meta or {}

    def _int_or_none(v: Any) -> int | None:
        try:
            if v is None or isinstance(v, bool):
                return None
            return int(str(v).strip())
        except Exception:
            return None

    candidates = [
        _int_or_none(m.get("chunk_index")),
        _int_or_none(m.get("part")),
        _int_or_none(m.get("seq")),
        _int_or_none(m.get("rec_index")),
        _int_or_none(m.get("offset")),
        _int_or_none(m.get("start_ms")),
        _int_or_none(m.get("start")),
    ]
    idx = next((c for c in candidates if c is not None), None)
    return (0 if idx is not None else 1, idx or 0, row.created_at or "", row.id)


def _merge_text(chunks: list[ChunkRow]) -> str:
    parts: list[str] = []
    for c in chunks:
        t = (c.content or "").strip()
        if t:
            parts.append(t)
    return "\n".join(parts).strip()


def clean_transcript(text_value: str) -> str:
    """
    Cleaning tuned to keep technical phrases:
    - normalize whitespace
    - remove explicit noise sentences
    - remove immediate duplicate sentences
    """
    txt = _WS.sub(" ", (text_value or "").replace("\r", "\n")).strip()
    if not txt:
        return ""

    noise_re = re.compile("|".join(f"(?:{p})" for p in NOISE_PATTERNS), re.IGNORECASE)
    sentences = [s.strip() for s in _SENT_SPLIT.split(txt) if s.strip()]

    out: list[str] = []
    prev_norm = ""
    for s in sentences:
        if noise_re.search(s):
            continue
        norm = re.sub(r"[^a-zA-Zа-яА-Я0-9]+", "", s.casefold())
        if not norm:
            continue
        if norm == prev_norm:
            continue
        if prev_norm and (norm.startswith(prev_norm) or prev_norm.startswith(norm)) and min(len(norm), len(prev_norm)) > 28:
            continue
        out.append(s)
        prev_norm = norm

    return " ".join(out).strip()


def _weighted_hits(text_value: str, weights: list[tuple[str, float]]) -> tuple[float, dict[str, int]]:
    t = (text_value or "").casefold()
    score = 0.0
    hits: dict[str, int] = {}
    for kw, w in weights:
        if not kw:
            continue
        c = t.count(kw.casefold())
        if c > 0:
            hits[kw] = c
            score += w * min(3, c)  # cap repeats
    return score, hits


def score_transcript(text_value: str) -> tuple[float, dict[str, Any]]:
    t = (text_value or "").casefold().strip()
    if not t:
        return 0.0, {"tech": 0.0, "sales": 0.0, "dims": 0, "noise": 0, "len": 0}

    tech_score, tech_hits = _weighted_hits(t, TECH_WEIGHTS)
    sales_score, sales_hits = _weighted_hits(t, SALES_WEIGHTS)

    dims = len(_DIM_RE.findall(t))
    dims_bonus = float(min(6, dims)) * 1.8

    noise = sum(1 for p in NOISE_PATTERNS if re.search(p, t, re.IGNORECASE))
    noise_penalty = float(noise) * 2.0

    # length bonus is small; we want semantics > length
    length_bonus = min(1.5, len(t) / 2200.0)

    # If it's mostly sales/admin talk and has no technical signal, push it down.
    tech_tokens = len(tech_hits)
    sales_tokens = len(sales_hits)
    sales_only_penalty = 2.0 if (tech_tokens <= 1 and sales_tokens >= 2 and dims == 0) else 0.0

    final = tech_score + dims_bonus + length_bonus + (sales_score * 0.15) - noise_penalty - sales_only_penalty
    dbg = {
        "tech_score": tech_score,
        "sales_score": sales_score,
        "dims": dims,
        "noise": noise,
        "len": len(t),
        "tech_hits": tech_hits,
        "sales_hits": sales_hits,
        "sales_only_penalty": sales_only_penalty,
    }
    return float(final), dbg


def _fetch_chunks(dsn: str, deal_id: str) -> list[ChunkRow]:
    eng = create_engine(dsn, future=True, pool_pre_ping=True)
    q = text(
        """
        SELECT
          id,
          created_at::text AS created_at,
          meta::jsonb->>'activity_id' AS activity_id,
          meta::jsonb->>'deal_id' AS deal_id,
          meta::jsonb->>'path' AS path,
          content::text AS content,
          meta::jsonb AS meta
        FROM rag_chunks
        WHERE meta::jsonb->>'deal_id' = :deal_id
        ORDER BY created_at ASC NULLS LAST, id ASC
        """
    )
    with eng.connect() as c:
        rows = c.execute(q, {"deal_id": deal_id}).mappings().all()

    return [
        ChunkRow(
            id=int(r["id"]),
            created_at=r.get("created_at"),
            activity_id=r.get("activity_id") or None,
            deal_id=r.get("deal_id") or None,
            path=r.get("path") or None,
            content=r.get("content"),
            meta=_as_dict_meta(r.get("meta")),
        )
        for r in rows
    ]


def _group_by_activity(chunks: list[ChunkRow]) -> dict[str, list[ChunkRow]]:
    groups: dict[str, list[ChunkRow]] = {}
    for ch in chunks:
        key = ch.activity_id or ch.path or f"chunk:{ch.id}"
        groups.setdefault(key, []).append(ch)
    return groups


def rank_calls(deal_id: str, prefer: str = "file") -> list[CallTranscript]:
    dsn = _env("DB_DSN")
    if not dsn:
        raise RuntimeError("DB_DSN is empty. source .env first.")

    system_root = _env("SYSTEM_ROOT_DIRECTORY")
    collector_root = _collector_root(system_root)

    chunks = _fetch_chunks(dsn, deal_id)
    groups = _group_by_activity(chunks)

    ranked: list[CallTranscript] = []

    for grp in groups.values():
        grp_sorted = sorted(grp, key=_chunk_sort_key)
        activity_id = grp_sorted[0].activity_id
        path = grp_sorted[0].path
        chunk_ids = [c.id for c in grp_sorted]

        created_at_vals = [c.created_at for c in grp_sorted if c.created_at]
        created_at_min = min(created_at_vals) if created_at_vals else None
        created_at_max = max(created_at_vals) if created_at_vals else None

        db_text = _merge_text(grp_sorted)
        file_text = _resolve_file_text(path, collector_root)

        if prefer == "file":
            raw_text = (file_text or "").strip() or db_text
        else:
            raw_text = (db_text or "").strip() or (file_text or "").strip()

        cleaned = clean_transcript(raw_text)
        sc, dbg = score_transcript(cleaned)

        ranked.append(
            CallTranscript(
                deal_id=deal_id,
                activity_id=activity_id,
                path=path,
                chunk_ids=chunk_ids,
                created_at_min=created_at_min,
                created_at_max=created_at_max,
                raw_text=raw_text,
                cleaned_text=cleaned,
                score=sc,
                debug=dbg,
            )
        )

    ranked.sort(key=lambda x: x.score, reverse=True)
    return ranked


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--deal-id", required=True)
    ap.add_argument("--prefer", choices=["db", "file"], default="file")
    ap.add_argument("--out", default="", help="JSON output path for BEST call. If empty -> stdout.")
    ap.add_argument("--print-text", action="store_true", help="Print cleaned text for BEST call.")
    ap.add_argument("--top", type=int, default=0, help="Print top-N ranking table to stderr.")
    args = ap.parse_args()

    ranked = rank_calls(args.deal_id, prefer=args.prefer)
    if not ranked:
        raise RuntimeError(f"No transcripts for deal_id={args.deal_id}")

    if args.top and args.top > 0:
        n = min(len(ranked), int(args.top))
        print(f"Top {n} calls for deal_id={args.deal_id} (prefer={args.prefer})", file=os.sys.stderr)
        for i, c in enumerate(ranked[:n], 1):
            dbg = c.debug or {}
            print(
                f"{i:>2}. score={c.score:>6.2f} activity_id={c.activity_id} "
                f"len={dbg.get('len')} tech={dbg.get('tech_score'):.1f} dims={dbg.get('dims')} "
                f"noise={dbg.get('noise')} path={c.path}",
                file=os.sys.stderr,
            )

    best = ranked[0]
    payload = {
        "deal_id": best.deal_id,
        "activity_id": best.activity_id,
        "path": best.path,
        "chunk_ids": best.chunk_ids,
        "created_at_min": best.created_at_min,
        "created_at_max": best.created_at_max,
        "score": best.score,
        "debug": best.debug,
        "raw_text_len": len(best.raw_text or ""),
        "cleaned_text_len": len(best.cleaned_text or ""),
        "cleaned_text": best.cleaned_text,
    }

    out = json.dumps(payload, ensure_ascii=False, default=str)
    if args.out.strip():
        Path(args.out).expanduser().resolve().write_text(out + "\n", encoding="utf-8")
    else:
        print(out)

    if args.print_text:
        print("\n--- CLEANED TEXT ---\n")
        print(best.cleaned_text)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())