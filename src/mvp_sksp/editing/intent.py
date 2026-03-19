from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal, Optional

PatchAction = Literal["replace", "add", "remove", "set_qty", "replace_brand", "unknown"]


@dataclass(frozen=True)
class PatchIntent:
    action: PatchAction
    raw: str
    target: str = ""
    replacement: str = ""
    qty: Optional[Decimal] = None
