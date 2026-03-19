from __future__ import annotations

from typing import Optional

from ..adapters.rag_wrappers import retrieve_candidates
from ..domain.candidates import CandidatePool
from ..domain.spec import Spec
from ..editing.parser import parse_patch_intent
from ..editing.scope import infer_scope_whitelist


def build_candidate_pool_from_repo(
    text: str,
    *,
    current_spec: Optional[Spec] = None,
    mode: str = "compose",
) -> CandidatePool:
    scope_whitelist = None
    if mode == "patch" and current_spec is not None:
        intent = parse_patch_intent(text)
        scope_whitelist = infer_scope_whitelist(current_spec, intent)

    return retrieve_candidates(text, scope_whitelist=scope_whitelist)
