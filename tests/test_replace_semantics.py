from decimal import Decimal

from mvp_sksp.domain.candidates import CandidatePool, CandidateItem
from mvp_sksp.domain.ops import PatchOperation, ItemRef, TargetSelector, MatchSelector
from mvp_sksp.domain.spec import Spec
from mvp_sksp.editing.editor import apply_operations


def test_replace_reuses_line_id_and_does_not_add():
    pool = CandidatePool(
        items=[
            CandidateItem(candidate_id="old", category="processing", sku="SKU-OLD", manufacturer="A", model="X",
                          name="Controller A", description="контроллер", unit_price_rub=Decimal("1000")),
            CandidateItem(candidate_id="new", category="processing", sku="SKU-NEW", manufacturer="B", model="Y",
                          name="Controller B", description="контроллер", unit_price_rub=Decimal("2000")),
        ],
        tasks=[],
    )
    spec = Spec(spec_id="sp1", items=[], project_title="X")
    apply_operations(spec, [PatchOperation(op="add_line", category="processing", item=ItemRef(candidate_id="old"), qty=Decimal("1"))], pool)
    line_id = spec.items[0].line_id

    ops = [
        PatchOperation(
            op="replace_line",
            target=TargetSelector(match=MatchSelector(category="processing", contains=["контроллер"])),
            category="processing",
            item=ItemRef(candidate_id="new"),
            qty=Decimal("1"),
        )
    ]
    apply_operations(spec, ops, pool)
    assert len(spec.items) == 1
    assert spec.items[0].line_id == line_id
    assert spec.items[0].sku == "SKU-NEW"
