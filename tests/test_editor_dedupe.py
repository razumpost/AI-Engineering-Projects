from decimal import Decimal

from mvp_sksp.domain.candidates import CandidatePool, CandidateItem
from mvp_sksp.domain.ops import PatchOperation, ItemRef
from mvp_sksp.domain.spec import Spec
from mvp_sksp.editing.editor import apply_operations


def test_dedupe_merges_same_candidate_by_item_key():
    pool = CandidatePool(
        items=[
            CandidateItem(
                candidate_id="ci1",
                category="display",
                sku="SKU-1",
                manufacturer="Brand",
                model="M1",
                name="Panel",
                description="Panel",
                unit_price_rub=Decimal("100"),
            ),
        ],
        tasks=[],
    )
    spec = Spec(spec_id="sp1", items=[], project_title="X")

    ops = [
        PatchOperation(op="add_line", category="display", item=ItemRef(candidate_id="ci1"), qty=Decimal("9")),
        PatchOperation(op="add_line", category="display", item=ItemRef(candidate_id="ci1"), qty=Decimal("9")),
    ]
    apply_operations(spec, ops, pool)
    assert len(spec.items) == 1
    assert spec.items[0].qty == Decimal("18")
