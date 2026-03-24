from dataclasses import dataclass
from decimal import Decimal

from mvp_sksp.knowledge.models import ProjectRequirements
from mvp_sksp.planning.plan_models import TopologyDecision
from mvp_sksp.planning.role_expander import ExpandedRole
from mvp_sksp.planning.triage import run_triage


@dataclass
class FakeLine:
    category: str
    manufacturer: str
    sku: str
    name: str
    description: str
    qty: Decimal


@dataclass
class FakeSpec:
    items: list[FakeLine]
    risks: list[str]


def test_triage_detects_100v_speaker_amp_mismatch():
    spec = FakeSpec(
        items=[
            FakeLine(
                category="conference",
                manufacturer="CVGaudio",
                sku="NF4TW",
                name="Настенная акустика",
                description="Используется только в составе 100V трансляционных линий",
                qty=Decimal("2"),
            ),
            FakeLine(
                category="conference",
                manufacturer="Enewave",
                sku="DA 650.4",
                name="Усилитель мощности",
                description="4×650Вт (8Ом), 4×1105Вт (4Ом)",
                qty=Decimal("1"),
            ),
        ],
        risks=[],
    )

    req = ProjectRequirements(room_type="meeting_room")
    topo = TopologyDecision(topology_key="meeting_room_delegate_dsp", score=1.0, reason="test")
    roles: list[ExpandedRole] = []

    issues = run_triage(spec=spec, requirements=req, topology=topo, roles=roles)
    codes = {x.code for x in issues}
    assert "incompatible_amp_for_100v_speakers" in codes