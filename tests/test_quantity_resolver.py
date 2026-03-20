from dataclasses import dataclass, field

from mvp_sksp.planning.quantity_resolver import resolve_quantities
from mvp_sksp.planning.requirements import parse_requirements
from mvp_sksp.planning.topology_selector import select_topology


@dataclass
class FakeCandidate:
    candidate_id: str
    category: str | None
    sku: str | None
    manufacturer: str | None
    name: str
    description: str | None
    evidence_task_ids: list[int] | None = None
    unit_price_rub: float | None = None


@dataclass
class FakeLine:
    candidate_id: str
    category: str
    manufacturer: str
    sku: str
    description: str
    qty: int | float
    unit_price_rub: float | None = None
    evidence_task_ids: list[int] = field(default_factory=list)


@dataclass
class FakeSpec:
    items: list[FakeLine]


@dataclass
class FakePool:
    items: list[FakeCandidate]

    def by_id(self):
        return {x.candidate_id: x for x in self.items}


def test_quantity_resolver_normalizes_meeting_room_stack():
    req = parse_requirements("переговорная на 12 мест под ВКС: 2 камеры, панель, BYOD")
    topology = select_topology(req)

    pool = FakePool(
        items=[
            FakeCandidate("disp1", "display", "IFCKV5INT75", "NexTouch", "NextPanel 75", "Интерактивная панель 75", [1], 100),
            FakeCandidate("cam1", "cameras", "HD-PTZ430HSU3-W", "Prestel", "PTZ camera", "PTZ NDI камера", [1], 100),
            FakeCandidate("del1", "conference", "CS-501D", "Relacart", "Delegate", "Пульт делегата", [1], 100),
            FakeCandidate("ch1", "conference", "CS-501C", "Relacart", "Chairman", "Пульт председателя", [1], 100),
            FakeCandidate("spk1", "conference", "NF4TW", "CVGaudio", "Speaker", "100V настенная акустика", [1], 100),
            FakeCandidate("byod1", "signal_transport", "VWC-HC14", "Prestel", "BYOD", "USB-C/HDMI gateway", [1], 100),
        ]
    )

    spec = FakeSpec(
        items=[
            FakeLine("cam1", "cameras", "Prestel", "HD-PTZ430HSU3-W", "PTZ камера", 4, 100, [1]),
            FakeLine("disp1", "display", "NexTouch", "IFCKV5INT75", "Интерактивная панель", 3, 100, [1]),
            FakeLine("del1", "conference", "Relacart", "CS-501D", "Пульт делегата", 13, 100, [1]),
            FakeLine("ch1", "conference", "Relacart", "CS-501C", "Пульт председателя", 2, 100, [1]),
            FakeLine("spk1", "conference", "CVGaudio", "NF4TW", "100V акустика", 1, 100, [1]),
            FakeLine("byod1", "signal_transport", "Prestel", "VWC-HC14", "BYOD gateway", 3, 100, [1]),
        ]
    )

    warnings = resolve_quantities(spec, pool, req, topology)

    by_id = {x.candidate_id: x for x in spec.items}
    assert by_id["cam1"].qty == 2
    assert by_id["disp1"].qty == 1
    assert by_id["ch1"].qty == 1
    assert by_id["del1"].qty == 11
    assert by_id["spk1"].qty == 2
    assert by_id["byod1"].qty == 1
    assert warnings == [] or isinstance(warnings, list)