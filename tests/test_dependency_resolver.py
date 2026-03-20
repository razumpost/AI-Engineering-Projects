from dataclasses import dataclass, field

from mvp_sksp.planning.dependency_resolver import resolve_dependencies
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
    risks: list[str] = field(default_factory=list)


@dataclass
class FakePool:
    items: list[FakeCandidate]

    def by_id(self):
        return {x.candidate_id: x for x in self.items}


def test_dependency_resolver_adds_required_discussion_components():
    req = parse_requirements("переговорная на 12 мест под ВКС: 2 камеры, панель, BYOD")
    topology = select_topology(req)

    pool = FakePool(
        items=[
            FakeCandidate("disp1", "display", "IFCKV5INT75", "NexTouch", "NextPanel 75", "Интерактивная панель 75", [1], 100),
            FakeCandidate("cam1", "cameras", "HD-PTZ430HSU3-W", "Prestel", "PTZ camera", "PTZ NDI камера", [1], 100),
            FakeCandidate("del1", "conference", "CS-501D", "Relacart", "Delegate", "Пульт делегата", [1], 100),
            FakeCandidate("ch1", "conference", "CS-501C", "Relacart", "Chairman", "Пульт председателя", [1], 100),
            FakeCandidate("ctrl1", "conference", "CS-302M", "Relacart", "Controller", "Центральный блок конференц-системы", [1], 100),
            FakeCandidate("dsp1", "processing", "DAP-0404AD", "Prestel", "DSP", "Аудиопроцессор Dante", [1], 100),
            FakeCandidate("spk1", "conference", "NF4TW", "CVGaudio", "Speaker", "100V настенная акустика", [1], 100),
            FakeCandidate("amp1", "processing", "AMP-100V", "Demo", "Amp", "Усилитель для 100V линии", [1], 100),
        ]
    )

    spec = FakeSpec(
        items=[
            FakeLine("disp1", "display", "NexTouch", "IFCKV5INT75", "Интерактивная панель", 1, 100, [1]),
            FakeLine("cam1", "cameras", "Prestel", "HD-PTZ430HSU3-W", "PTZ камера", 2, 100, [1]),
            FakeLine("del1", "conference", "Relacart", "CS-501D", "Пульт делегата", 12, 100, [1]),
            FakeLine("spk1", "conference", "CVGaudio", "NF4TW", "100V акустика", 1, 100, [1]),
        ]
    )

    warnings = resolve_dependencies(spec, pool, req, topology)
    candidate_ids = {x.candidate_id for x in spec.items}

    assert "ch1" in candidate_ids
    assert "ctrl1" in candidate_ids
    assert "dsp1" in candidate_ids
    assert "amp1" in candidate_ids
    assert warnings == [] or isinstance(warnings, list)
