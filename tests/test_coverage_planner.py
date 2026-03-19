from dataclasses import dataclass

from mvp_sksp.planning.coverage_planner import build_filtered_pool_for_coverage
from mvp_sksp.planning.requirements import parse_requirements
from mvp_sksp.planning.role_expander import expand_required_roles
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
class FakeTask:
    task_id: int
    url: str | None = None


@dataclass
class FakePool:
    tasks: list[FakeTask]
    items: list[FakeCandidate]


def test_coverage_planner_drops_projection_led_and_video_mixer_in_meeting_room():
    req = parse_requirements("переговорная на 12 мест под ВКС: 2 камеры, панель, BYOD")
    topology = select_topology(req)
    roles = expand_required_roles(req)

    pool = FakePool(
        tasks=[FakeTask(1), FakeTask(2), FakeTask(3)],
        items=[
            FakeCandidate("cam1", "cameras", "HD-PTZ430HSU3-W", "Prestel", "PTZ camera", "Камера для видеоконференцсвязи, NDI, PTZ", [1], 100),
            FakeCandidate("disp1", "display", "IFCKV5INT75", "NexTouch", "NextPanel 75", "Интерактивная панель 75 с Android", [2], 100),
            FakeCandidate("screen1", "display", "LMC-100116", "Lumien", "Экран", "Экран с электроприводом, полотно Matte White FiberGlass", [3], 100),
            FakeCandidate("led1", "display", "AMP 3.91", "AMILED", "LED", "Светодиодный экран для сцены", [3], 100),
            FakeCandidate("mix1", "processing", "MINI-MX", "RGBLINK", "Видеомикшер", "Видеомикшер для трансляции", [1], 100),
        ],
    )

    result = build_filtered_pool_for_coverage(pool=pool, requirements=req, topology=topology, roles=roles)

    assert "cam1" in result.kept_candidate_ids
    assert "disp1" in result.kept_candidate_ids
    assert "screen1" not in result.kept_candidate_ids
    assert "led1" not in result.kept_candidate_ids
    assert "mix1" not in result.kept_candidate_ids