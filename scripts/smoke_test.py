from __future__ import annotations

from dataclasses import dataclass

from mvp_sksp.config import Settings
from mvp_sksp.knowledge.loader import load_knowledge_map
from mvp_sksp.normalization.candidate_classifier import classify_candidates
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


def main() -> int:
    s = Settings()
    km = load_knowledge_map()
    req = parse_requirements("переговорная на 12 мест под ВКС: 2 камеры, панель, BYOD")
    topology = select_topology(req)
    roles = expand_required_roles(req)

    demo_items = [
        FakeCandidate("ci1", "cameras", "HD-PTZ430HSU3-W", "Prestel", "PTZ camera", "Камера для видеоконференцсвязи, NDI, PTZ", [79004], 162500),
        FakeCandidate("ci2", "display", "IFCKV5INT75", "NexTouch", "NextPanel 75", "Интерактивная панель 75 с Android", [79042], 471000),
        FakeCandidate("ci3", "display", "LMC-100116", "Lumien", "Экран", "Экран с электроприводом, полотно Matte White FiberGlass", [78972], 64055),
        FakeCandidate("ci4", "processing", "MINI-MX", "RGBLINK", "Видеомикшер", "Видеомикшер для трансляции", [79004], 198556),
    ]
    demo_tasks = [FakeTask(78972), FakeTask(79004), FakeTask(79042)]

    classified = classify_candidates(demo_items)
    filtered = build_filtered_pool_for_coverage(
        pool=FakePool(tasks=demo_tasks, items=demo_items),
        requirements=req,
        topology=topology,
        roles=roles,
    )

    print("Settings:")
    print("  endpoint:", s.yandex_fm_endpoint)
    print("  bitrix_base_url:", s.bitrix_base_url or "<empty>")
    print("Knowledge:")
    print("  room_types:", len(km.room_types))
    print("  roles:", len(km.roles))
    print("  families:", len(km.families))
    print("  topology_patterns:", len(km.topology_patterns))
    print("  conflict_rules:", len(km.conflict_rules))
    print("Planning:")
    print("  requirements:", req.model_dump(mode="json"))
    print("  topology:", topology.model_dump(mode="json"))
    print("  roles:", [r.role_key for r in roles[:12]])
    print("Normalization:")
    for c in classified:
        print(" ", c.model_dump(mode="json"))
    print("Coverage:")
    print("  kept:", filtered.kept_candidate_ids)
    print("  dropped:", filtered.dropped_candidate_ids)
    for d in filtered.role_debug:
        print(" ", d)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())