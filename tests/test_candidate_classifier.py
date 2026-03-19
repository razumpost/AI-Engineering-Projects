from dataclasses import dataclass

from mvp_sksp.normalization.candidate_classifier import classify_candidate


@dataclass
class FakeCandidate:
    candidate_id: str
    category: str | None
    sku: str | None
    manufacturer: str | None
    name: str
    description: str | None


def test_classify_ptz_camera():
    item = FakeCandidate(
        candidate_id="ci1",
        category="cameras",
        sku="HD-PTZ430HSU3-W",
        manufacturer="Prestel",
        name="PTZ camera",
        description="Камера для видеоконференцсвязи, NDI, PTZ",
    )
    c = classify_candidate(item)
    assert c.family == "ptz_camera"
    assert c.family_confidence > 0.5


def test_classify_interactive_panel():
    item = FakeCandidate(
        candidate_id="ci2",
        category="display",
        sku="IFCKV5INT75",
        manufacturer="NexTouch",
        name="NextPanel 75",
        description="Интерактивная панель 75 с Android",
    )
    c = classify_candidate(item)
    assert c.family == "interactive_panel"


def test_classify_projection_screen():
    item = FakeCandidate(
        candidate_id="ci3",
        category="display",
        sku="LMC-100116",
        manufacturer="Lumien",
        name="Экран",
        description="Экран с электроприводом, полотно Matte White FiberGlass",
    )
    c = classify_candidate(item)
    assert c.family == "projection_screen"