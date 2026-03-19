from mvp_sksp.knowledge.loader import load_knowledge_map


def test_knowledge_map_loads():
    km = load_knowledge_map()
    assert "meeting_room" in km.room_types
    assert "vks" in km.capabilities
    assert "ptz_camera" in km.families
