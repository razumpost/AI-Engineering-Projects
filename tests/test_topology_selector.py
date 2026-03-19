from mvp_sksp.planning.requirements import parse_requirements
from mvp_sksp.planning.topology_selector import select_topology


def test_select_topology_meeting_room_delegate_dsp():
    req = parse_requirements("переговорная на 12 мест под ВКС: 2 камеры, панель, BYOD")
    topology = select_topology(req)
    assert topology.topology_key == "meeting_room_delegate_dsp"
    assert "room_camera_main" in topology.required_roles
    assert "room_audio_capture" in topology.required_roles


def test_select_topology_led_screen():
    req = parse_requirements("светодиодный экран для сцены 6x3")
    topology = select_topology(req)
    assert topology.topology_key == "led_screen_mobile"