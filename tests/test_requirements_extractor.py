from mvp_sksp.planning.requirements import parse_requirements


def test_parse_requirements_meeting_room_vks_byod():
    req = parse_requirements("переговорная на 12 мест под ВКС: 2 камеры, панель, BYOD")
    assert req.room_type == "meeting_room"
    assert req.caps.seat_count == 12
    assert req.caps.camera_count == 2
    assert req.flags.vks is True
    assert req.flags.byod is True
    assert req.flags.presentation is True


def test_parse_requirements_led_screen():
    req = parse_requirements("Светодиодный экран для сцены 6х3м")
    assert req.room_type == "led_screen"
