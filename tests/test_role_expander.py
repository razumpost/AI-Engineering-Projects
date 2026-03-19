from mvp_sksp.planning.requirements import parse_requirements
from mvp_sksp.planning.role_expander import expand_required_roles


def test_role_expander_adds_secondary_camera_and_byod_roles():
    req = parse_requirements("переговорная на 12 мест под ВКС: 2 камеры, панель, BYOD")
    roles = expand_required_roles(req)
    by_key = {r.role_key: r for r in roles}

    assert "room_display_main" in by_key
    assert "room_camera_main" in by_key
    assert "room_camera_secondary" in by_key
    assert by_key["room_camera_secondary"].suggested_qty == 1
    assert "room_byod_ingest" in by_key
    assert "room_usb_bridge_or_byod_gateway" in by_key


def test_meeting_room_display_role_prefers_panels_not_projector():
    req = parse_requirements("переговорная на 10 мест с панелью")
    roles = expand_required_roles(req)
    display_role = next(r for r in roles if r.role_key == "room_display_main")
    assert "display_panel" in display_role.allowed_families
    assert "interactive_panel" in display_role.allowed_families
    assert "projector" not in display_role.allowed_families
