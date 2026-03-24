from __future__ import annotations

from typing import Literal

from .models import ProjectRequirements


AudioProfile = Literal["lowz", "100v"]


def audio_profile(req: ProjectRequirements) -> AudioProfile:
    """
    A) 'audio profile' policy.
    - meeting_room: low-Z by default
    - auditorium: can be 100V if speech_reinforcement/paging requested
    """
    room = req.room_type
    wants_pa = bool(getattr(req.flags, "speech_reinforcement", False))

    if room == "meeting_room":
        return "lowz"

    if room == "auditorium":
        return "100v" if wants_pa else "lowz"

    # safe default
    return "lowz"