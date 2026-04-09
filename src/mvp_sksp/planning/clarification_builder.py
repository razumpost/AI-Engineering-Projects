from __future__ import annotations

from typing import Iterable


_FAMILY_QUESTIONS: dict[str, str] = {
    "delegate_unit": "Нужна проводная или беспроводная дискуссионная система для делегатов? Есть ли предпочтение по бренду/серии?",
    "chairman_unit": "Нужен ли отдельный председательский пульт с приоритетом/мьютом/управлением очередью?",
    "discussion_central_unit": "Нужен ли центральный блок дискуссионной системы и есть ли требования по бренду/совместимости?",
    "discussion_dsp": "Нужна ли интеграция с внешней акустикой, ВКС или звукоусилением через DSP/аудиопроцессор?",
    "power_supply_discussion": "Для системы требуется отдельный блок питания/расширения. Есть ли ограничения по размещению и резервированию питания?",
    "display_panel": "Подтвердите тип основного средства отображения: профессиональная панель или интерактивная панель?",
    "interactive_panel": "Подтвердите, что нужна именно интерактивная панель, а не обычный профессиональный дисплей.",
    "ptz_camera": "Сколько камер нужно и требуется ли PTZ-управление/пресеты?",
    "fixed_conference_camera": "Подтвердите, достаточно ли фиксированной конференц-камеры без PTZ.",
    "videobar": "Подтвердите, допустим ли видеобар как единое устройство камера+микрофоны+акустика.",
    "cabling_av": "Нужна ли отдельная детализация кабельной инфраструктуры в первой версии СкСп?",
}


_ROLE_QUESTIONS: dict[str, str] = {
    "room_display_main": "Уточните тип и диагональ основного средства отображения.",
    "room_camera_main": "Уточните количество и тип основной камеры для помещения.",
    "room_audio_capture": "Уточните, какая система захвата речи нужна: спикерфон, настольные микрофоны, потолочный массив или дискуссионная система.",
    "room_audio_playback": "Уточните требования к акустике: встроенный звук панели, саундбар или отдельная акустическая система.",
    "room_signal_switching": "Нужен ли отдельный коммутатор/свитчер или достаточно прямого подключения к дисплею/ВКС.",
}


def build_clarification_questions(
    *,
    uncovered_families: Iterable[str] | None = None,
    uncovered_roles: Iterable[str] | None = None,
) -> list[str]:
    questions: list[str] = []
    seen: set[str] = set()

    for fam in uncovered_families or []:
        q = _FAMILY_QUESTIONS.get(str(fam))
        if q and q not in seen:
            seen.add(q)
            questions.append(q)

    for role in uncovered_roles or []:
        q = _ROLE_QUESTIONS.get(str(role))
        if q and q not in seen:
            seen.add(q)
            questions.append(q)

    return questions