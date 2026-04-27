from __future__ import annotations

from typing import Any

from ..knowledge.models import ProjectRequirements
from ..normalization.candidate_classifier import classify_candidate
from ..planning.plan_models import TopologyDecision


def _safe_set(obj: Any, name: str, value: Any) -> None:
    try:
        setattr(obj, name, value)
    except Exception:
        pass


def _items(spec: Any) -> list[Any]:
    return list(getattr(spec, "items", []) or [])


def _line_qty(line: Any) -> int:
    try:
        return int(round(float(getattr(line, "qty", 0) or 0)))
    except Exception:
        return 0


def _to_float(v: Any) -> float | None:
    if v in (None, "", "-", "—"):
        return None
    try:
        f = float(v)
    except Exception:
        return None
    if f == 0:
        return None
    return f


def _line_price(line: Any) -> float | None:
    money = getattr(line, "unit_price", None)
    if money is not None:
        amount = getattr(money, "amount", None)
        f = _to_float(amount)
        if f is not None:
            return f

    for key in ("unit_price_rub", "unit_price", "price", "price_rub"):
        f = _to_float(getattr(line, key, None))
        if f is not None:
            return f

    meta = getattr(line, "meta", None)
    if isinstance(meta, dict):
        for key in ("unit_price_rub", "unit_price", "price", "price_rub"):
            f = _to_float(meta.get(key))
            if f is not None:
                return f

    return None


def _line_name(line: Any) -> str:
    return str(getattr(line, "name", "") or getattr(line, "description", "") or "Позиция").strip()


def _placeholder_kind(line: Any) -> str | None:
    meta = getattr(line, "meta", None)
    if isinstance(meta, dict) and meta.get("placeholder_kind"):
        return str(meta.get("placeholder_kind"))
    return None


def _task_ids(line: Any) -> list[int]:
    vals = list(getattr(line, "evidence_task_ids", []) or [])
    out: list[int] = []
    for v in vals:
        try:
            out.append(int(v))
        except Exception:
            pass
    return out


def _family(line: Any) -> str | None:
    try:
        return classify_candidate(line).family
    except Exception:
        return None


def _find_first_by_family(spec: Any, family: str) -> Any | None:
    for line in _items(spec):
        if _family(line) == family:
            return line
    return None


def _find_first_placeholder(spec: Any, kind: str) -> Any | None:
    for line in _items(spec):
        if _placeholder_kind(line) == kind:
            return line
    return None


def _grounded_role(line: Any) -> str | None:
    meta = getattr(line, "meta", None)
    if isinstance(meta, dict) and meta.get("grounded_from_role"):
        return str(meta.get("grounded_from_role"))
    return None


def _has_videowall_role(spec: Any, role: str) -> bool:
    for line in _items(spec):
        if _grounded_role(line) == role or _placeholder_kind(line) == role:
            return True

    family_fallback = {
        "videowall_panel": ["videowall_panel", "display_panel"],
        "videowall_mount": ["videowall_mount", "mounting_kit"],
        "videowall_controller": ["videowall_controller"],
        "matrix_switcher": ["matrix_switcher"],
        "cabling_av": ["cabling_av", "power_accessories"],
    }
    for fam in family_fallback.get(role, []):
        if _find_first_by_family(spec, fam) is not None:
            return True
    return False


def _find_first_any_family(spec: Any, families: list[str]) -> Any | None:
    for fam in families:
        line = _find_first_by_family(spec, fam)
        if line is not None:
            return line
    return None


def _has_line(spec: Any, family_or_placeholder: str) -> bool:
    return (
        _find_first_by_family(spec, family_or_placeholder) is not None
        or _find_first_placeholder(spec, family_or_placeholder) is not None
    )


def _summary(spec: Any, requirements: ProjectRequirements, topology: TopologyDecision) -> str:
    _ = topology
    seat_count = int(requirements.caps.seat_count or 0)
    display_count = int(requirements.caps.display_count or 0)

    if requirements.room_type == "videowall":
        base = (
            f"Система видеостены на {display_count} панелей"
            if display_count > 0
            else "Система видеостены"
        )
        detail_parts: list[str] = []
        if _has_videowall_role(spec, "videowall_panel"):
            detail_parts.append("панелями видеостены")
        if _has_videowall_role(spec, "videowall_mount"):
            detail_parts.append("креплением/каркасом")
        if _has_videowall_role(spec, "videowall_controller"):
            detail_parts.append("контроллером видеостены")
        if _has_videowall_role(spec, "matrix_switcher"):
            detail_parts.append("матричной коммутацией")
        if _has_videowall_role(spec, "cabling_av"):
            detail_parts.append("кабельной системой")

        if detail_parts:
            return base + " с " + ", ".join(detail_parts) + ". Требуется уточнение моделей ключевых компонентов."

        return base + "."

    if requirements.room_type == "meeting_room":
        base = (
            f"Конференц-система для переговорной на {seat_count} места"
            if seat_count > 0
            else "Конференц-система для переговорной"
        )
    else:
        base = "Конференц-система"

    detail_parts: list[str] = []

    delegate_line = _find_first_by_family(spec, "delegate_unit")
    if delegate_line is not None:
        dq = _line_qty(delegate_line)
        detail_parts.append(f"пультами делегатов ({dq} шт.)" if dq > 0 else "пультами делегатов")

    if _has_line(spec, "chairman_unit"):
        detail_parts.append("пультом председателя")

    if _has_line(spec, "discussion_central_unit"):
        detail_parts.append("центральным блоком")

    if _has_line(spec, "discussion_dsp"):
        detail_parts.append("интеграцией со звуком через DSP")

    if _has_line(spec, "power_supply_discussion"):
        detail_parts.append("блоком питания")

    if (
        _has_line(spec, "audio_lowz")
        or _has_line(spec, "wall_speaker")
        or _has_line(spec, "ceiling_speaker")
        or _has_line(spec, "soundbar")
    ):
        detail_parts.append("акустикой")

    if detail_parts:
        return base + " с " + ", ".join(detail_parts) + ". Требуется уточнение моделей ключевых компонентов."

    return base + "."


def _selection_bullets(spec: Any, requirements: ProjectRequirements, topology: TopologyDecision) -> list[str]:
    _ = requirements
    bullets: list[str] = []

    if requirements.room_type == "videowall":
        if _has_videowall_role(spec, "videowall_panel"):
            bullets.append("Сначала закрыта базовая роль wall_display_tiles (панели видеостены).")
        if _has_videowall_role(spec, "videowall_mount"):
            bullets.append("Добавлена роль wall_mounting_system (крепление/каркас видеостены).")
        if _has_videowall_role(spec, "videowall_controller"):
            bullets.append("Добавлена роль wall_signal_controller (контроллер/процессор видеостены).")
        if _has_videowall_role(spec, "matrix_switcher"):
            bullets.append("Добавлена роль wall_signal_distribution (матричный коммутатор).")
        if _has_videowall_role(spec, "cabling_av"):
            bullets.append("Добавлена роль wall_cabling_and_accessories (коммутация и кабельная система).")

        missing_families: list[str] = []
        for fam in ["videowall_panel", "videowall_mount", "videowall_controller", "matrix_switcher", "cabling_av"]:
            if _find_first_placeholder(spec, fam) is not None:
                missing_families.append(fam)
        if missing_families:
            bullets.append(
                "Часть критических family из ENGINEERING_GRAPH_CONTEXT "
                f"({', '.join(missing_families)}) пока не покрыта надежными candidate_id и добавлена как placeholder."
            )

        if not bullets:
            bullets.append("Состав видеостены сформирован на основе инженерного контекста и финальной спецификации.")
        return bullets

    delegate = _find_first_by_family(spec, "delegate_unit")
    psu = _find_first_by_family(spec, "power_supply_discussion")

    if delegate is not None and psu is not None:
        bullets.append(
            "Сначала закрыты обязательные роли выбранной topology: room_audio_capture "
            "(пульты делегатов) и часть room_cabling_and_accessories (блок питания)."
        )
    elif delegate is not None:
        bullets.append("Сначала закрыта обязательная роль room_audio_capture (пульты делегатов).")
    elif psu is not None:
        bullets.append("Сначала закрыта обязательная роль room_cabling_and_accessories (блок питания).")

    missing_families: list[str] = []
    for fam in ["chairman_unit", "discussion_central_unit", "discussion_dsp"]:
        if _find_first_placeholder(spec, fam) is not None:
            missing_families.append(fam)

    if missing_families:
        bullets.append(
            "Критические family из ENGINEERING_GRAPH_CONTEXT "
            f"({', '.join(missing_families)}) пока не покрыты конкретными candidate_id "
            "и добавлены как placeholder-строки."
        )

    if _find_first_placeholder(spec, "audio_lowz") is not None:
        bullets.append(
            "Аудиовоспроизведение не найдено в candidate pool, поэтому добавлена "
            "placeholder-строка на акустику для переговорной."
        )

    if not bullets:
        bullets.append("Состав сформирован на основе инженерного контекста и финальной спецификации.")

    return bullets


def _quantity_and_price_bullets(spec: Any, requirements: ProjectRequirements, topology: TopologyDecision) -> list[str]:
    _ = topology
    bullets: list[str] = []

    seat_count = int(requirements.caps.seat_count or 0)
    display_count = int(requirements.caps.display_count or 0)

    if requirements.room_type == "videowall":
        panel = _find_first_any_family(spec, ["videowall_panel"])
        mount = _find_first_any_family(spec, ["videowall_mount", "mounting_kit"])
        controller = _find_first_any_family(spec, ["videowall_controller"])
        matrix = _find_first_any_family(spec, ["matrix_switcher"])
        cabling = _find_first_any_family(spec, ["cabling_av"])

        qty_parts: list[str] = []
        if panel is not None:
            pq = _line_qty(panel)
            if pq > 0:
                qty_parts.append(f"Панели видеостены: {pq} шт.")
            elif display_count > 0:
                qty_parts.append(f"Панели видеостены: целевое количество {display_count} шт.")
        elif display_count > 0:
            qty_parts.append(f"Панели видеостены: целевое количество {display_count} шт.")

        if mount is not None:
            mq = _line_qty(mount)
            qty_parts.append(f"Крепление/каркас: {mq if mq > 0 else 1} шт.")

        if controller is not None:
            cq = _line_qty(controller)
            qty_parts.append(f"Контроллер видеостены: {cq if cq > 0 else 1} шт.")

        if matrix is not None:
            xq = _line_qty(matrix)
            qty_parts.append(f"Матричный коммутатор: {xq if xq > 0 else 1} шт.")

        if cabling is not None:
            wq = _line_qty(cabling)
            qty_parts.append(f"Коммутация/кабели: {wq if wq > 0 else 1} комплект.")

        if qty_parts:
            bullets.append("; ".join(qty_parts))

    delegate = _find_first_by_family(spec, "delegate_unit")
    chairman = _find_first_placeholder(spec, "chairman_unit") or _find_first_by_family(spec, "chairman_unit")
    central = _find_first_placeholder(spec, "discussion_central_unit") or _find_first_by_family(spec, "discussion_central_unit")
    psu = _find_first_by_family(spec, "power_supply_discussion") or _find_first_placeholder(spec, "power_supply_discussion")
    dsp = _find_first_placeholder(spec, "discussion_dsp") or _find_first_by_family(spec, "discussion_dsp")
    audio = (
        _find_first_placeholder(spec, "audio_lowz")
        or _find_first_by_family(spec, "wall_speaker")
        or _find_first_by_family(spec, "ceiling_speaker")
        or _find_first_by_family(spec, "soundbar")
    )

    qty_parts: list[str] = []

    if delegate is not None:
        dq = _line_qty(delegate)
        if seat_count > 0 and chairman is not None:
            qty_parts.append(
                f"Количество пультов делегатов: {dq} шт., "
                f"исходя из {seat_count} мест минус одно место председателя"
            )
        elif dq > 0:
            qty_parts.append(f"Количество пультов делегатов: {dq} шт.")

    if chairman is not None:
        qty_parts.append("Пульт председателя: 1 шт.")

    if central is not None:
        qty_parts.append("Центральный блок: 1 шт.")

    if psu is not None:
        qty_parts.append("Блок питания: 1 шт.")

    if dsp is not None:
        qty_parts.append("DSP: 1 шт.")

    if audio is not None:
        aq = _line_qty(audio)
        if aq > 0:
            qty_parts.append(f"Акустика: {aq} шт.")
        else:
            qty_parts.append("Акустика: требуется подбор.")

    if qty_parts:
        bullets.append("; ".join(qty_parts) + ".")

    priced: list[str] = []
    missing_price: list[str] = []

    for line in _items(spec):
        name = _line_name(line)
        price = _line_price(line)
        if price is None:
            missing_price.append(name)
        else:
            pretty = int(price) if float(price).is_integer() else price
            priced.append(f"{name} — {pretty} ₽/шт")

    if priced:
        bullets.append("Цены найдены для: " + "; ".join(priced[:8]) + ".")

    if missing_price:
        uniq: list[str] = []
        seen: set[str] = set()
        for x in missing_price:
            if x not in seen:
                uniq.append(x)
                seen.add(x)
        bullets.append("Цена требует уточнения для: " + "; ".join(uniq[:8]) + ".")

    return bullets


def _precedents(spec: Any) -> list[str]:
    tids: list[int] = []
    seen: set[int] = set()

    for line in _items(spec):
        for tid in _task_ids(line):
            if tid not in seen:
                seen.add(tid)
                tids.append(tid)

    return [str(x) for x in tids]


def _manager_questions(spec: Any, requirements: ProjectRequirements, topology: TopologyDecision) -> list[str]:
    _ = requirements
    _ = topology
    qs: list[str] = []

    if requirements.room_type == "videowall":
        if _find_first_placeholder(spec, "videowall_panel") is not None:
            qs.append("Нужны панели именно для LCD видеостены (узкий шов), или допустим другой тип экрана?")
        if _find_first_placeholder(spec, "videowall_mount") is not None:
            qs.append("Какой тип монтажа видеостены требуется: настенный каркас/pull-out или иная конструкция?")
        if _find_first_placeholder(spec, "videowall_controller") is not None:
            qs.append("Какой контроллер/процессор видеостены планируется (кол-во окон, пресеты, сценарии)?")
        if _find_first_placeholder(spec, "matrix_switcher") is not None:
            qs.append("Подтвердите требуемую матрицу по входам/выходам (например 12x4) и типы сигналов (HDMI/DP/IP).")
        if _find_first_placeholder(spec, "cabling_av") is not None:
            qs.append("Какие расстояния и трассы по кабельной системе видеостены нужно закладывать?")

        qs.extend(
            [
                "Сколько одновременно независимых источников контента должно отображаться на видеостене?",
                "Нужны ли резервирование контроллера/матрицы и требования по 24/7 режиму?",
                "Требуется ли удаленное управление и интеграция с общей системой управления объекта?",
            ]
        )

        out: list[str] = []
        seen: set[str] = set()
        for q in qs:
            if q not in seen:
                seen.add(q)
                out.append(q)
        return out

    if _find_first_placeholder(spec, "discussion_central_unit") is not None:
        qs.append(
            "Какой центральный блок конференц-системы (discussion_central_unit) предполагается "
            "использовать? Он обязателен для работы пультов делегатов и председателя."
        )

    if _find_first_placeholder(spec, "chairman_unit") is not None:
        qs.append(
            "Какой модели нужен пульт председателя (chairman_unit), если он отличается "
            "от пульта делегата?"
        )

    if _find_first_placeholder(spec, "audio_lowz") is not None:
        qs.append(
            "Нужны ли отдельные колонки (настенные/потолочные) для озвучивания комнаты? "
            "Это обязательная роль аудиовоспроизведения (room_audio_playback)."
        )

    if _find_first_placeholder(spec, "discussion_dsp") is not None:
        qs.append(
            "Требуется ли отдельный процессор обработки звука (DSP) для интеграции со звуком, "
            "или центральный блок имеет встроенную обработку?"
        )

    if _find_first_by_family(spec, "power_supply_discussion") is not None:
        qs.append(
            "Какая кабельная инфраструктура требуется, кроме блока питания? "
            "(например, кабели для подключения пультов, центрального блока, аудио)"
        )

    qs.extend(
        [
            "Какое расстояние от стола подключений до панели/экрана? Если до ~10 м — можно упростить коммутацию.",
            "ВКС будет через ноутбук (BYOD) или нужен отдельный ПК/кодек в комнате?",
            "Нужна ли запись/трансляция встреч или только онлайн-участие?",
        ]
    )

    out: list[str] = []
    seen: set[str] = set()
    for q in qs:
        if q not in seen:
            seen.add(q)
            out.append(q)
    return out


def _assumptions(spec: Any, requirements: ProjectRequirements, topology: TopologyDecision) -> list[str]:
    _ = requirements
    _ = topology
    vals: list[str] = []

    if requirements.room_type == "videowall":
        if _has_line(spec, "videowall_panel"):
            vals.append("Панели видеостены подбираются как профессиональные LCD-videowall панели под непрерывный режим работы.")
        if _has_line(spec, "videowall_mount"):
            vals.append("Для монтажа видеостены требуется специализированный каркас/крепление с юстировкой.")
        if _has_line(spec, "videowall_controller"):
            vals.append("Контроллер/процессор видеостены обязателен для управления раскладками и сценариями отображения.")
        if _has_line(spec, "matrix_switcher"):
            vals.append("Для нескольких источников сигнала закладывается матричная коммутация.")
        if _has_line(spec, "cabling_av"):
            vals.append("Коммутация и кабельная система учитываются как обязательная часть внедрения видеостены.")
        return vals

    if _find_first_by_family(spec, "delegate_unit") is not None:
        vals.append("Пульты делегатов требуются в количестве по числу мест делегатов.")

    if _has_line(spec, "chairman_unit"):
        vals.append(
            "Пульт председателя требуется в одном экземпляре и может отличаться по модели "
            "от пульта делегата."
        )

    if _has_line(spec, "discussion_central_unit"):
        vals.append(
            "Central unit (discussion_central_unit) обязателен из-за зависимостей "
            "delegate_unit -> discussion_central_unit и chairman_unit -> discussion_central_unit."
        )

    if _has_line(spec, "power_supply_discussion"):
        vals.append("При количестве делегатов >20 рекомендуется блок питания / расширения дискуссионной системы.")

    if _has_line(spec, "discussion_dsp"):
        vals.append("Интеграция со звуком может потребовать отдельного DSP (discussion_dsp).")

    return vals


def _risks(spec: Any, requirements: ProjectRequirements, topology: TopologyDecision) -> list[str]:
    _ = requirements
    _ = topology
    vals: list[str] = []

    if requirements.room_type == "videowall":
        if _find_first_placeholder(spec, "videowall_panel") is not None:
            vals.append("Без подтвержденных моделей панелей нельзя финализировать шов, яркость и тепловой режим видеостены.")
        if _find_first_placeholder(spec, "videowall_mount") is not None:
            vals.append("Без уточнения крепления/каркаса есть риск срыва монтажных сроков.")
        if _find_first_placeholder(spec, "videowall_controller") is not None:
            vals.append("Без контроллера/процессора видеостены невозможно гарантировать требуемые сценарии отображения.")
        if _find_first_placeholder(spec, "matrix_switcher") is not None:
            vals.append("Неуточненная матрица входов/выходов может привести к нехватке портов коммутации.")
        if _find_first_placeholder(spec, "cabling_av") is not None:
            vals.append("Недооценка кабельной системы может вызвать проблемы при пусконаладке.")

    if _find_first_placeholder(spec, "discussion_central_unit") is not None:
        vals.append("Без центрального блока система не будет работать.")

    if _find_first_placeholder(spec, "audio_lowz") is not None:
        vals.append("Без аудиовоспроизведения (колонок) участники не будут слышать друг друга.")

    if _find_first_placeholder(spec, "discussion_dsp") is not None:
        vals.append("Отсутствие DSP может ограничить возможности обработки звука.")

    if _find_first_by_family(spec, "power_supply_discussion") is not None:
        vals.append("Нехватка кабельной инфраструктуры и питания может привести к проблемам с развертыванием.")

    for line in _items(spec):
        if _line_price(line) is None:
            vals.append(f"[price_missing] Цена уточняется: {_line_name(line)}")

    out: list[str] = []
    seen: set[str] = set()
    for r in vals:
        if r not in seen:
            seen.add(r)
            out.append(r)
    return out


def build_fallback_explanations(
    *,
    spec: Any,
    requirements: ProjectRequirements,
    topology: TopologyDecision,
) -> Any:
    summary = _summary(spec, requirements, topology)
    selected = _selection_bullets(spec, requirements, topology)
    qty_price = _quantity_and_price_bullets(spec, requirements, topology)
    precedents = _precedents(spec)
    questions = _manager_questions(spec, requirements, topology)
    assumptions = _assumptions(spec, requirements, topology)
    risks = _risks(spec, requirements, topology)

    # Поля, которые реально читает exporter
    _safe_set(spec, "project_summary", summary)
    _safe_set(spec, "why_composition", selected)
    _safe_set(spec, "why_qty_and_price", qty_price)

    # Доп. совместимость
    _safe_set(spec, "brief_conclusion", summary)
    _safe_set(spec, "summary", summary)
    _safe_set(spec, "summary_text", summary)

    _safe_set(spec, "why_selected", selected)
    _safe_set(spec, "selection_rationale", selected)
    _safe_set(spec, "why_chosen", selected)

    _safe_set(spec, "why_quantities", qty_price)
    _safe_set(spec, "quantity_rationale", qty_price)
    _safe_set(spec, "price_rationale", qty_price)

    _safe_set(spec, "precedents", precedents)
    _safe_set(spec, "bitrix_precedents", precedents)
    _safe_set(spec, "precedent_task_ids", precedents)

    _safe_set(spec, "manager_questions", questions)
    _safe_set(spec, "clarifying_questions", questions)

    _safe_set(spec, "assumptions", assumptions)
    _safe_set(spec, "risks", risks)

    return spec