# KB Playbook: categories / families / dependencies (эксплуатационная заточка)

Цель: чтобы исправления работали "вперёд" на будущие запросы, без точечных патчей под каждый кейс.

## Где живёт KB

- `src/mvp_sksp/knowledge/ontology/families.yaml` — базовая онтология families.
- `src/mvp_sksp/knowledge/ontology/families_kb.yaml` — *малые overrides*, которые вы правите по итогам эксплуатации.
- `src/mvp_sksp/knowledge/ontology/dependency_rules.yaml` — requires_any / requires_all / incompatible_by_family.
- `src/mvp_sksp/knowledge/ontology/quantity_rules.yaml` — qty правила.
- `src/mvp_sksp/knowledge/ontology/triage_rules.yaml` — конфиг детекторов triage (ключевые слова/семейства).

## Правило эксплуатации

1) Не лечим повторяемую проблему в коде. Сначала лечим в KB (`families_kb.yaml`, `triage_rules.yaml`, dependency/quantity rules).
2) Каждую фиксу сопровождаем регресс-тестом (минимальный тест на triage/Step10-инварианты).
3) Если уверенность/совместимость сомнительна — лучше placeholder+warning, чем "левый" кандидат.

## Как фиксить ложноположительные family

- Добавь `signature.must_not` для family, куда ошибочно попадает кандидат.
- Добавь `videowall_controller.signature.strong_keywords` (или другой целевой family), чтобы оно выигрывалось.

Пример: видеостенный контроллер ошибочно попадает в BYOD:
- `byod_*: must_not: ["видеостен", "1x4", ...]`
- `videowall_controller: strong_keywords: ["контроллер видеостены", "1x4", ...]`

## Как фиксить несовместимость 100V

- Triage ловит 100V keywords в описании акустики и проверяет усилитель на наличие 100V keywords.
- Если часто встречается, дальше делаем шаг: разделяем family `amplifier_100v` / `amplifier_low_ohm` или вводим tag `audio_power`.

## Команды

```bash
source .venv/bin/activate
pip install -e ".[dev]"
pytest -q tests/test_triage.py