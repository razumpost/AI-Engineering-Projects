# mvp-sksp

Refactored MVP scaffold for building SKSP / commercial equipment specifications from:
- user request or call transcript,
- precedent tasks / Bitrix history,
- graph / relational retrieval,
- deterministic editing and validation.

## Current architecture

```text
src/mvp_sksp/
  adapters/      # external read wrappers: Bitrix links, retrieval wrappers
  domain/        # pydantic models for spec, ops, candidates, llm contract
  editing/       # deterministic spec editing and patch semantics
  knowledge/     # ontology / dependency graph / quantity rules
  llm/           # Yandex FM client + prompts
  persistence/   # snapshots / last_valid rollback
  pipeline/      # retrieval / orchestrator / export / autofill
  planning/      # requirements parsing + role expansion (V1)
  validation/    # integrity / dedupe / qty sanity
scripts/
  mvp_sksp_cli.py
  bootstrap_dev.py
  smoke_test.py
vendor/
  legacy_vectorbdragcollector/  # archived legacy code, not part of installed package
```

## Why this repo is "adult" enough to grow

- Runtime package contains only product code in `src/mvp_sksp/`
- Recovery / one-off patch shell scripts were removed
- Local IDE files, logs, backups, databases and secrets were removed
- Legacy code is preserved under `vendor/` but isolated from the installed package
- Knowledge map and planning layer now exist as first-class modules

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
python scripts/bootstrap_dev.py
python scripts/smoke_test.py
python scripts/mvp_sksp_cli.py --request "переговорная на 12 мест под ВКС" --interactive
```

## Environment

Copy `.env.example` to `.env` and fill:
- PostgreSQL DSN
- Bitrix webhook/base URL
- Yandex AI Studio folder/API key/model URI
- Cognee/Kuzu system root

## Development direction

1. `planning.requirements` should become the single source of truth for normalized project intent.
2. `knowledge/ontology/*.yaml` should grow incrementally during demo validation.
3. `pipeline.autofill` should gradually be replaced by role coverage planning over the knowledge map.
4. `validation` should absorb required-kit, compatibility and commercial sanity checks.

## Note on `vendor/`

`vendor/legacy_vectorbdragcollector/` is kept only as a migration reference.
New code should not import it directly from production paths.
