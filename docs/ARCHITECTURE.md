# Architecture

## Core flow
1. Request / transcript
2. Requirements extraction
3. Knowledge-map role expansion
4. Retrieval / candidate pool
5. LLM planning over candidates
6. Deterministic editor
7. Validator
8. Export

## Boundary
- LLM chooses among candidates and explains reasoning.
- Python enforces structure, dedupe, replace-vs-add semantics, quantity sanity and export stability.
