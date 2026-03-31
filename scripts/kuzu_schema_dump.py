from __future__ import annotations

import os
from pathlib import Path

import kuzu
from dotenv import load_dotenv  # type: ignore


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def main() -> int:
    load_dotenv(dotenv_path=str(_repo_root() / ".env"), override=False)

    explicit = (os.getenv("KUZU_DB_PATH") or "").strip()
    if explicit:
        db_path = Path(explicit).expanduser().resolve()
    else:
        sys_root = (os.getenv("SYSTEM_ROOT_DIRECTORY") or "").strip()
        if not sys_root:
            raise SystemExit("SYSTEM_ROOT_DIRECTORY is not set and KUZU_DB_PATH is empty")
        db_path = (Path(sys_root) / "databases" / "cognee_graph_kuzu").resolve()

    print("kuzu_db_path:", db_path)
    db_path.mkdir(parents=True, exist_ok=True)

    db = kuzu.Database(str(db_path))
    conn = kuzu.Connection(db)

    # show_tables requires RETURN * in your version
    res = conn.execute("CALL show_tables() RETURN *")
    print("\n=== CALL show_tables() RETURN * ===")
    print("cols:", res.get_column_names())
    while res.has_next():
        print(res.get_next())

    # top node types
    try:
        res = conn.execute("MATCH (n:Node) RETURN n.type AS type, count(*) AS cnt ORDER BY cnt DESC LIMIT 12")
        print("\n=== top Node.type ===")
        while res.has_next():
            print(res.get_next())
    except Exception as e:
        print("[fail] type summary ->", type(e).__name__, e)

    # relationships
    try:
        res = conn.execute("MATCH ()-[r]->() RETURN LABEL(r) AS rel, count(*) AS cnt ORDER BY cnt DESC LIMIT 12")
        print("\n=== top relationships (by LABEL(r)) ===")
        while res.has_next():
            print(res.get_next())
    except Exception as e:
        print("[info] relationship scan not available ->", type(e).__name__, e)

    # sample items
    try:
        res = conn.execute("MATCH (i:Node) WHERE i.type='Item' RETURN i.id AS id, left(i.properties, 800) AS props LIMIT 3")
        print("\n=== sample Item.properties ===")
        while res.has_next():
            print(res.get_next())
    except Exception as e:
        print("[fail] sample items ->", type(e).__name__, e)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())