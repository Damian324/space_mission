"""
Applies all SQL model files under models/ to space_missions.db.
Execution order: staging → (future layers)
"""
import duckdb
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DB_FILE = BASE_DIR / "space_missions.db"

MODEL_LAYERS = [
    "staging",
    # "intermediate",  # add future layers here in order
    "marts",
]


def run_model(con: duckdb.DuckDBPyConnection, path: Path) -> None:
    sql = path.read_text(encoding="utf-8")
    con.execute(sql)
    print(f"  ✓ {path.relative_to(BASE_DIR)}")


def main() -> None:
    con = duckdb.connect(str(DB_FILE))
    print(f"Connected to {DB_FILE.name}\n")

    for layer in MODEL_LAYERS:
        layer_dir = BASE_DIR / "models" / layer
        sql_files = sorted(layer_dir.glob("*.sql"))
        if not sql_files:
            print(f"[{layer}] no models found, skipping.")
            continue
        print(f"[{layer}]")
        for sql_file in sql_files:
            run_model(con, sql_file)

    con.close()
    print("\nAll models built successfully.")


if __name__ == "__main__":
    main()
