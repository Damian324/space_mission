import csv
import duckdb
import re
import tempfile
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
LOG_FILE = BASE_DIR / "data" / "space_missions.log"
DB_FILE  = BASE_DIR / "space_missions.db"

SKIP_PREFIXES = ("#", "SYSTEM:", "CONFIG:", "CHECKSUM:", "CHECKPOINT:")
COLUMNS = ["date", "mission_id", "destination", "status",
            "crew_size", "duration_days", "success_rate", "security_code"]


def parse_log(path: Path) -> list[tuple]:
    records = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or any(line.startswith(p) for p in SKIP_PREFIXES):
                continue
            parts = [p.strip() for p in line.split("|")]
            if len(parts) != 8:
                continue
            if not re.match(r"^\d{4}-\d{2}-\d{2}$", parts[0]):
                continue
            date, mission_id, destination, status, crew_size, duration, success_rate, security_code = parts
            try:
                records.append((
                    date, mission_id, destination, status,
                    int(crew_size), int(duration), float(success_rate), security_code,
                ))
            except ValueError as e:
                print(f"Skipping malformed row: {parts} — {e}")
    return records


def main():
    records = parse_log(LOG_FILE)
    print(f"Parsed {len(records)} records from {LOG_FILE.name}")

    # Write to a temp CSV so DuckDB can bulk-load it
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, newline=""
    ) as tmp:
        writer = csv.writer(tmp)
        writer.writerow(COLUMNS)
        writer.writerows(records)
        tmp_path = tmp.name

    con = duckdb.connect(str(DB_FILE))
    con.execute(f"""
        CREATE OR REPLACE TABLE raw_space_missions AS
        SELECT
            date::VARCHAR            AS date,
            mission_id::VARCHAR      AS mission_id,
            destination::VARCHAR     AS destination,
            status::VARCHAR          AS status,
            crew_size::INTEGER       AS crew_size,
            duration_days::INTEGER   AS duration_days,
            success_rate::DOUBLE     AS success_rate,
            security_code::VARCHAR   AS security_code
        FROM read_csv_auto('{tmp_path}', header=true)
    """)

    count = con.execute("SELECT COUNT(*) FROM raw_space_missions").fetchone()[0]
    print(f"Loaded {count} rows into raw_space_missions in {DB_FILE.name}")
    con.close()


if __name__ == "__main__":
    main()
