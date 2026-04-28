# Space Mission test
Solution to the Warp Space Mission Log Analysis hiring challenge.
## The Challenge
Parse a large, messy pipe-delimited log of space missions flown between
2030 and 2070 and find the **security code of the longest successful Mars
mission**.
Full instructions: [`docs/mission_challenge.md`](docs/mission_challenge.md)
Record format (8 fields, `|`-separated, with inconsistent whitespace):
```
Date | Mission ID | Destination | Status | Crew Size | Duration (days) | Success Rate | Security Code
```
The raw log is at [`data/space_missions.log`](data/space_missions.log)
(~10 MB, 105k lines, includes `#` comment lines and non-record junk that must
be skipped).
## Repository Layout
```
.
├── data/
│   └── space_missions.log        # raw pipe-delimited mission log
├── docs/
│   ├── challenge_overview.md     # original challenge README
│   └── mission_challenge.md      # full challenge instructions
├── models/
│   ├── staging/
│   │   └── stg_space_missions.sql      # cleans and types raw_space_missions
│   └── marts/
│       ├── mart_mission_performance.sql # outcomes & success metrics by destination
│       └── mart_destination_risk.sql    # composite risk score & rank by destination
├── scripts/
│   ├── solve_challenge.sh        # runs the awk solver + verifies the result
│   ├── load_data.py              # parses the log and loads raw_space_missions into DuckDB
│   └── build_models.py           # applies all SQL models to space_missions.db in layer order
├── requirements.txt              # Python dependencies (duckdb)
└── README.md
```
## Running the Solution
From the repo root:
```bash
./scripts/solve_challenge.sh
```
Optionally pass a different log path:
```bash
./scripts/solve_challenge.sh path/to/space_missions.log
```
Expected output:
```
Log file: /.../data/space_missions.log
Longest Completed Mars mission
  Date:          2065-06-05
  Mission ID:    WGU-0200
  Duration:      1629 days
  Security Code: XRT-421-ZQP
Verification: OK
ANSWER: XRT-421-ZQP
```
Exit codes:
- `0` — success
- `1` — log file not found
- `2` — no Mars/Completed mission found
- `3` — output didn't match the `ABC-123-XYZ` format
- `4` — cross-check against the log failed
## How the Solution Works
The core of the script is a single `awk` pass over the log:
```bash
awk -F'|' '
  /^#/       { next }          # skip comment lines
  NF != 8    { next }          # skip anything that isnt a full record
  {
    for (i = 1; i <= NF; i++)  # trim leading/trailing whitespace
      gsub(/^[ \t]+|[ \t]+$/, "", $i)
    if ($3 == "Mars" && $4 == "Completed" && $6 + 0 > max) {
      max  = $6 + 0            # track the biggest Duration seen
      code = $8                # remember its Security Code
    }
  }
  END { print code }
' data/space_missions.log
```
Step by step:
1. `-F'|'` splits each line on `|`, exposing `$1`…`$8`.
2. `/^#/ { next }` drops comment lines.
3. `NF != 8 { next }` drops header/junk/blank lines that aren't full records.
4. The `for`/`gsub` loop trims whitespace from every field to handle the
   log's sloppy spacing (e.g. `|  Mars   |`).
5. The filter keeps only `Destination == "Mars"` and `Status == "Completed"`.
6. `$6 + 0` forces numeric comparison of Duration (otherwise `"387" > "1629"`
   lexicographically).
7. The max Duration and its Security Code are tracked in a single pass.
8. `END { print code }` emits the answer.
## Verification
`scripts/solve_challenge.sh` does three checks after running the solver:
1. **Non-empty** — a matching mission must exist.
2. **Format** — the code must match `^[A-Z]{3}-[0-9]{3}-[A-Z]{3}$`.
3. **Cross-check** — a second `awk` pass confirms the winning Mission ID +
   Security Code actually appear on a Mars/Completed row in the log.
## Answer
**`XRT-421-ZQP`** — Mission `WGU-0200`, launched `2065-06-05`, 1629-day
Completed Mars mission.
---
## Analytics Engineering Pipeline
Beyond the challenge solver, this repo contains a lightweight analytics
engineering pipeline that ingests the full 100k-mission log into a local
[DuckDB](https://duckdb.org/) database and exposes clean, queryable models.
### Setup
```bash
# Create and activate the virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```
### Loading the raw data
```bash
python scripts/load_data.py
```
Parses `data/space_missions.log`, skips comment/metadata lines, and bulk-loads
100,000 records into the `raw_space_missions` table inside `space_missions.db`.
### Building the models
```bash
python scripts/build_models.py
```
Applies all SQL models under `models/` in layer order (staging → marts),
creating or replacing each table/view in `space_missions.db`.
### Data layers
#### Staging — `stg_space_missions` (view)
A 1-to-1 cleaning layer over `raw_space_missions`. No business logic.
| Transformation | Detail |
|---|---|
| Type casting | `date` VARCHAR → `mission_date` DATE |
| Renamed columns | `date` → `mission_date`, `success_rate` → `success_rate_pct` |
| String trimming | All VARCHAR columns trimmed |
| `is_crewed` | `true` when `crew_size > 0` |
| `is_duplicate_mission_id` | Flags the 25 `mission_id` values that appear more than once |
#### Marts
All mart tables exclude duplicate `mission_id` rows and use **settled missions
only** (Completed, Partial Success, Failed, Aborted) as the denominator for
rate calculations — Planned and In Progress missions have not yet concluded.
**`mart_mission_performance`**
Grain: one row per destination. Aggregates mission volume, outcome counts,
completion/failure rates, crew profile, success score statistics, and the
full date range of activity.
Key columns: `total_missions`, `settled_missions`, `completed_missions`,
`failed_missions`, `aborted_missions`, `completion_rate_pct`,
`failure_rate_pct`, `avg_success_rate_pct`, `avg_duration_days`,
`avg_crew_size`, `first_mission_date`, `last_mission_date`.
**`mart_destination_risk`**
Grain: one row per destination. Scores and ranks each destination using a
composite risk score built from three min-max normalised components:
| Component | Weight | Rationale |
|---|---|---|
| `failure_rate` | 50% | Share of settled missions that Failed or Aborted |
| `avg_duration_days` | 30% | Longer missions carry more sustained exposure |
| `avg_crew_size` | 20% | Larger crews mean more personnel at risk |
The normalised components (`failure_rate_norm`, `duration_norm`,
`crew_size_norm`) are exposed alongside the final `risk_score` and `risk_rank`
for full auditability. `risk_rank = 1` is the highest-risk destination.
### Querying with the DuckDB CLI
```bash
# Install (macOS)
brew install duckdb

# Open an interactive session
duckdb space_missions.db

# Example queries
SELECT destination, risk_rank, risk_score FROM mart_destination_risk ORDER BY risk_rank;
SELECT destination, completion_rate_pct, failure_rate_pct FROM mart_mission_performance ORDER BY failure_rate_pct DESC;
```
