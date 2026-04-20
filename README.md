# Space Mission
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
├── scripts/
│   └── solve_challenge.sh        # runs the awk solver + verifies the result
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
