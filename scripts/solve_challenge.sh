#!/usr/bin/env bash
#
# solve_challenge.sh
# ------------------
# Finds the security code of the longest "Completed" Mars mission in
# data/space_missions.log and verifies the result.
#
# Usage:
#   ./scripts/solve_challenge.sh [path/to/space_missions.log]
#
# Exit codes:
#   0 - success, a valid security code was found
#   1 - log file missing
#   2 - no matching mission found
#   3 - output did not match the expected ABC-123-XYZ format
#   4 - sanity check against the log failed

set -euo pipefail

# ---------------------------------------------------------------------------
# Resolve paths
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_FILE="${1:-$REPO_ROOT/data/space_missions.log}"

if [[ ! -f "$LOG_FILE" ]]; then
  echo "ERROR: log file not found: $LOG_FILE" >&2
  exit 1
fi

echo "Log file: $LOG_FILE"
echo

# ---------------------------------------------------------------------------
# Run the awk solver
#
# The program reads the pipe-delimited log, discards comments and malformed
# rows, trims whitespace around every field, and tracks the largest Duration
# among missions with Destination=Mars and Status=Completed. At EOF it prints
# the security code, the duration, the date, and the mission ID.
# ---------------------------------------------------------------------------
read -r CODE DURATION DATE MISSION_ID < <(
  awk -F'|' '
    /^#/       { next }          # skip comment lines
    NF != 8    { next }          # skip anything that isnt a full record
    {
      for (i = 1; i <= NF; i++)  # trim leading/trailing whitespace
        gsub(/^[ \t]+|[ \t]+$/, "", $i)

      if ($3 == "Mars" && $4 == "Completed" && $6 + 0 > max) {
        max      = $6 + 0
        code     = $8
        date     = $1
        mission  = $2
      }
    }
    END { print code, max, date, mission }
  ' "$LOG_FILE"
)

# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------
if [[ -z "${CODE:-}" ]]; then
  echo "ERROR: no Mars + Completed mission found in log" >&2
  exit 2
fi

if [[ ! "$CODE" =~ ^[A-Z]{3}-[0-9]{3}-[A-Z]{3}$ ]]; then
  echo "ERROR: security code '$CODE' does not match expected format ABC-123-XYZ" >&2
  exit 3
fi

# Cross-check: make sure a line actually exists in the log with this exact
# mission id + security code, and that it is a Completed Mars mission.
if ! awk -F'|' -v id="$MISSION_ID" -v code="$CODE" '
  /^#/ || NF != 8 { next }
  { for (i=1;i<=NF;i++) gsub(/^[ \t]+|[ \t]+$/, "", $i) }
  $2 == id && $8 == code && $3 == "Mars" && $4 == "Completed" { found=1; exit }
  END { exit !found }
' "$LOG_FILE"; then
  echo "ERROR: could not verify $MISSION_ID / $CODE against the log" >&2
  exit 4
fi

# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------
echo "Longest Completed Mars mission"
echo "  Date:          $DATE"
echo "  Mission ID:    $MISSION_ID"
echo "  Duration:      $DURATION days"
echo "  Security Code: $CODE"
echo
echo "Verification: OK"
echo
echo "ANSWER: $CODE"
