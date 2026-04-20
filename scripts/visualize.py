#!/usr/bin/env python3
"""
Generate two professional analytics charts from space_missions.db.

Outputs
-------
destination_risk.png    — Horizontal bar chart ranked by composite risk score
mission_performance.png — Stacked bar chart of mission outcomes by destination
"""
import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.patches as mpatches
import numpy as np
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parents[1]
DB_FILE  = BASE_DIR / "space_missions.db"

# ── Global style ──────────────────────────────────────────────────────────
plt.rcParams.update({
    "font.family":        "sans-serif",
    "axes.spines.top":    False,
    "axes.spines.right":  False,
    "axes.grid":          True,
    "grid.color":         "#e8e8e8",
    "grid.linewidth":     0.6,
    "axes.axisbelow":     True,
    "figure.facecolor":   "white",
})

RISK_CMAP = plt.cm.RdYlGn_r      # red = high risk, green = low risk

OUTCOME_COLORS = {
    "completed":       "#2a9d8f",   # teal
    "partial_success": "#e9c46a",   # amber
    "failed":          "#e76f51",   # coral
    "aborted":         "#8d99ae",   # slate
}


# ── Data loading ──────────────────────────────────────────────────────────
def load_data() -> tuple:
    con = duckdb.connect(str(DB_FILE), read_only=True)

    risk_rows = con.execute("""
        SELECT
            destination,
            risk_score,
            risk_rank,
            failure_rate,
            avg_duration_days,
            avg_crew_size,
            failure_rate_norm,
            duration_norm,
            crew_size_norm
        FROM mart_destination_risk
        ORDER BY risk_score ASC     -- lowest first so highest sits at top of barh
    """).fetchall()

    perf_rows = con.execute("""
        SELECT
            destination,
            total_missions,
            settled_missions,
            completed_missions,
            partial_success_missions,
            failed_missions,
            aborted_missions,
            completion_rate_pct,
            failure_rate_pct
        FROM mart_mission_performance
        ORDER BY total_missions DESC
    """).fetchall()

    con.close()
    return risk_rows, perf_rows


# ── Chart 1: Destination Risk ─────────────────────────────────────────────
def chart_destination_risk(rows: list, save_path: Path) -> None:

    destinations  = [r[0] for r in rows]
    risk_scores   = [float(r[1]) for r in rows]
    risk_ranks    = [int(r[2])   for r in rows]
    failure_rates = [float(r[3]) for r in rows]
    avg_durations = [float(r[4]) for r in rows]
    avg_crews     = [float(r[5]) for r in rows]
    fn_norms      = [float(r[6]) for r in rows]
    dur_norms     = [float(r[7]) for r in rows]
    crew_norms    = [float(r[8]) for r in rows]

    n     = len(destinations)
    y_pos = np.arange(n)
    norm  = mcolors.Normalize(vmin=min(risk_scores), vmax=max(risk_scores))
    colors = [RISK_CMAP(norm(s)) for s in risk_scores]

    # ── Figure layout ──
    fig = plt.figure(figsize=(13, 17), facecolor="white")
    gs  = fig.add_gridspec(2, 1, height_ratios=[3.4, 1], hspace=0.08)
    ax      = fig.add_subplot(gs[0])
    ax_txt  = fig.add_subplot(gs[1])

    # ── Bars ──
    bars = ax.barh(
        y_pos, risk_scores,
        color=colors, height=0.62,
        edgecolor="white", linewidth=0.8,
    )

    # Score + rank annotations beside each bar
    for bar, score, rank in zip(bars, risk_scores, risk_ranks):
        ax.text(
            score + 0.013,
            bar.get_y() + bar.get_height() / 2,
            f"{score:.3f}   (rank #{rank})",
            va="center", ha="left", fontsize=9.5, color="#333333",
        )

    ax.set_yticks(y_pos)
    ax.set_yticklabels(destinations, fontsize=12)
    ax.set_xlabel(
        "Composite Risk Score   (failure rate 50%  ·  avg duration 30%  ·  avg crew size 20%)",
        fontsize=10, labelpad=10,
    )
    ax.set_xlim(0, 1.22)
    ax.set_title(
        "Space Mission Destination Risk Rankings",
        fontsize=18, fontweight="bold", pad=16,
    )
    ax.tick_params(axis="y", length=0)
    ax.spines["left"].set_visible(False)
    ax.grid(axis="x")

    # Vertical divider at score = 0.5 to indicate high/low risk boundary
    ax.axvline(0.5, color="#aaaaaa", linewidth=0.9, linestyle="--")
    ax.text(0.502, n - 0.4, "high risk threshold",
            fontsize=8, color="#888888", va="top")

    # Colourbar legend
    sm = plt.cm.ScalarMappable(cmap=RISK_CMAP, norm=norm)
    sm.set_array([])
    cbar = fig.colorbar(sm, ax=ax, orientation="vertical",
                        fraction=0.018, pad=0.02, shrink=0.82)
    cbar.set_label("Risk Level", fontsize=9)
    cbar.ax.set_yticks([norm.vmin, (norm.vmin + norm.vmax) / 2, norm.vmax])
    cbar.ax.set_yticklabels(["Low", "Medium", "High"], fontsize=8.5)

    # ── Derive insights ──
    # rows are sorted ASC so index -1 = highest risk, index 0 = lowest risk
    top3   = rows[-3:][::-1]   # [rank1, rank2, rank3]
    safe2  = rows[:2]          # [rank17, rank16]

    def primary_driver(fn, dn, cn) -> str:
        weighted = {
            "failure rate":   fn * 0.50,
            "mission duration": dn * 0.30,
            "crew size":        cn * 0.20,
        }
        return max(weighted, key=weighted.get)

    r1_driver = primary_driver(float(top3[0][6]), float(top3[0][7]), float(top3[0][8]))
    r2_driver = primary_driver(float(top3[1][6]), float(top3[1][7]), float(top3[1][8]))
    r3_driver = primary_driver(float(top3[2][6]), float(top3[2][7]), float(top3[2][8]))

    insights = (
        "KEY INSIGHTS & RECOMMENDATIONS\n\n"
        f"  Highest-risk:   {top3[0][0]} (score {float(top3[0][1]):.3f}, rank #1)  ·  "
        f"{top3[1][0]} (score {float(top3[1][1]):.3f}, rank #2)  ·  "
        f"{top3[2][0]} (score {float(top3[2][1]):.3f}, rank #3)\n"
        f"  Lowest-risk:    {safe2[0][0]} (score {float(safe2[0][1]):.3f})  ·  "
        f"{safe2[1][0]} (score {float(safe2[1][1]):.3f})\n\n"
        f"  • {top3[0][0]} is the highest-risk destination, driven by {r1_driver} "
        f"(failure rate {float(top3[0][3]):.1%}). Avoid committing crewed missions until\n"
        f"    reliability improves — consider unmanned probes to gather operational data first.\n"
        f"  • {top3[1][0]} and {top3[2][0]} risk profiles are shaped by {r2_driver} and "
        f"{r3_driver} respectively. Shorter-duration missions or smaller crew\n"
        f"    rotations would meaningfully reduce their composite scores.\n"
        f"  • {safe2[0][0]} and {safe2[1][0]} present the lowest operational risk and are the recommended\n"
        f"    destinations for initial crewed assignments or crew certification missions.\n"
        f"  • Improving mission reliability (failure rate carries 50% of the score weight) has the greatest "
        f"lever for reducing risk across all high-risk destinations."
    )

    ax_txt.axis("off")
    ax_txt.text(
        0.01, 0.97, insights,
        transform=ax_txt.transAxes,
        va="top", ha="left", fontsize=9.8, linespacing=1.75,
        bbox=dict(facecolor="#f5f5f5", edgecolor="#cccccc",
                  boxstyle="round,pad=0.9", linewidth=0.8),
    )

    fig.savefig(save_path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"  ✓  {save_path.name}")


# ── Chart 2: Mission Performance ──────────────────────────────────────────
def chart_mission_performance(rows: list, save_path: Path) -> None:

    destinations    = [r[0] for r in rows]
    total_missions  = [int(r[1])   for r in rows]
    settled         = [int(r[2])   for r in rows]
    completed       = [int(r[3])   for r in rows]
    partial         = [int(r[4])   for r in rows]
    failed          = [int(r[5])   for r in rows]
    aborted         = [int(r[6])   for r in rows]
    comp_rates      = [float(r[7]) if r[7] is not None else 0.0 for r in rows]
    fail_rates      = [float(r[8]) if r[8] is not None else 0.0 for r in rows]

    n     = len(destinations)
    x_pos = np.arange(n)
    bar_w = 0.60

    # Cumulative bottoms for stacking
    bottom_partial  = completed
    bottom_failed   = [c + p for c, p in zip(completed, partial)]
    bottom_aborted  = [c + p + f for c, p, f in zip(completed, partial, failed)]

    # ── Figure layout ──
    fig = plt.figure(figsize=(16, 14), facecolor="white")
    gs  = fig.add_gridspec(2, 1, height_ratios=[3, 1], hspace=0.08)
    ax      = fig.add_subplot(gs[0])
    ax_txt  = fig.add_subplot(gs[1])

    # ── Stacked bars ──
    ax.bar(x_pos, completed, bar_w,
           label="Completed",       color=OUTCOME_COLORS["completed"])
    ax.bar(x_pos, partial, bar_w, bottom=bottom_partial,
           label="Partial Success", color=OUTCOME_COLORS["partial_success"])
    ax.bar(x_pos, failed, bar_w, bottom=bottom_failed,
           label="Failed",          color=OUTCOME_COLORS["failed"])
    ax.bar(x_pos, aborted, bar_w, bottom=bottom_aborted,
           label="Aborted",         color=OUTCOME_COLORS["aborted"])

    # Completion rate % label above each bar
    for i, (total, cr) in enumerate(zip(total_missions, comp_rates)):
        ax.text(
            i, total + 60,
            f"{cr:.1f}%",
            ha="center", va="bottom",
            fontsize=8.5, color="#444444", fontweight="semibold",
        )

    ax.set_xticks(x_pos)
    ax.set_xticklabels(destinations, rotation=32, ha="right", fontsize=10.5)
    ax.set_ylabel("Number of Missions", fontsize=11, labelpad=8)
    ax.set_title(
        "Mission Outcomes by Destination",
        fontsize=18, fontweight="bold", pad=16,
    )
    ax.set_ylim(0, max(total_missions) * 1.09)
    ax.tick_params(axis="x", length=0)
    ax.spines["bottom"].set_visible(False)
    ax.grid(axis="y")

    # Annotation: percentage labels are completion rate of settled missions
    ax.text(
        0.99, 0.97,
        "Labels show completion rate\n(% of settled missions)",
        transform=ax.transAxes, ha="right", va="top",
        fontsize=8.5, color="#666666",
        bbox=dict(facecolor="white", edgecolor="#dddddd",
                  boxstyle="round,pad=0.4", linewidth=0.7),
    )

    ax.legend(
        loc="upper left", frameon=True, framealpha=0.95,
        fontsize=10.5, edgecolor="#cccccc", ncol=4,
    )

    # ── Derive insights ──
    best_comp  = max(zip(comp_rates, destinations), key=lambda x: x[0])
    worst_comp = min(zip(comp_rates, destinations), key=lambda x: x[0])
    worst_fail = max(zip(fail_rates, destinations), key=lambda x: x[0])
    best_fail  = min(zip(fail_rates, destinations), key=lambda x: x[0])

    total_completed_all  = sum(completed)
    total_settled_all    = sum(settled)
    total_missions_all   = sum(total_missions)
    overall_comp_rate    = total_completed_all / total_settled_all * 100
    overall_fail_rate    = sum(f + a for f, a in zip(failed, aborted)) / total_settled_all * 100

    spread_comp = max(comp_rates) - min(comp_rates)

    insights = (
        "KEY INSIGHTS & RECOMMENDATIONS\n\n"
        f"  Fleet-wide (settled missions):   Completion rate {overall_comp_rate:.1f}%   ·   "
        f"Failure + Abort rate {overall_fail_rate:.1f}%   ·   "
        f"{total_completed_all:,} completed out of {total_missions_all:,} total missions\n\n"
        f"  • Outcome rates are strikingly uniform across all 17 destinations — completion rates span only "
        f"{spread_comp:.1f} percentage points ({worst_comp[0]:.1f}% – {best_comp[0]:.1f}%),\n"
        f"    suggesting mission success is driven by fleet-wide factors (technology, crew training) rather "
        f"than destination-specific conditions.\n"
        f"  • {best_comp[1]} achieves the highest completion rate ({best_comp[0]:.1f}%) and "
        f"{best_fail[1]} has the lowest failure+abort rate ({best_fail[0]:.1f}%) — "
        f"both are solid choices\n    for high-priority crewed assignments.\n"
        f"  • {worst_fail[1]} has the highest failure+abort rate ({worst_fail[0]:.1f}%). Combined with its "
        f"#1 risk ranking, this destination warrants a structured\n"
        f"    reliability review before further crewed mission allocations.\n"
        f"  • With ~{total_missions_all // n:,} missions per destination, investment is evenly spread — "
        f"consider reallocating budget from high-risk to high-performing destinations."
    )

    ax_txt.axis("off")
    ax_txt.text(
        0.01, 0.97, insights,
        transform=ax_txt.transAxes,
        va="top", ha="left", fontsize=9.8, linespacing=1.75,
        bbox=dict(facecolor="#f5f5f5", edgecolor="#cccccc",
                  boxstyle="round,pad=0.9", linewidth=0.8),
    )

    fig.savefig(save_path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"  ✓  {save_path.name}")


# ── Entry point ───────────────────────────────────────────────────────────
def main() -> None:
    print(f"Loading data from {DB_FILE.name}...")
    risk_rows, perf_rows = load_data()

    print("Generating charts...")
    chart_destination_risk(risk_rows,    BASE_DIR / "destination_risk.png")
    chart_mission_performance(perf_rows, BASE_DIR / "mission_performance.png")
    print("\nDone.")


if __name__ == "__main__":
    main()
