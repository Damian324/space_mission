-- mart_destination_risk
-- Grain: one row per destination.
-- Source: stg_space_missions (duplicates excluded).
--
-- Scores and ranks each destination using a composite risk score derived
-- from three normalised components:
--
--   failure_rate      (weight: 50%) — share of settled missions that Failed or Aborted
--   avg_duration_days (weight: 30%) — longer missions carry more sustained exposure
--   avg_crew_size     (weight: 20%) — larger crews mean more personnel at risk
--
-- Each component is min-max normalised to [0, 1] before weighting so that
-- differences in scale do not distort the final score.
-- risk_rank = 1 is the highest-risk destination.

create or replace table mart_destination_risk as

with

base_metrics as (

    -- Aggregate the raw inputs needed for risk scoring.
    -- Failure rate is computed against settled missions only to avoid
    -- diluting the signal with missions that have not yet concluded.

    select
        destination,

        count(*)                                                        as total_missions,

        sum(case when status in ('Completed', 'Partial Success',
                                 'Failed', 'Aborted') then 1 else 0 end) as settled_missions,

        sum(case when status in ('Failed',
                                 'Aborted')           then 1 else 0 end) as unsuccessful_missions,

        round(
            sum(case when status in ('Failed', 'Aborted') then 1 else 0 end) * 1.0
            / nullif(
                sum(case when status in ('Completed', 'Partial Success',
                                         'Failed', 'Aborted') then 1 else 0 end),
              0),
        4)                                                              as failure_rate,

        round(avg(duration_days), 2)                                    as avg_duration_days,
        round(avg(crew_size), 2)                                        as avg_crew_size

    from stg_space_missions
    where not is_duplicate_mission_id
    group by destination

),

normalised as (

    -- Min-max normalise each risk component to [0, 1].
    -- A value of 1.0 means the destination scored highest on that factor.

    select
        destination,
        total_missions,
        settled_missions,
        unsuccessful_missions,
        failure_rate,
        avg_duration_days,
        avg_crew_size,

        round(
            (failure_rate - min(failure_rate) over ())
            / nullif(max(failure_rate) over () - min(failure_rate) over (), 0),
        4)                                                              as failure_rate_norm,

        round(
            (avg_duration_days - min(avg_duration_days) over ())
            / nullif(max(avg_duration_days) over () - min(avg_duration_days) over (), 0),
        4)                                                              as duration_norm,

        round(
            (avg_crew_size - min(avg_crew_size) over ())
            / nullif(max(avg_crew_size) over () - min(avg_crew_size) over (), 0),
        4)                                                              as crew_size_norm

    from base_metrics

),

scored as (

    -- Apply weights and compute the composite risk score.

    select
        destination,
        total_missions,
        settled_missions,
        unsuccessful_missions,
        failure_rate,
        avg_duration_days,
        avg_crew_size,
        failure_rate_norm,
        duration_norm,
        crew_size_norm,

        round(
            (0.50 * failure_rate_norm)
            + (0.30 * duration_norm)
            + (0.20 * crew_size_norm),
        4)                                                              as risk_score

    from normalised

)

select
    destination,

    -- Risk output
    round(risk_score, 4)                                                as risk_score,
    rank() over (order by risk_score desc)                              as risk_rank,

    -- Raw inputs for transparency
    failure_rate,
    avg_duration_days,
    avg_crew_size,

    -- Normalised components for auditability
    failure_rate_norm,
    duration_norm,
    crew_size_norm,

    -- Volume context
    total_missions,
    settled_missions,
    unsuccessful_missions

from scored
order by risk_rank
