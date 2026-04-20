-- mart_mission_performance
-- Grain: one row per destination.
-- Source: stg_space_missions (duplicates excluded).
--
-- Aggregates mission volume, outcome counts, success metrics,
-- and mission profile by destination.

create or replace table mart_mission_performance as

with

mission_flags as (

    -- Classify each mission with boolean outcome flags to keep
    -- downstream aggregations clean and free of inline conditionals.

    select
        destination,
        mission_id,
        mission_date,
        status,
        success_rate_pct,
        duration_days,
        crew_size,
        is_crewed,

        status = 'Completed'                        as is_completed,
        status = 'Partial Success'                  as is_partial_success,
        status = 'Failed'                           as is_failed,
        status = 'Aborted'                          as is_aborted,
        status = 'In Progress'                      as is_in_progress,
        status = 'Planned'                          as is_planned,

        -- A mission is "settled" once it has a terminal outcome.
        -- Planned and In Progress are excluded from rate calculations.
        status in ('Completed', 'Partial Success',
                   'Failed', 'Aborted')             as is_settled

    from stg_space_missions
    where not is_duplicate_mission_id

),

aggregated as (

    select
        destination,

        -- Volume
        count(*)                                                as total_missions,
        sum(case when is_completed       then 1 else 0 end)    as completed_missions,
        sum(case when is_partial_success then 1 else 0 end)    as partial_success_missions,
        sum(case when is_failed          then 1 else 0 end)    as failed_missions,
        sum(case when is_aborted         then 1 else 0 end)    as aborted_missions,
        sum(case when is_in_progress     then 1 else 0 end)    as in_progress_missions,
        sum(case when is_planned         then 1 else 0 end)    as planned_missions,
        sum(case when is_settled         then 1 else 0 end)    as settled_missions,

        -- Crew profile
        sum(case when is_crewed          then 1 else 0 end)    as crewed_missions,
        sum(case when not is_crewed      then 1 else 0 end)    as uncrewed_missions,
        round(avg(crew_size), 1)                               as avg_crew_size,

        -- Duration
        round(avg(duration_days), 1)                           as avg_duration_days,

        -- Success score
        round(avg(success_rate_pct), 2)                        as avg_success_rate_pct,
        round(min(success_rate_pct), 2)                        as min_success_rate_pct,
        round(max(success_rate_pct), 2)                        as max_success_rate_pct,

        -- Timeline
        min(mission_date)                                      as first_mission_date,
        max(mission_date)                                      as last_mission_date

    from mission_flags
    group by destination

),

with_rates as (

    -- Compute rates separately so aggregated counts are already resolved.
    -- Rates are calculated against settled missions only to avoid
    -- diluting results with missions that have not yet concluded.

    select
        destination,

        total_missions,
        settled_missions,
        completed_missions,
        partial_success_missions,
        failed_missions,
        aborted_missions,
        in_progress_missions,
        planned_missions,

        round(
            completed_missions * 100.0 / nullif(settled_missions, 0),
        2)                                                     as completion_rate_pct,

        round(
            (failed_missions + aborted_missions) * 100.0 / nullif(settled_missions, 0),
        2)                                                     as failure_rate_pct,

        crewed_missions,
        uncrewed_missions,
        avg_crew_size,

        avg_duration_days,

        avg_success_rate_pct,
        min_success_rate_pct,
        max_success_rate_pct,

        first_mission_date,
        last_mission_date

    from aggregated

)

select
    destination,

    total_missions,
    settled_missions,
    completed_missions,
    partial_success_missions,
    failed_missions,
    aborted_missions,
    in_progress_missions,
    planned_missions,

    completion_rate_pct,
    failure_rate_pct,

    crewed_missions,
    uncrewed_missions,
    avg_crew_size,

    avg_duration_days,

    avg_success_rate_pct,
    min_success_rate_pct,
    max_success_rate_pct,

    first_mission_date,
    last_mission_date

from with_rates
order by total_missions desc
