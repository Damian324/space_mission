-- stg_space_missions
-- Staging layer for raw_space_missions.
-- Responsibilities:
--   - Cast types (date string → DATE)
--   - Rename columns for clarity and to avoid reserved words
--   - Trim residual whitespace on all string fields
--   - Derive simple, unambiguous boolean flags (no business logic)
--   - Surface duplicate mission_id occurrences for downstream handling

CREATE OR REPLACE VIEW stg_space_missions AS
SELECT
    -- Identifiers
    TRIM(mission_id)                                                AS mission_id,

    -- Dates
    CAST(TRIM(date) AS DATE)                                        AS mission_date,

    -- Dimensions
    TRIM(destination)                                               AS destination,
    TRIM(status)                                                    AS status,

    -- Measures
    crew_size,
    duration_days,
    ROUND(success_rate, 2)                                          AS success_rate_pct,

    -- Derived flags (non-business-logic)
    crew_size > 0                                                   AS is_crewed,

    -- Data quality flags
    COUNT(*) OVER (PARTITION BY TRIM(mission_id)) > 1              AS is_duplicate_mission_id,

    -- Passthrough (sensitive — kept as-is, no transformation)
    TRIM(security_code)                                             AS security_code

FROM raw_space_missions
