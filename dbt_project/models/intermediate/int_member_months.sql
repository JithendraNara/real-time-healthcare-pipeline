-- Intermediate: member months
-- Calculate coverage months per member for PMPM calculations
{{ config(
    materialized='table',
    schema='intermediate'
) }}

WITH members AS (
    SELECT
        m.member_id,
        m.first_name,
        m.last_name,
        m.date_of_birth,
        m.coverage_effective_date,
        m.coverage_termination_date,
        m.plan_type,
        m.metal_level,
        m.state,
        m.relationship,
        -- Active only
        CASE
            WHEN m.coverage_termination_date IS NULL
                 OR m.coverage_termination_date >= CURRENT_DATE
            THEN TRUE ELSE FALSE
        END AS is_active
    FROM {{ ref('stg_eligibility_members') }} m
),

unrolled AS (
    SELECT
        member_id,
        first_name,
        last_name,
        date_of_birth,
        coverage_effective_date,
        coverage_termination_date,
        plan_type,
        metal_level,
        state,
        relationship,
        is_active,
        -- Compute the end month as a scalar
        COALESCE(
            DATE_TRUNC('month', coverage_termination_date),
            DATE_TRUNC('month', CURRENT_DATE)
        ) AS end_month,
        DATE_TRUNC('month', coverage_effective_date) AS start_month,
        -- Compute age once
        CAST(DATE_DIFF('year', date_of_birth, CURRENT_DATE) AS INTEGER) AS age
    FROM members
),

month_series AS (
    SELECT
        u.member_id,
        u.first_name,
        u.last_name,
        u.date_of_birth,
        u.age,
        u.plan_type,
        u.metal_level,
        u.state,
        u.relationship,
        u.is_active,
        gs.coverage_month,
        CASE WHEN u.relationship = 'Self' THEN TRUE ELSE FALSE END AS is_primary
    FROM unrolled u
    CROSS JOIN UNNEST(
        GENERATE_SERIES(
            CAST(u.start_month AS DATE),
            CAST(u.end_month AS DATE),
            INTERVAL '1' MONTH
        )
    ) AS gs(coverage_month)
)

SELECT
    member_id,
    first_name,
    last_name,
    date_of_birth,
    age,
    plan_type,
    metal_level,
    state,
    coverage_month,
    is_primary,
    1 AS member_months
FROM month_series
WHERE is_active
