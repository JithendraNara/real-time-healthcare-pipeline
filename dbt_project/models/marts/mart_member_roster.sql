-- Mart: member roster snapshot
-- Analytics-ready member data with derived fields
{{ config(
    materialized='incremental',
    schema='marts',
    unique_key='member_id',
    on_schema_change='sync_all'
) }}

WITH member_roster AS (
    SELECT
        m.member_id,
        m.first_name,
        m.last_name,
        m.date_of_birth,
        -- Age buckets (useful for segmentation)
        CASE
            WHEN im.age < 18 THEN 'Minor'
            WHEN im.age BETWEEN 18 AND 25 THEN 'Young Adult'
            WHEN im.age BETWEEN 26 AND 40 THEN 'Adult'
            WHEN im.age BETWEEN 41 AND 55 THEN 'Middle'
            WHEN im.age BETWEEN 56 AND 65 THEN 'Pre-Senior'
            ELSE 'Senior'
        END AS age_bucket,
        m.email,
        m.phone,
        m.address,
        m.city,
        m.state,
        m.zip_code,
        m.plan_type,
        m.metal_level,
        m.hsa_eligible,
        m.coverage_effective_date,
        m.coverage_termination_date,
        -- Days since enrollment
        CAST(CURRENT_DATE - m.coverage_effective_date AS BIGINT) AS days_enrolled,
        -- Relationship
        m.relationship,
        -- Active status
        CASE
            WHEN m.coverage_termination_date IS NOT NULL
             AND m.coverage_termination_date < CURRENT_DATE
                THEN 'Inactive'
            ELSE 'Active'
        END AS enrollment_status,
        -- DQ flags
        CASE WHEN m.zip_code IS NULL OR m.zip_code = '' THEN 1 ELSE 0 END AS missing_zip,
        CASE WHEN m.email IS NULL OR m.email = '' THEN 1 ELSE 0 END AS missing_email,
        CASE WHEN m.phone IS NULL OR m.phone = '' THEN 1 ELSE 0 END AS missing_phone,
        -- Child over age flag (potential data issue)
        CASE
            WHEN m.relationship = 'Child' AND im.age > 26 THEN 1
            ELSE 0
        END AS child_overage_flag,
        -- Load metadata
        CURRENT_TIMESTAMP AS dbt_loaded_at
    FROM {{ ref('stg_eligibility_members') }} m
    LEFT JOIN {{ ref('int_member_months') }} im
        ON m.member_id = im.member_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY m.member_id ORDER BY im.coverage_month DESC) = 1
)
SELECT * FROM member_roster
