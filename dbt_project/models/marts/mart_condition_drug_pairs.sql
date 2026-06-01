-- Gold: Condition-Drug Pairings
--
-- For each (person, condition, drug) tuple, the count of exposures and
-- average days between condition onset and drug start. Useful for:
--   - Treatment pathway analysis ("what do we prescribe for hypertension?")
--   - Off-label use detection
--   - Cohort characterization
--
-- The output is a row per (person_id, condition_source_value, drug_source_value).

{{ config(
    materialized='table',
    schema='marts',
    tags=['gold', 'clinical', 'treatment_pathways', 'pharmacy'],
) }}

WITH conditions AS (
    SELECT
        person_id,
        condition_source_value,
        -- Use the first column from CCS as a human-readable category
        ccs_category,
        condition_start_date,
        condition_concept_id
    FROM {{ ref('omcdm_condition_occurrence') }}
    WHERE condition_start_date IS NOT NULL
),

drugs AS (
    SELECT
        person_id,
        drug_source_value,
        drug_source_label,
        drug_exposure_start_date,
        drug_exposure_id
    FROM {{ ref('omcdm_drug_exposure') }}
    WHERE drug_exposure_start_date IS NOT NULL
),

joined AS (
    -- For each (person, condition, drug): include only if the drug was
    -- prescribed within 90 days of the condition onset. This is a
    -- pragmatic proxy for "treatment relationship".
    SELECT
        c.person_id,
        c.condition_source_value,
        c.ccs_category                              AS condition_ccs_category,
        d.drug_source_value,
        ANY_VALUE(d.drug_source_label)               AS drug_source_label,
        MIN(d.drug_exposure_start_date)              AS first_drug_for_condition,
        MIN(c.condition_start_date)                  AS first_condition_onset,
        -- Days from condition onset to first drug: positive = drug after dx
        CAST(MIN(d.drug_exposure_start_date) - MIN(c.condition_start_date) AS INTEGER)
                                                    AS days_from_onset_to_drug,
        COUNT(DISTINCT d.drug_exposure_id)           AS drug_fills_for_condition
    FROM conditions c
    INNER JOIN drugs d
        ON c.person_id = d.person_id
        AND d.drug_exposure_start_date BETWEEN c.condition_start_date
                                            AND c.condition_start_date + INTERVAL '90' DAY
    GROUP BY 1, 2, 3, 4
)

SELECT
    person_id,
    condition_source_value,
    condition_ccs_category,
    drug_source_value,
    drug_source_label,
    first_condition_onset,
    first_drug_for_condition,
    days_from_onset_to_drug,
    drug_fills_for_condition,
    -- Categorical: was the drug started in the same month as the dx?
    CASE
        WHEN days_from_onset_to_drug = 0 THEN 'same_day'
        WHEN days_from_onset_to_drug <= 7 THEN 'within_week'
        WHEN days_from_onset_to_drug <= 30 THEN 'within_month'
        WHEN days_from_onset_to_drug <= 90 THEN 'within_quarter'
        ELSE 'after_quarter'
    END AS treatment_lag_bucket,
    CURRENT_TIMESTAMP                               AS dbt_loaded_at
FROM joined
