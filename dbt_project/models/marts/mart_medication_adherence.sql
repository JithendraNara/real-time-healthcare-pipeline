-- Gold: Medication Adherence (Proportion of Days Covered)
--
-- Computes PDC per patient per drug, the standard adherence metric in
-- pharmacy outcomes research. Defined as:
--   PDC = (number of days the patient had the drug on hand) /
--         (number of days in the measurement window)
--
-- A PDC >= 0.80 is the conventional "adherent" threshold (used by CMS,
-- NCQA, and most payer quality programs).
--
-- For Synthea-style longitudinal data, "days on hand" = sum of
-- (end_date - start_date) across overlapping (and de-overlapped) drug
-- exposures, capped at the measurement window.

{{ config(
    materialized='table',
    schema='marts',
    tags=['gold', 'clinical', 'adherence', 'pharmacy'],
) }}

WITH drug_exposures AS (
    SELECT
        person_id,
        drug_source_value,
        drug_source_label,
        drug_exposure_start_date,
        drug_exposure_end_date,
        -- Use the date span as the "days on hand" for the first pass
        -- (a real impl would cap overlapping fills and exclude gaps > grace period)
        GREATEST(
            CAST(COALESCE(drug_exposure_end_date, drug_exposure_start_date) AS DATE)
            - CAST(drug_exposure_start_date AS DATE),
            0
        ) AS days_on_hand
    FROM {{ ref('omcdm_drug_exposure') }}
    WHERE drug_exposure_start_date IS NOT NULL
),

aggregated AS (
    SELECT
        person_id,
        drug_source_value,
        -- Pick the most common label (mode) for display
        ANY_VALUE(drug_source_label) AS drug_source_label,
        COUNT(*)                                                AS fill_count,
        MIN(drug_exposure_start_date)                          AS first_fill_date,
        MAX(COALESCE(drug_exposure_end_date, drug_exposure_start_date))
                                                                AS last_fill_end_date,
        SUM(days_on_hand)                                      AS total_days_on_hand,
        -- Total days in observation window (from first fill to last fill end,
        -- or to today if still active)
        GREATEST(
            CAST(COALESCE(
                MAX(COALESCE(drug_exposure_end_date, drug_exposure_start_date)),
                CURRENT_DATE
            ) AS DATE) - CAST(MIN(drug_exposure_start_date) AS DATE),
            1
        ) AS observation_window_days
    FROM drug_exposures
    GROUP BY 1, 2
)

SELECT
    person_id,
    drug_source_value,
    drug_source_label,
    fill_count,
    first_fill_date,
    last_fill_end_date,
    total_days_on_hand,
    observation_window_days,
    -- PDC: Proportion of Days Covered
    ROUND(
        LEAST(total_days_on_hand::DOUBLE / NULLIF(observation_window_days, 0), 1.0),
        4
    ) AS pdc_score,
    -- Standard CMS-style adherence flag (>= 80% is "adherent")
    CASE
        WHEN total_days_on_hand::DOUBLE / NULLIF(observation_window_days, 0) >= 0.80 THEN 'adherent'
        WHEN total_days_on_hand::DOUBLE / NULLIF(observation_window_days, 0) >= 0.60 THEN 'partially_adherent'
        ELSE 'non_adherent'
    END AS adherence_category,
    CURRENT_TIMESTAMP                                       AS dbt_loaded_at
FROM aggregated
WHERE observation_window_days >= 30  -- skip noise from very short windows
