-- OMOP CDM v5.4 — Measurement
-- Synthea observations (vitals + labs) → OMOP-aligned measurement
{{ config(
    materialized='table',
    schema='omop',
    tags=['omop', 'cdm', 'measurement', 'silver']
) }}

WITH observations AS (
    SELECT
        patient_id,
        DATE::DATE                  AS measurement_date,
        DATE::TIMESTAMP             AS measurement_datetime,
        NULLIF(CODE, '')::VARCHAR   AS measurement_source_value,
        COALESCE(DESCRIPTION, '')::VARCHAR
                                    AS measurement_source_label,
        -- VALUE and UNITS are stored as strings in Synthea; we coerce safely.
        TRY_CAST(VALUE AS DOUBLE)   AS value_as_number,
        NULLIF(UNITS, '')::VARCHAR  AS unit_source_value,
        -- Categorize the measurement for downstream cohorts
        CASE
            WHEN regexp_matches(LOWER(DESCRIPTION), 'blood pressure|systolic|diastolic') THEN 'vitals_bp'
            WHEN regexp_matches(LOWER(DESCRIPTION), 'heart rate|pulse')                  THEN 'vitals_hr'
            WHEN regexp_matches(LOWER(DESCRIPTION), 'temperature|temp')                  THEN 'vitals_temp'
            WHEN regexp_matches(LOWER(DESCRIPTION), 'bmi|weight|height')                  THEN 'vitals_body'
            WHEN regexp_matches(LOWER(DESCRIPTION), 'glucose|hba1c|cholesterol|triglyceride|a1c')
                                                                                       THEN 'lab_metabolic'
            WHEN regexp_matches(LOWER(DESCRIPTION), 'creatinine|egfr|bun|urea')          THEN 'lab_renal'
            WHEN regexp_matches(LOWER(DESCRIPTION), 'hemoglobin|hct|wbc|plt|rbc')        THEN 'lab_hematology'
            WHEN regexp_matches(LOWER(DESCRIPTION), 'tsh|t3|t4|insulin|cortisol')         THEN 'lab_endocrine'
            ELSE 'other'
        END AS measurement_category,
        CURRENT_TIMESTAMP           AS loaded_at
    FROM {{ source('synthea_raw', 'observations') }}
    WHERE NULLIF(CODE, '') IS NOT NULL
)

SELECT
    -- 63-bit hash to keep in signed BIGINT range. Include ROW_NUMBER()
    -- to break ties on identical (patient, datetime, code) tuples.
    (HASH(patient_id || '|' || measurement_datetime || '|' || measurement_source_value || '|' || row_num)
        & 9223372036854775807)::BIGINT
        AS measurement_id,

    (HASH(patient_id) & 9223372036854775807)::BIGINT
        AS person_id,

    -- Concept_id (would be a SNOMED/LOINC lookup in real impl)
    (HASH(LOWER(measurement_source_value)) & 9223372036854775807)::BIGINT
        AS measurement_concept_id,

    measurement_date,
    measurement_datetime,

    -- Time of day. For multi-daily measurements this matters.
    CAST(EXTRACT(HOUR FROM measurement_datetime) AS INTEGER)
        AS measurement_time,

    CAST(value_as_number AS DOUBLE) AS value_as_number,

    -- Concept_id for the unit. Real impl: UCUM lookup.
    0 AS unit_concept_id,
    unit_source_value,

    -- LOINC category for grouping (0 = no info)
    0 AS measurement_type_concept_id,

    NULL AS operator_concept_id,                      -- =, <, >, etc.
    NULL AS value_as_concept_id,
    NULL AS range_low,
    NULL AS range_high,
    NULL AS provider_id,
    NULL AS visit_occurrence_id,

    measurement_source_value,
    measurement_source_label,
    measurement_category,

    loaded_at AS cdm_loaded_at

FROM (
    SELECT
        *,
        CAST(ROW_NUMBER() OVER (
            PARTITION BY patient_id, measurement_datetime, measurement_source_value
            ORDER BY loaded_at
        ) AS VARCHAR) AS row_num
    FROM observations
    WHERE value_as_number IS NOT NULL
) obs
