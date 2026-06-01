-- OMOP CDM v5.4 — Condition Occurrence
-- Synthea conditions → OMOP-aligned condition_occurrence
{{ config(
    materialized='table',
    schema='omop',
    tags=['omop', 'cdm', 'condition', 'silver']
) }}

WITH conditions AS (
    SELECT
        patient_id,
        START::DATE                AS condition_start_date,
        NULLIF(STOP, '')::DATE     AS condition_end_date,
        code                       AS condition_source_value,
        description                AS condition_source_label,
        CURRENT_TIMESTAMP          AS loaded_at
    FROM {{ source('synthea_raw', 'conditions') }}
),

joined AS (
    SELECT
        -- 63-bit mask to keep the value in signed BIGINT range.
        (HASH(c.patient_id || '|' || c.condition_start_date || '|' || c.condition_source_value)
            & 9223372036854775807)::BIGINT
            AS condition_occurrence_id,

        (HASH(c.patient_id) & 9223372036854775807)::BIGINT
            AS person_id,

        -- Condition concept_id lookup would go here. For demo, use the source code hash.
        (HASH(c.condition_source_value) & 9223372036854775807)::BIGINT
            AS condition_concept_id,

        c.condition_start_date     AS condition_start_date,
        c.condition_start_date::TIMESTAMP
            AS condition_start_datetime,
        c.condition_end_date       AS condition_end_date,
        c.condition_end_date::TIMESTAMP
            AS condition_end_datetime,

        -- Type: 32827 = EHR billing diagnosis (simplified)
        32827                      AS condition_type_concept_id,

        NULL                       AS stop_reason,
        NULL                       AS provider_id,
        NULL                       AS visit_occurrence_id,

        c.condition_source_value,
        c.condition_source_label,

        -- CCS category via Python UDF
        COALESCE(ccs.ccs_category, 'unmapped') AS ccs_category,

        c.loaded_at                AS cdm_loaded_at
    FROM conditions c
    LEFT JOIN {{ ref('icd10_to_ccs') }} ccs
        ON SUBSTRING(c.condition_source_value, 1, LENGTH(ccs.icd10_prefix)) = ccs.icd10_prefix
)

SELECT * FROM joined
