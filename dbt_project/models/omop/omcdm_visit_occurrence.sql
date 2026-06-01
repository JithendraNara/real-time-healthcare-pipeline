-- OMOP CDM v5.4 — Visit Occurrence
-- Synthea encounters → OMOP-aligned visit_occurrence
{{ config(
    materialized='table',
    schema='omop',
    tags=['omop', 'cdm', 'visit', 'silver']
) }}

SELECT
    -- 63-bit mask to keep the value in signed BIGINT range.
    (HASH(encounter_id) & 9223372036854775807)::BIGINT  AS visit_occurrence_id,
    (HASH(patient_id) & 9223372036854775807)::BIGINT    AS person_id,

    -- Visit concept: 0=No matching concept; real values: 9201=Inpatient, 9202=Outpatient, 9203=Emergency
    CASE UPPER(encounterclass)
        WHEN 'INPATIENT'     THEN 9201
        WHEN 'OUTPATIENT'    THEN 9202
        WHEN 'EMERGENCY'     THEN 9203
        WHEN 'AMBULATORY'    THEN 9202
        WHEN 'WELLNESS'      THEN 9202
        ELSE 0
    END                                                 AS visit_concept_id,

    START::DATE                                         AS visit_start_date,
    START::TIMESTAMP                                    AS visit_start_datetime,
    STOP::DATE                                          AS visit_end_date,
    STOP::TIMESTAMP                                     AS visit_end_datetime,

    32827                                               AS visit_type_concept_id,  -- EHR
    NULL                                                AS provider_id,
    NULL                                                AS care_site_id,

    (HASH(encounter_id) & 9223372036854775807)::BIGINT  AS visit_source_value,
    encounterclass                                      AS visit_source_label,
    description                                         AS visit_source_description,

    NULL                                                AS admitted_from_concept_id,
    NULL                                                AS admitted_from_source_value,
    NULL                                                AS discharged_to_concept_id,
    NULL                                                AS discharged_to_source_value,

    CURRENT_TIMESTAMP                                   AS cdm_loaded_at
FROM {{ source('synthea_raw', 'encounters') }}
