-- OMOP CDM v5.4 — Person table
-- Synthea patients → OMOP-aligned Person records
{{ config(
    materialized='table',
    schema='omop',
    tags=['omop', 'cdm', 'person', 'silver']
) }}

SELECT
    -- OMOP person_id (sequence, but for portability use a hash of the source id)
    -- 63-bit mask to keep the value in signed BIGINT range.
    (HASH(patient_id) & 9223372036854775807)::BIGINT    AS person_id,
    patient_id                                        AS person_source_value,

    -- Gender concept (OMOP uses concept_ids; simplified to a lookup here)
    CASE UPPER(gender)
        WHEN 'M' THEN 8507
        WHEN 'F' THEN 8532
        ELSE 0
    END                                               AS gender_concept_id,
    UPPER(gender)                                     AS gender_source_value,

    -- Birth
    CAST(birthdate AS DATE)                           AS birth_datetime,
    EXTRACT(YEAR FROM CAST(birthdate AS DATE))::INT   AS year_of_birth,
    EXTRACT(MONTH FROM CAST(birthdate AS DATE))::INT  AS month_of_birth,
    EXTRACT(DAY FROM CAST(birthdate AS DATE))::INT    AS day_of_birth,

    -- Race / ethnicity (default to 0=Unknown; real impl would map from source)
    0                                                 AS race_concept_id,
    NULL                                              AS race_source_value,
    0                                                 AS ethnicity_concept_id,
    NULL                                              AS ethnicity_source_value,

    -- Location (FK to location_id if you have a location table)
    NULL                                              AS location_id,
    NULL                                              AS provider_id,
    NULL                                              AS care_site_id,

    CURRENT_TIMESTAMP                                 AS cdm_loaded_at
FROM {{ source('synthea_raw', 'patients') }}
