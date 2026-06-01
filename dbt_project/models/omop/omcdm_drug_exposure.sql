-- OMOP CDM v5.4 — Drug Exposure
-- Synthea medications → OMOP-aligned drug_exposure
{{ config(
    materialized='table',
    schema='omop',
    tags=['omop', 'cdm', 'drug', 'silver']
) }}

WITH medications AS (
    SELECT
        patient_id,
        START::DATE                 AS drug_exposure_start_date,
        STOP::DATE                  AS drug_exposure_end_date,
        START::TIMESTAMP            AS drug_exposure_start_datetime,
        COALESCE(STOP, START)::TIMESTAMP
                                    AS drug_exposure_end_datetime,
        NULLIF(CODE, '')::VARCHAR   AS drug_source_value,
        COALESCE(DESCRIPTION, '')::VARCHAR
                                    AS drug_source_label,
        -- Common drug classification dimensions
        LOWER(REASONDESCRIPTION)::VARCHAR
                                    AS reason_description,
        CURRENT_TIMESTAMP           AS loaded_at
    FROM {{ source('synthea_raw', 'medications') }}
    WHERE NULLIF(CODE, '') IS NOT NULL
)

SELECT
    -- 63-bit hash to keep in signed BIGINT range. Include ROW_NUMBER()
    -- to break ties on identical (patient, start_date, drug) tuples.
    (HASH(patient_id || '|' || drug_exposure_start_date || '|' || drug_source_value || '|' || row_num)
        & 9223372036854775807)::BIGINT
        AS drug_exposure_id,

    (HASH(patient_id) & 9223372036854775807)::BIGINT
        AS person_id,

    -- Drug concept_id lookup. Real impl would join to a CONCEPT table.
    -- For demo, use a stable hash of the drug code.
    (HASH(LOWER(drug_source_value)) & 9223372036854775807)::BIGINT
        AS drug_concept_id,

    drug_exposure_start_date,
    drug_exposure_start_datetime,
    drug_exposure_end_date,
    drug_exposure_end_datetime,

    -- Type: 32827 = EHR prescribing
    32827 AS drug_type_concept_id,

    NULL AS stop_reason,
    NULL AS refills,
    NULL AS quantity,
    NULL AS days_supply,

    NULL AS sig,                                     -- Free-text directions
    NULL AS route_concept_id,                        -- 0=Unknown; real lookup later
    NULL AS lot_number,
    NULL AS provider_id,
    NULL AS visit_occurrence_id,

    drug_source_value,
    drug_source_label,

    -- Derived: drug class via simple prefix match on the source code
    -- Real impl would use RxNorm + ATC. This is a coarse categorical flag.
    CASE
        WHEN drug_source_value ~ '^[0-9]+'  THEN 'rxcui_numeric'
        ELSE 'rxcui_other'
    END AS drug_code_type,

    reason_description,
    loaded_at AS cdm_loaded_at

FROM (
    SELECT
        *,
        CAST(ROW_NUMBER() OVER (
            PARTITION BY patient_id, drug_exposure_start_date, drug_source_value
            ORDER BY loaded_at
        ) AS VARCHAR) AS row_num
    FROM medications
) m
