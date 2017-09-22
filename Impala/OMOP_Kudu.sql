-- Use the search/replace regex in an editor to fix DATE columns:
-- ([^ ]+) VARCHAR\(8\), \-\- DATE
-- TO_UTC_TIMESTAMP(CONCAT_WS('-', SUBSTR(CAST($1 AS STRING), 1, 4), SUBSTR(CAST($1 AS STRING), 5, 2), SUBSTR(CAST($1 AS STRING), 7, 2)), 'UTC') AS $1,
-- Fix VARCHAR to STRING
-- ([^ ]+) VARCHAR\(\d+\)
-- CAST($1 AS STRING) AS $1

CREATE TABLE omop_cdm_kudu.concept
PRIMARY KEY (concept_id)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = '${var:KUDU_MASTER}'
)
AS
SELECT
 concept_id,
 CAST(concept_name AS STRING) AS concept_name,
 CAST(domain_id AS STRING) AS domain_id,
 CAST(vocabulary_id AS STRING) AS vocabulary_id,
 CAST(concept_class_id AS STRING) AS concept_class_id,
 CAST(standard_concept AS STRING) AS standard_concept,
 CAST(concept_code AS STRING) AS concept_code,
 TO_UTC_TIMESTAMP(CONCAT_WS('-', SUBSTR(CAST(valid_start_date AS STRING), 1, 4), SUBSTR(CAST(valid_start_date AS STRING), 5, 2), SUBSTR(CAST(valid_start_date AS STRING), 7, 2)), 'UTC') AS valid_start_date,
 TO_UTC_TIMESTAMP(CONCAT_WS('-', SUBSTR(CAST(valid_end_date AS STRING), 1, 4), SUBSTR(CAST(valid_end_date AS STRING), 5, 2), SUBSTR(CAST(valid_end_date AS STRING), 7, 2)), 'UTC') AS valid_end_date,
 CAST(nullif(invalid_reason, '') AS STRING) AS invalid_reason
FROM omop_cdm.concept;

CREATE TABLE omop_cdm_kudu.vocabulary
PRIMARY KEY (vocabulary_id)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = '${var:KUDU_MASTER}'
)
AS
SELECT
 CAST(vocabulary_id AS STRING) AS vocabulary_id,
 CAST(vocabulary_name AS STRING) AS vocabulary_name,
 CAST(vocabulary_reference AS STRING) AS vocabulary_reference,
 CAST(vocabulary_version AS STRING) AS vocabulary_version,
 vocabulary_concept_id
FROM omop_cdm.vocabulary;

CREATE TABLE omop_cdm_kudu.domain
PRIMARY KEY (domain_id)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = '${var:KUDU_MASTER}'
)
AS
SELECT
 CAST(domain_id AS STRING) AS domain_id,
 CAST(domain_name AS STRING) AS domain_name,
 domain_concept_id
FROM omop_cdm.domain;

CREATE TABLE omop_cdm_kudu.concept_class
PRIMARY KEY (concept_class_id)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = '${var:KUDU_MASTER}'
)
AS
SELECT
 CAST(concept_class_id AS STRING) AS concept_class_id,
 CAST(concept_class_name AS STRING) AS concept_class_name,
 concept_class_concept_id
FROM omop_cdm.concept_class;

CREATE TABLE omop_cdm_kudu.concept_relationship
PRIMARY KEY (concept_id_1, concept_id_2, relationship_id)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = '${var:KUDU_MASTER}'
)
AS
SELECT
 concept_id_1,
 concept_id_2,
 CAST(relationship_id AS STRING) AS relationship_id,
 TO_UTC_TIMESTAMP(CONCAT_WS('-', SUBSTR(CAST(valid_start_date AS STRING), 1, 4), SUBSTR(CAST(valid_start_date AS STRING), 5, 2), SUBSTR(CAST(valid_start_date AS STRING), 7, 2)), 'UTC') AS valid_start_date,
 TO_UTC_TIMESTAMP(CONCAT_WS('-', SUBSTR(CAST(valid_end_date AS STRING), 1, 4), SUBSTR(CAST(valid_end_date AS STRING), 5, 2), SUBSTR(CAST(valid_end_date AS STRING), 7, 2)), 'UTC') AS valid_end_date,
 CAST(nullif(invalid_reason, '') AS STRING) AS invalid_reason
FROM omop_cdm.concept_relationship;

CREATE TABLE omop_cdm_kudu.relationship
PRIMARY KEY (relationship_id)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = '${var:KUDU_MASTER}'
)
AS
SELECT
 CAST(relationship_id AS STRING) AS relationship_id,
 CAST(relationship_name AS STRING) AS relationship_name,
 CAST(is_hierarchical AS STRING) AS is_hierarchical,
 CAST(defines_ancestry AS STRING) AS defines_ancestry,
 CAST(reverse_relationship_id AS STRING) AS reverse_relationship_id,
 relationship_concept_id
FROM omop_cdm.relationship;

CREATE TABLE omop_cdm_kudu.concept_synonym
PRIMARY KEY (concept_id, concept_synonym_name, language_concept_id)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = '${var:KUDU_MASTER}'
)
AS
SELECT
 concept_id,
 CAST(concept_synonym_name AS STRING) AS concept_synonym_name,
 language_concept_id
FROM omop_cdm.concept_synonym;

CREATE TABLE omop_cdm_kudu.concept_ancestor
PRIMARY KEY (ancestor_concept_id, descendant_concept_id)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = '${var:KUDU_MASTER}'
)
AS
SELECT * FROM omop_cdm.concept_ancestor;

CREATE TABLE omop_cdm_kudu.source_to_concept_map
PRIMARY KEY (source_vocabulary_id, target_concept_id, source_code, valid_end_date)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = '${var:KUDU_MASTER}'
)
AS
SELECT
 CAST(source_vocabulary_id AS STRING) AS source_vocabulary_id,
 target_concept_id,
 CAST(source_code AS STRING) AS source_code,
 TO_UTC_TIMESTAMP(CONCAT_WS('-', SUBSTR(CAST(valid_end_date AS STRING), 1, 4), SUBSTR(CAST(valid_end_date AS STRING), 5, 2), SUBSTR(CAST(valid_end_date AS STRING), 7, 2)), 'UTC') AS valid_end_date,
 source_concept_id,
 CAST(source_code_description AS STRING) AS source_code_description,
 CAST(target_vocabulary_id AS STRING) AS target_vocabulary_id,
 TO_UTC_TIMESTAMP(CONCAT_WS('-', SUBSTR(CAST(valid_start_date AS STRING), 1, 4), SUBSTR(CAST(valid_start_date AS STRING), 5, 2), SUBSTR(CAST(valid_start_date AS STRING), 7, 2)), 'UTC') AS valid_start_date,
 CAST(nullif(invalid_reason, '') AS STRING) AS invalid_reason
FROM omop_cdm.source_to_concept_map;

CREATE TABLE omop_cdm_kudu.drug_strength
PRIMARY KEY (drug_concept_id, ingredient_concept_id)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = '${var:KUDU_MASTER}'
)
AS
SELECT
 drug_concept_id,
 ingredient_concept_id,
 amount_value, -- NUMERIC
 amount_unit_concept_id,
 numerator_value, -- NUMERIC
 numerator_unit_concept_id,
 denominator_value, -- NUMERIC
 denominator_unit_concept_id,
 TO_UTC_TIMESTAMP(CONCAT_WS('-', SUBSTR(CAST(valid_start_date AS STRING), 1, 4), SUBSTR(CAST(valid_start_date AS STRING), 5, 2), SUBSTR(CAST(valid_start_date AS STRING), 7, 2)), 'UTC') AS valid_start_date,
 TO_UTC_TIMESTAMP(CONCAT_WS('-', SUBSTR(CAST(valid_end_date AS STRING), 1, 4), SUBSTR(CAST(valid_end_date AS STRING), 5, 2), SUBSTR(CAST(valid_end_date AS STRING), 7, 2)), 'UTC') AS valid_end_date,
 CAST(nullif(invalid_reason, '') AS STRING) AS invalid_reason
FROM omop_cdm.drug_strength;

CREATE TABLE omop_cdm_kudu.cohort_definition
PRIMARY KEY (cohort_definition_id)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = '${var:KUDU_MASTER}'
)
AS
SELECT
 cohort_definition_id,
 CAST(cohort_definition_name AS STRING) AS cohort_definition_name,
 cohort_definition_description, -- TEXT
 definition_type_concept_id,
 cohort_definition_syntax, -- TEXT
 subject_concept_id,
 TO_UTC_TIMESTAMP(CONCAT_WS('-', SUBSTR(CAST(cohort_initiation_date AS STRING), 1, 4), SUBSTR(CAST(cohort_initiation_date AS STRING), 5, 2), SUBSTR(CAST(cohort_initiation_date AS STRING), 7, 2)), 'UTC') AS cohort_initiation_date
FROM omop_cdm.cohort_definition;

CREATE TABLE omop_cdm_kudu.attribute_definition
PRIMARY KEY (attribute_definition_id)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = '${var:KUDU_MASTER}'
)
AS
SELECT
 attribute_definition_id,
 CAST(attribute_name AS STRING) AS attribute_name,
 attribute_description, -- TEXT
 attribute_type_concept_id,
 attribute_syntax -- TEXT
FROM omop_cdm.attribute_definition;

CREATE TABLE omop_cdm_kudu.cdm_source
PRIMARY KEY (cdm_source_name)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = '${var:KUDU_MASTER}'
)
AS
SELECT
 CAST(cdm_source_name AS STRING) AS cdm_source_name,
 CAST(cdm_source_abbreviation AS STRING) AS cdm_source_abbreviation,
 CAST(cdm_holder AS STRING) AS cdm_holder,
 source_description, -- TEXT
 CAST(source_documentation_reference AS STRING) AS source_documentation_reference,
 CAST(cdm_etl_reference AS STRING) AS cdm_etl_reference,
 TO_UTC_TIMESTAMP(CONCAT_WS('-', SUBSTR(CAST(source_release_date AS STRING), 1, 4), SUBSTR(CAST(source_release_date AS STRING), 5, 2), SUBSTR(CAST(source_release_date AS STRING), 7, 2)), 'UTC') AS source_release_date,
 TO_UTC_TIMESTAMP(CONCAT_WS('-', SUBSTR(CAST(cdm_release_date AS STRING), 1, 4), SUBSTR(CAST(cdm_release_date AS STRING), 5, 2), SUBSTR(CAST(cdm_release_date AS STRING), 7, 2)), 'UTC') AS cdm_release_date,
 CAST(cdm_version AS STRING) AS cdm_version,
 CAST(vocabulary_version AS STRING) AS vocabulary_version
FROM omop_cdm.cdm_source;

CREATE TABLE omop_cdm_kudu.person
PRIMARY KEY (person_id)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = '${var:KUDU_MASTER}'
)
AS
SELECT
 person_id,
 gender_concept_id,
 year_of_birth,
 month_of_birth,
 day_of_birth,
 CAST(time_of_birth AS STRING) AS time_of_birth,
 race_concept_id,
 ethnicity_concept_id,
 location_id,
 provider_id,
 care_site_id,
 CAST(person_source_value AS STRING) AS person_source_value,
 CAST(gender_source_value AS STRING) AS gender_source_value,
 gender_source_concept_id,
 CAST(race_source_value AS STRING) AS race_source_value,
 race_source_concept_id,
 CAST(ethnicity_source_value AS STRING) AS ethnicity_source_value,
 ethnicity_source_concept_id
FROM omop_cdm.person;

CREATE TABLE omop_cdm_kudu.observation_period
PRIMARY KEY (observation_period_id)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = '${var:KUDU_MASTER}'
)
AS
SELECT
 observation_period_id,
 person_id,
 TO_UTC_TIMESTAMP(CONCAT_WS('-', SUBSTR(CAST(observation_period_start_date AS STRING), 1, 4), SUBSTR(CAST(observation_period_start_date AS STRING), 5, 2), SUBSTR(CAST(observation_period_start_date AS STRING), 7, 2)), 'UTC') AS observation_period_start_date,
 TO_UTC_TIMESTAMP(CONCAT_WS('-', SUBSTR(CAST(observation_period_end_date AS STRING), 1, 4), SUBSTR(CAST(observation_period_end_date AS STRING), 5, 2), SUBSTR(CAST(observation_period_end_date AS STRING), 7, 2)), 'UTC') AS observation_period_end_date,
 period_type_concept_id
FROM omop_cdm.observation_period;

CREATE TABLE omop_cdm_kudu.specimen
PRIMARY KEY (specimen_id)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = '${var:KUDU_MASTER}'
)
AS
SELECT
 specimen_id,
 person_id,
 specimen_concept_id,
 specimen_type_concept_id,
 TO_UTC_TIMESTAMP(CONCAT_WS('-', SUBSTR(CAST(specimen_date AS STRING), 1, 4), SUBSTR(CAST(specimen_date AS STRING), 5, 2), SUBSTR(CAST(specimen_date AS STRING), 7, 2)), 'UTC') AS specimen_date,
 CAST(specimen_time AS STRING) AS specimen_time,
 quantity, -- NUMERIC
 unit_concept_id,
 anatomic_site_concept_id,
 disease_status_concept_id,
 CAST(specimen_source_id AS STRING) AS specimen_source_id,
 CAST(specimen_source_value AS STRING) AS specimen_source_value,
 CAST(unit_source_value AS STRING) AS unit_source_value,
 CAST(anatomic_site_source_value AS STRING) AS anatomic_site_source_value,
 CAST(disease_status_source_value AS STRING) AS disease_status_source_value
FROM omop_cdm.specimen;

CREATE TABLE omop_cdm_kudu.death
PRIMARY KEY (person_id)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = '${var:KUDU_MASTER}'
)
AS
SELECT
 person_id,
 TO_UTC_TIMESTAMP(CONCAT_WS('-', SUBSTR(CAST(death_date AS STRING), 1, 4), SUBSTR(CAST(death_date AS STRING), 5, 2), SUBSTR(CAST(death_date AS STRING), 7, 2)), 'UTC') AS death_date,
 death_type_concept_id,
 cause_concept_id,
 CAST(cause_source_value AS STRING) AS cause_source_value,
 cause_source_concept_id
FROM omop_cdm.death;

CREATE TABLE omop_cdm_kudu.visit_occurrence
PRIMARY KEY (visit_occurrence_id)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = '${var:KUDU_MASTER}'
)
AS
SELECT
 visit_occurrence_id,
 person_id,
 visit_concept_id,
 TO_UTC_TIMESTAMP(CONCAT_WS('-', SUBSTR(CAST(visit_start_date AS STRING), 1, 4), SUBSTR(CAST(visit_start_date AS STRING), 5, 2), SUBSTR(CAST(visit_start_date AS STRING), 7, 2)), 'UTC') AS visit_start_date,
 CAST(visit_start_time AS STRING) AS visit_start_time,
 TO_UTC_TIMESTAMP(CONCAT_WS('-', SUBSTR(CAST(visit_end_date AS STRING), 1, 4), SUBSTR(CAST(visit_end_date AS STRING), 5, 2), SUBSTR(CAST(visit_end_date AS STRING), 7, 2)), 'UTC') AS visit_end_date,
 CAST(visit_end_time AS STRING) AS visit_end_time,
 visit_type_concept_id,
 provider_id,
 care_site_id,
 CAST(visit_source_value AS STRING) AS visit_source_value,
 visit_source_concept_id
FROM omop_cdm.visit_occurrence;

CREATE TABLE omop_cdm_kudu.procedure_occurrence
PRIMARY KEY (procedure_occurrence_id)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = '${var:KUDU_MASTER}'
)
AS
SELECT
 procedure_occurrence_id,
 person_id,
 procedure_concept_id,
 TO_UTC_TIMESTAMP(CONCAT_WS('-', SUBSTR(CAST(procedure_date AS STRING), 1, 4), SUBSTR(CAST(procedure_date AS STRING), 5, 2), SUBSTR(CAST(procedure_date AS STRING), 7, 2)), 'UTC') AS procedure_date,
 procedure_type_concept_id,
 modifier_concept_id,
 quantity,
 provider_id,
 visit_occurrence_id,
 CAST(procedure_source_value AS STRING) AS procedure_source_value,
 procedure_source_concept_id,
 CAST(qualifier_source_value AS STRING) AS qualifier_source_value
FROM omop_cdm.procedure_occurrence;

CREATE TABLE omop_cdm_kudu.drug_exposure
PRIMARY KEY (drug_exposure_id)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = '${var:KUDU_MASTER}'
)
AS
SELECT
 drug_exposure_id,
 person_id,
 drug_concept_id,
TO_UTC_TIMESTAMP(CONCAT_WS('-', SUBSTR(CAST(drug_exposure_start_date AS STRING), 1, 4), SUBSTR(CAST(drug_exposure_start_date AS STRING), 5, 2), SUBSTR(CAST(drug_exposure_start_date AS STRING), 7, 2)), 'UTC') AS drug_exposure_start_date,
 TO_UTC_TIMESTAMP(CONCAT_WS('-', SUBSTR(CAST(drug_exposure_end_date AS STRING), 1, 4), SUBSTR(CAST(drug_exposure_end_date AS STRING), 5, 2), SUBSTR(CAST(drug_exposure_end_date AS STRING), 7, 2)), 'UTC') AS drug_exposure_end_date,
 drug_type_concept_id,
 CAST(stop_reason AS STRING) AS stop_reason,
 refills,
 quantity, -- NUMERIC
 days_supply,
 sig, -- TEXT
 route_concept_id,
 effective_drug_dose, -- NUMERIC
 dose_unit_concept_id,
 CAST(lot_number AS STRING) AS lot_number,
 provider_id,
 visit_occurrence_id,
 CAST(drug_source_value AS STRING) AS drug_source_value,
 drug_source_concept_id,
 CAST(route_source_value AS STRING) AS route_source_value,
 CAST(dose_unit_source_value AS STRING) AS dose_unit_source_value
FROM omop_cdm.drug_exposure;

CREATE TABLE omop_cdm_kudu.device_exposure
PRIMARY KEY (device_exposure_id)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = '${var:KUDU_MASTER}'
)
AS
SELECT
 device_exposure_id,
 person_id,
 device_concept_id,
 TO_UTC_TIMESTAMP(CONCAT_WS('-', SUBSTR(CAST(device_exposure_start_date AS STRING), 1, 4), SUBSTR(CAST(device_exposure_start_date AS STRING), 5, 2), SUBSTR(CAST(device_exposure_start_date AS STRING), 7, 2)), 'UTC') AS device_exposure_start_date,
 TO_UTC_TIMESTAMP(CONCAT_WS('-', SUBSTR(CAST(device_exposure_end_date AS STRING), 1, 4), SUBSTR(CAST(device_exposure_end_date AS STRING), 5, 2), SUBSTR(CAST(device_exposure_end_date AS STRING), 7, 2)), 'UTC') AS device_exposure_end_date,
 device_type_concept_id,
 CAST(unique_device_id AS STRING) AS unique_device_id,
 quantity,
 provider_id,
 visit_occurrence_id,
 CAST(device_source_value AS STRING) AS device_source_value,
 device_source_concept_id
FROM omop_cdm.device_exposure;

CREATE TABLE omop_cdm_kudu.condition_occurrence
PRIMARY KEY (condition_occurrence_id)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = '${var:KUDU_MASTER}'
)
AS
SELECT
 condition_occurrence_id,
 person_id,
 condition_concept_id,
 TO_UTC_TIMESTAMP(CONCAT_WS('-', SUBSTR(CAST(condition_start_date AS STRING), 1, 4), SUBSTR(CAST(condition_start_date AS STRING), 5, 2), SUBSTR(CAST(condition_start_date AS STRING), 7, 2)), 'UTC') AS condition_start_date,
 TO_UTC_TIMESTAMP(CONCAT_WS('-', SUBSTR(CAST(condition_end_date AS STRING), 1, 4), SUBSTR(CAST(condition_end_date AS STRING), 5, 2), SUBSTR(CAST(condition_end_date AS STRING), 7, 2)), 'UTC') AS condition_end_date,
 condition_type_concept_id,
 CAST(stop_reason AS STRING) AS stop_reason,
 provider_id,
 visit_occurrence_id,
 CAST(condition_source_value AS STRING) AS condition_source_value,
 condition_source_concept_id
FROM omop_cdm.condition_occurrence;

CREATE TABLE omop_cdm_kudu.measurement
PRIMARY KEY (measurement_id)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = '${var:KUDU_MASTER}'
)
AS
SELECT
 measurement_id,
 person_id,
 measurement_concept_id,
 TO_UTC_TIMESTAMP(CONCAT_WS('-', SUBSTR(CAST(measurement_date AS STRING), 1, 4), SUBSTR(CAST(measurement_date AS STRING), 5, 2), SUBSTR(CAST(measurement_date AS STRING), 7, 2)), 'UTC') AS measurement_date,
 CAST(measurement_time AS STRING) AS measurement_time,
 measurement_type_concept_id,
 operator_concept_id,
 value_as_number, -- NUMERIC
 value_as_concept_id,
 unit_concept_id,
 range_low, -- NUMERIC
 range_high, -- NUMERIC
 provider_id,
 visit_occurrence_id,
 CAST(measurement_source_value AS STRING) AS measurement_source_value,
 measurement_source_concept_id,
 CAST(unit_source_value AS STRING) AS unit_source_value,
 CAST(value_source_value AS STRING) AS value_source_value
FROM omop_cdm.measurement;

CREATE TABLE omop_cdm_kudu.note
PRIMARY KEY (note_id)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = '${var:KUDU_MASTER}'
)
AS
SELECT
 note_id,
 person_id,
 TO_UTC_TIMESTAMP(CONCAT_WS('-', SUBSTR(CAST(note_date AS STRING), 1, 4), SUBSTR(CAST(note_date AS STRING), 5, 2), SUBSTR(CAST(note_date AS STRING), 7, 2)), 'UTC') AS note_date,
 CAST(note_time AS STRING) AS note_time,
 note_type_concept_id,
 note_text, -- TEXT
 provider_id,
 visit_occurrence_id,
 CAST(note_source_value AS STRING) AS note_source_value
FROM omop_cdm.note;

CREATE TABLE omop_cdm_kudu.observation
PRIMARY KEY (observation_id)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = '${var:KUDU_MASTER}'
)
AS
SELECT
 observation_id,
 person_id,
 observation_concept_id,
 TO_UTC_TIMESTAMP(CONCAT_WS('-', SUBSTR(CAST(observation_date AS STRING), 1, 4), SUBSTR(CAST(observation_date AS STRING), 5, 2), SUBSTR(CAST(observation_date AS STRING), 7, 2)), 'UTC') AS observation_date,
 CAST(observation_time AS STRING) AS observation_time,
 observation_type_concept_id,
 value_as_number, -- NUMERIC
 CAST(value_as_string AS STRING) AS value_as_string,
 value_as_concept_id,
 qualifier_concept_id,
 unit_concept_id,
 provider_id,
 visit_occurrence_id,
 CAST(observation_source_value AS STRING) AS observation_source_value,
 observation_source_concept_id,
 CAST(unit_source_value AS STRING) AS unit_source_value,
 CAST(qualifier_source_value AS STRING) AS qualifier_source_value
FROM omop_cdm.observation;

CREATE TABLE omop_cdm_kudu.fact_relationship
PRIMARY KEY (domain_concept_id_1, fact_id_1, domain_concept_id_2, fact_id_2)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = '${var:KUDU_MASTER}'
)
AS
SELECT * FROM omop_cdm.fact_relationship;

CREATE TABLE omop_cdm_kudu.`location`
PRIMARY KEY (location_id)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = '${var:KUDU_MASTER}'
)
AS
SELECT
 location_id,
 CAST(address_1 AS STRING) AS address_1,
 CAST(address_2 AS STRING) AS address_2,
 CAST(city AS STRING) AS city,
 CAST(state AS STRING) AS state,
 CAST(zip AS STRING) AS zip,
 CAST(county AS STRING) AS county,
 CAST(location_source_value AS STRING) AS location_source_value
FROM omop_cdm.`location`;

CREATE TABLE omop_cdm_kudu.care_site
PRIMARY KEY (care_site_id)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = '${var:KUDU_MASTER}'
)
AS
SELECT
 care_site_id,
 CAST(care_site_name AS STRING) AS care_site_name,
 place_of_service_concept_id,
 location_id,
 CAST(care_site_source_value AS STRING) AS care_site_source_value,
 CAST(place_of_service_source_value AS STRING) AS place_of_service_source_value
FROM omop_cdm.care_site;

CREATE TABLE omop_cdm_kudu.provider
PRIMARY KEY (provider_id)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = '${var:KUDU_MASTER}'
)
AS
SELECT
 provider_id,
 CAST(provider_name AS STRING) AS provider_name,
 CAST(NPI AS STRING) AS NPI,
 CAST(DEA AS STRING) AS DEA,
 specialty_concept_id,
 care_site_id,
 year_of_birth,
 gender_concept_id,
 CAST(provider_source_value AS STRING) AS provider_source_value,
 CAST(specialty_source_value AS STRING) AS specialty_source_value,
 specialty_source_concept_id,
 CAST(gender_source_value AS STRING) AS gender_source_value,
 gender_source_concept_id
FROM omop_cdm.provider;

CREATE TABLE omop_cdm_kudu.payer_plan_period
PRIMARY KEY (payer_plan_period_id)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = '${var:KUDU_MASTER}'
)
AS
SELECT
 payer_plan_period_id,
 person_id,
 TO_UTC_TIMESTAMP(CONCAT_WS('-', SUBSTR(CAST(payer_plan_period_start_date AS STRING), 1, 4), SUBSTR(CAST(payer_plan_period_start_date AS STRING), 5, 2), SUBSTR(CAST(payer_plan_period_start_date AS STRING), 7, 2)), 'UTC') AS payer_plan_period_start_date,
 TO_UTC_TIMESTAMP(CONCAT_WS('-', SUBSTR(CAST(payer_plan_period_end_date AS STRING), 1, 4), SUBSTR(CAST(payer_plan_period_end_date AS STRING), 5, 2), SUBSTR(CAST(payer_plan_period_end_date AS STRING), 7, 2)), 'UTC') AS payer_plan_period_end_date,
 CAST(payer_source_value AS STRING) AS payer_source_value,
 CAST(plan_source_value AS STRING) AS plan_source_value,
 CAST(family_source_value AS STRING) AS family_source_value
FROM omop_cdm.payer_plan_period;


/* The individual cost tables are being phased out and will disappear soon

CREATE TABLE omop_cdm_kudu.visit_cost
PRIMARY KEY (visit_cost_id)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = '${var:KUDU_MASTER}'
)
AS
SELECT * FROM omop_cdm.visit_cost;

CREATE TABLE omop_cdm_kudu.procedure_cost
PRIMARY KEY (procedure_cost_id)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = '${var:KUDU_MASTER}'
)
AS
SELECT
 procedure_cost_id,
 procedure_occurrence_id,
 currency_concept_id,
 paid_copay, -- NUMERIC
 paid_coinsurance, -- NUMERIC
 paid_toward_deductible, -- NUMERIC
 paid_by_payer, -- NUMERIC
 paid_by_coordination_benefits, -- NUMERIC
 total_out_of_pocket, -- NUMERIC
 total_paid, -- NUMERIC
 revenue_code_concept_id,
 payer_plan_period_id,
 CAST(revenue_code_source_value AS STRING) AS revenue_code_source_value
FROM omop_cdm.procedure_cost;

CREATE TABLE omop_cdm_kudu.drug_cost
PRIMARY KEY (drug_cost_id)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = '${var:KUDU_MASTER}'
)
AS
SELECT * FROM omop_cdm.drug_cost;

CREATE TABLE omop_cdm_kudu.device_cost
PRIMARY KEY (device_cost_id)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = '${var:KUDU_MASTER}'
)
AS
SELECT * FROM omop_cdm.device_cost;
*/

CREATE TABLE omop_cdm_kudu.cost
PRIMARY KEY (cost_id)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = '${var:KUDU_MASTER}'
)
AS
SELECT
 cost_id,
 cost_event_id,
 CAST(cost_domain_id AS STRING) AS cost_domain_id,
 cost_type_concept_id,
 currency_concept_id,
 CAST(total_charge AS FLOAT) AS total_charge, -- NUMERIC
 CAST(total_cost AS FLOAT) AS total_cost, -- NUMERIC
 CAST(total_paid AS FLOAT) AS total_paid, -- NUMERIC
 CAST(paid_by_payer AS FLOAT) AS paid_by_payer, -- NUMERIC
 CAST(paid_by_patient AS FLOAT) AS paid_by_patient, -- NUMERIC
 CAST(paid_patient_copay AS FLOAT) AS paid_patient_copay, -- NUMERIC
 CAST(paid_patient_coinsurance AS FLOAT) AS paid_patient_coinsurance, -- NUMERIC
 CAST(paid_patient_deductible AS FLOAT) AS paid_patient_deductible, -- NUMERIC
 CAST(paid_by_primary AS FLOAT) AS paid_by_primary, -- NUMERIC
 CAST(paid_ingredient_cost AS FLOAT) AS paid_ingredient_cost, -- NUMERIC
 CAST(paid_dispensing_fee AS FLOAT) AS paid_dispensing_fee, -- NUMERIC
 payer_plan_period_id,
 CAST(amount_allowed AS FLOAT) AS amount_allowed, -- NUMERIC
 revenue_code_concept_id,
 CAST(reveue_code_source_value AS STRING) AS reveue_code_source_value
FROM omop_cdm.cost;

CREATE TABLE omop_cdm_kudu.cohort
PRIMARY KEY (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = '${var:KUDU_MASTER}'
)
AS
SELECT
 cohort_definition_id,
 subject_id,
 TO_UTC_TIMESTAMP(CONCAT_WS('-', SUBSTR(CAST(cohort_start_date AS STRING), 1, 4), SUBSTR(CAST(cohort_start_date AS STRING), 5, 2), SUBSTR(CAST(cohort_start_date AS STRING), 7, 2)), 'UTC') AS cohort_start_date,
 TO_UTC_TIMESTAMP(CONCAT_WS('-', SUBSTR(CAST(cohort_end_date AS STRING), 1, 4), SUBSTR(CAST(cohort_end_date AS STRING), 5, 2), SUBSTR(CAST(cohort_end_date AS STRING), 7, 2)), 'UTC') AS cohort_end_date
FROM omop_cdm.cohort;

CREATE TABLE omop_cdm_kudu.cohort_attribute
PRIMARY KEY (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date, attribute_definition_id)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = '${var:KUDU_MASTER}'
)
AS
SELECT
 cohort_definition_id,
 TO_UTC_TIMESTAMP(CONCAT_WS('-', SUBSTR(CAST(cohort_start_date AS STRING), 1, 4), SUBSTR(CAST(cohort_start_date AS STRING), 5, 2), SUBSTR(CAST(cohort_start_date AS STRING), 7, 2)), 'UTC') AS cohort_start_date,
 TO_UTC_TIMESTAMP(CONCAT_WS('-', SUBSTR(CAST(cohort_end_date AS STRING), 1, 4), SUBSTR(CAST(cohort_end_date AS STRING), 5, 2), SUBSTR(CAST(cohort_end_date AS STRING), 7, 2)), 'UTC') AS cohort_end_date,
 subject_id,
 attribute_definition_id,
 value_as_number, -- NUMERIC
 value_as_concept_id
FROM omop_cdm.cohort_attribute;

CREATE TABLE omop_cdm_kudu.drug_era
PRIMARY KEY (drug_era_id)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = '${var:KUDU_MASTER}'
)
AS
SELECT
 drug_era_id,
 person_id,
 drug_concept_id,
 TO_UTC_TIMESTAMP(CONCAT_WS('-', SUBSTR(CAST(drug_era_start_date AS STRING), 1, 4), SUBSTR(CAST(drug_era_start_date AS STRING), 5, 2), SUBSTR(CAST(drug_era_start_date AS STRING), 7, 2)), 'UTC') AS drug_era_start_date,
 TO_UTC_TIMESTAMP(CONCAT_WS('-', SUBSTR(CAST(drug_era_end_date AS STRING), 1, 4), SUBSTR(CAST(drug_era_end_date AS STRING), 5, 2), SUBSTR(CAST(drug_era_end_date AS STRING), 7, 2)), 'UTC') AS drug_era_end_date,
 drug_exposure_count,
 gap_days
FROM omop_cdm.drug_era;

CREATE TABLE omop_cdm_kudu.dose_era
PRIMARY KEY (dose_era_id)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = '${var:KUDU_MASTER}'
)
AS
SELECT
 dose_era_id,
 person_id,
 drug_concept_id,
 unit_concept_id,
 dose_value, -- NUMERIC
 TO_UTC_TIMESTAMP(CONCAT_WS('-', SUBSTR(CAST(dose_era_start_date AS STRING), 1, 4), SUBSTR(CAST(dose_era_start_date AS STRING), 5, 2), SUBSTR(CAST(dose_era_start_date AS STRING), 7, 2)), 'UTC') AS dose_era_start_date,
 TO_UTC_TIMESTAMP(CONCAT_WS('-', SUBSTR(CAST(dose_era_end_date AS STRING), 1, 4), SUBSTR(CAST(dose_era_end_date AS STRING), 5, 2), SUBSTR(CAST(dose_era_end_date AS STRING), 7, 2)), 'UTC') AS dose_era_end_date
FROM omop_cdm.dose_era;

CREATE TABLE omop_cdm_kudu.condition_era
PRIMARY KEY (condition_era_id)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = '${var:KUDU_MASTER}'
)
AS
SELECT
 condition_era_id,
 person_id,
 condition_concept_id,
 TO_UTC_TIMESTAMP(CONCAT_WS('-', SUBSTR(CAST(condition_era_start_date AS STRING), 1, 4), SUBSTR(CAST(condition_era_start_date AS STRING), 5, 2), SUBSTR(CAST(condition_era_start_date AS STRING), 7, 2)), 'UTC') AS condition_era_start_date,
 TO_UTC_TIMESTAMP(CONCAT_WS('-', SUBSTR(CAST(condition_era_end_date AS STRING), 1, 4), SUBSTR(CAST(condition_era_end_date AS STRING), 5, 2), SUBSTR(CAST(condition_era_end_date AS STRING), 7, 2)), 'UTC') AS condition_era_end_date,
 condition_occurrence_count
FROM omop_cdm.condition_era;

