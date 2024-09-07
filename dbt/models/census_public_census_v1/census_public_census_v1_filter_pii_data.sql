SELECT
	key,
	population,
	population_female,
	population_age_00_09,
	population_age_10_19,
	population_age_20_29,
	population_age_30_39,
	population_age_40_49,
	population_age_50_59,
	population_age_60_69,
	population_age_70_79,
	population_age_80_and_older
FROM {{ source('census_public', 'census_v1') }}
