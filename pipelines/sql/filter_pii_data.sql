SELECT
$__not_pii_data__
FROM {{ source('$__dataset_source__', '$__table_source__') }}
