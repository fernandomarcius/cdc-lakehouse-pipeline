{{ config(database=var('lake_catalog'), alias='customers_ct') }}

-- RAW layer: the CDC change table, untouched, appended by LSN.
-- The LSN is monotonic per source database, so a single global high mark is enough here.
SELECT *
FROM {{ source('appdb', 'dbo_customers_ct') }}
{% if is_incremental() %}
WHERE "__$start_lsn" > (SELECT COALESCE(MAX("__$start_lsn"), X'00') FROM {{ this }})
{% endif %}
