{{ config(database=var('lake_catalog'), alias='lsn_time_mapping') }}

-- Append-only copy of the LSN -> commit time mapping.
SELECT start_lsn, tran_end_time
FROM {{ source('appdb', 'lsn_time_mapping') }}
{% if is_incremental() %}
WHERE start_lsn > (SELECT COALESCE(MAX(start_lsn), X'00') FROM {{ this }})
{% endif %}
