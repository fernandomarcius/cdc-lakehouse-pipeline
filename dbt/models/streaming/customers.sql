{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    database=var('lake_catalog'),
    alias='customers'
) }}

-- STREAMING layer: the event store projected into the exact shape of the target table, ready for the sink.
-- Incremental runs only read events newer than each tenant's watermark (see macros/high_watermark.sql).

WITH base AS (
    SELECT *
    FROM {{ ref('customers_events') }} src
    {% if is_incremental() %}
    WHERE {{ high_watermark(this, source_col='_src_start_lsn') }}
    {% endif %}
)
SELECT
    event_id                                  AS event_id,
    {{ var('tenant_key') }}                   AS {{ var('tenant_key') }},
    customer_id                               AS customer_id,
    name                                      AS name,
    email                                     AS email,
    country                                   AS country,
    created_at                                AS created_at,
    recorded_at                               AS recorded_at,
    CAST(deleted AS integer)                  AS deleted,
    from_big_endian_64(substr(_src_seqval, 3)) AS seq_val,
    operation                                 AS operation,
    _src_start_lsn                            AS _src_start_lsn
FROM base
