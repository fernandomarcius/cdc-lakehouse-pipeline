{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    database=var('lake_catalog'),
    alias='orders'
) }}

-- STREAMING layer for orders (same contract as streaming/customers.sql).

WITH base AS (
    SELECT *
    FROM {{ ref('orders_events') }} src
    {% if is_incremental() %}
    WHERE {{ high_watermark(this, source_col='_src_start_lsn') }}
    {% endif %}
)
SELECT
    event_id                                  AS event_id,
    {{ var('tenant_key') }}                   AS {{ var('tenant_key') }},
    order_id                                  AS order_id,
    customer_id                               AS customer_id,
    status                                    AS status,
    total_amount                              AS total_amount,
    ordered_at                                AS ordered_at,
    recorded_at                               AS recorded_at,
    CAST(deleted AS integer)                  AS deleted,
    from_big_endian_64(substr(_src_seqval, 3)) AS seq_val,
    operation                                 AS operation,
    _src_start_lsn                            AS _src_start_lsn
FROM base
