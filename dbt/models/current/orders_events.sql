{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    database=var('lake_catalog'),
    alias='orders_events'
) }}

-- CURRENT layer, event store for `orders`. Same rules as customers_events (see the comments there).

WITH src AS (
    SELECT
        ct."__$start_lsn"  AS _src_start_lsn,
        ct."__$seqval"     AS _src_seqval,
        ct."__$operation"  AS operation,
        ltm.tran_end_time  AS commit_time,
        ROW_NUMBER() OVER (
            PARTITION BY ct."__$start_lsn"
            ORDER BY ct."__$seqval" ASC, ct."__$operation" ASC
        )                  AS seq_within_lsn,
        ct.{{ var('tenant_key') }},
        ct.order_id,
        ct.customer_id,
        ct.status,
        ct.total_amount,
        ct.ordered_at
    FROM {{ ref('raw_orders_ct') }} ct
    LEFT JOIN {{ ref('raw_lsn_time_mapping') }} ltm
        ON ltm.start_lsn = ct."__$start_lsn"
    WHERE ct."__$operation" != 3
      AND ct."__$start_lsn" <= (SELECT COALESCE(MAX(start_lsn), X'00') FROM {{ ref('raw_lsn_time_mapping') }})
    {% if is_incremental() %}
      AND ct."__$start_lsn" > (SELECT COALESCE(MAX(_src_start_lsn), X'00') FROM {{ this }})
    {% endif %}
)
SELECT
    CAST(uuid() AS VARCHAR)                                        AS event_id,
    {{ var('tenant_key') }},
    _src_start_lsn,
    _src_seqval,
    operation,
    CASE WHEN operation = 1 THEN 1 ELSE 0 END                      AS deleted,
    commit_time,
    date_add('millisecond', (seq_within_lsn - 1) * 5, commit_time) AS recorded_at,
    order_id,
    customer_id,
    status,
    total_amount,
    ordered_at
FROM src
