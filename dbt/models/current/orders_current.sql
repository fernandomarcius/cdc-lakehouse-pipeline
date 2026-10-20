{{ config(materialized='view', database=var('lake_catalog'), alias='orders_current') }}

-- CURRENT layer, state: the latest non-deleted version of every order.

SELECT * FROM (
    SELECT
        e.*,
        ROW_NUMBER() OVER (
            PARTITION BY {{ var('tenant_key') }}, order_id
            ORDER BY _src_start_lsn DESC, _src_seqval DESC, operation DESC
        ) AS rn
    FROM {{ ref('orders_events') }} e
)
WHERE rn = 1 AND deleted = 0
