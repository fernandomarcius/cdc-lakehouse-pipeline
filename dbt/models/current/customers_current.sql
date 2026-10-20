{{ config(materialized='view', database=var('lake_catalog'), alias='customers_current') }}

-- CURRENT layer, state: the latest non-deleted version of every customer, rebuilt from the event store.
-- The sort key (LSN, seqval, operation) is a total order, so ties cannot flip between runs.

SELECT * FROM (
    SELECT
        e.*,
        ROW_NUMBER() OVER (
            PARTITION BY {{ var('tenant_key') }}, customer_id
            ORDER BY _src_start_lsn DESC, _src_seqval DESC, operation DESC
        ) AS rn
    FROM {{ ref('customers_events') }} e
)
WHERE rn = 1 AND deleted = 0
