{{ config(materialized='table', database=var('lake_catalog'), alias='customers_snapshot') }}

-- STREAMING layer, snapshot flavour: the full current state, replaced as a whole by the sink
-- (only when it contains at least one row; an empty result never wipes the target).

SELECT
    {{ var('tenant_key') }} AS {{ var('tenant_key') }},
    customer_id,
    name,
    email,
    country,
    created_at
FROM {{ ref('customers_current') }}
