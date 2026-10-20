{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    database=var('lake_catalog'),
    alias='customers_events'
) }}

-- CURRENT layer, event store: one row per change event, insert-only.
--
-- SQL Server CDC operations: 1 = delete, 2 = insert, 3 = update (before image), 4 = update (after image).
-- The before image (3) carries no new information, so it is dropped; the rest becomes `operation` and `deleted`.
--
-- Ordering is deterministic. Several events can share one commit LSN, so each event gets
-- `seq_within_lsn` = position by (__$seqval, __$operation); `recorded_at` is the commit time plus 5 ms per position.
-- Re-running the model over the same input always yields the same ids/order, which is what makes "latest event wins" safe.
--
-- Only LSNs that already have a commit time are ingested. The rest waits for the next run (the mapping catches up),
-- so no event is lost and no event ever gets a NULL time.

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
        ct.customer_id,
        ct.name,
        ct.email,
        ct.country,
        ct.created_at
    FROM {{ ref('raw_customers_ct') }} ct
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
    customer_id,
    name,
    email,
    country,
    created_at
FROM src
