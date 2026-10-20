{#
  Incremental filter for the streaming layer: only rows newer than each tenant's high watermark in the target.

  The obvious way to write it is a correlated subquery:

      src.__start_lsn > (SELECT MAX(t._src_start_lsn) FROM {{ this }} t WHERE t.tenant_id = src.tenant_id)

  Distributed SQL engines plan that as a join against the source, so the filter only runs AFTER the whole source table was
  scanned (and may spill) even when there is nothing new. Here the watermark of every tenant is read once, at compile time,
  and injected as a LITERAL:

      (tenant_id = 1 AND lsn > X'00AB...') OR (tenant_id = 2 AND lsn > X'00CD...') OR tenant_id IS NULL OR tenant_id NOT IN (1, 2)

  Literals are pushed down to the scan, so files and row groups that cannot match are never read.

  Semantics match the correlated form: a tenant with no row in the target (or a NULL tenant) receives everything (watermark X'00').
  Numeric tenant ids are emitted bare, anything else as a quoted string.
  During `dbt parse` (execute = false) the macro returns `1 = 1`.

  Arguments:
    relation      the target table (`this`)
    tenant_key    tenant column; defaults to var('tenant_key')
    source_col    LSN column in the source relation (the query that wraps the filter)
    target_col    LSN column in the target relation
#}
{% macro high_watermark(relation, tenant_key=none, source_col='_src_start_lsn', target_col='_src_start_lsn') -%}
  {%- set key = tenant_key or var('tenant_key') -%}
  {%- if not execute -%}
    1 = 1
  {%- else -%}
    {%- set query -%}
      SELECT {{ key }} AS tenant, to_hex(MAX({{ target_col }})) AS mark
      FROM {{ relation }}
      WHERE {{ key }} IS NOT NULL
      GROUP BY {{ key }}
    {%- endset -%}
    {%- set rows = run_query(query).rows -%}
    {%- set terms = [] -%}
    {%- set known = [] -%}
    {%- for row in rows -%}
      {%- set lit = (row[0] | string) if row[0] is number else "'" ~ (row[0] | string | replace("'", "''")) ~ "'" -%}
      {%- do known.append(lit) -%}
      {%- do terms.append("(" ~ key ~ " = " ~ lit ~ " AND " ~ source_col ~ " > X'" ~ row[1] ~ "')") -%}
    {%- endfor -%}
    {%- if known | length > 0 -%}
      {%- do terms.append(key ~ " IS NULL OR " ~ key ~ " NOT IN (" ~ (known | join(", ")) ~ ")") -%}
      ({{ terms | join(" OR ") }})
    {%- else -%}
      1 = 1
    {%- endif -%}
  {%- endif -%}
{%- endmacro %}
