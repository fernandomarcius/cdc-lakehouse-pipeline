{#
  Operation helper: `dbt run-operation drop_table --args '{table_name: customers, schema: streaming}'`.
  Dropping a streaming table makes the next run rebuild it from scratch (the watermark is read from it).
#}
{% macro drop_table(table_name, schema='streaming', database=none) %}
    {%- set catalog = database or var('lake_catalog') -%}
    {% do run_query('DROP TABLE IF EXISTS ' ~ catalog ~ '.' ~ schema ~ '.' ~ table_name) %}
    {{ log('dropped ' ~ catalog ~ '.' ~ schema ~ '.' ~ table_name, info=true) }}
{% endmacro %}
