{#
  Use the model's `schema` config as-is. dbt's default concatenates `<target schema>_<custom schema>`,
  which would turn `raw` into `raw_raw`; here the layer name is the schema name.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
