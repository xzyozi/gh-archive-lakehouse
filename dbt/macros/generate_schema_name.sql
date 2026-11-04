{#
  dbt 既定の generate_schema_name は「profile の dataset + _ + カスタムスキーマ」に連結するため、
  Terraform が作成するデータセット (<prefix>_bronze / <prefix>_silver / <prefix>_gold) と一致しない。
  カスタムスキーマ指定時は「<GCP_DATASET_PREFIX>_<カスタムスキーマ>」をそのまま出力先とする。
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ env_var('GCP_DATASET_PREFIX', 'octopulse') }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
