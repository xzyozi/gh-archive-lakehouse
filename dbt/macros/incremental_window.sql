{#
  インクリメンタル実行の再処理ウィンドウ (BD-001 NFR-004: 同一日付の再実行で結果が変わらない)。

  基準日 run_date:
    - dbt の var `run_date` (YYYY-MM-DD) を渡すとその日付。Airflow からは {{ ds }} を渡す。
    - 未指定なら current_date()。ローカルでの手動実行はこちら。
  ウィンドウ: [run_date - lookback_days, run_date]
    - 下限は遅延到着イベントの取りこぼし防止、上限は過去日の再実行 (backfill) を冪等にするためのもの。
    - lookback_days は dbt_project.yml の var (既定 3)。--vars で上書きできる。
#}
{% macro run_date_expr() -%}
    {%- set run_date = var('run_date', none) -%}
    {%- if run_date is none or run_date | string | length == 0 -%}
        current_date()
    {%- else -%}
        {#- SQL に埋め込むため、日付形式以外は受け付けない -#}
        {%- if not modules.re.match('^[0-9]{4}-[0-9]{2}-[0-9]{2}$', run_date | string) -%}
            {{ exceptions.raise_compiler_error("var run_date must be YYYY-MM-DD, got: " ~ run_date) }}
        {%- endif -%}
        date('{{ run_date }}')
    {%- endif -%}
{%- endmacro %}

{% macro incremental_window(date_column) -%}
    {{ date_column }} between date_sub({{ run_date_expr() }}, interval {{ var('lookback_days') }} day)
        and {{ run_date_expr() }}
{%- endmacro %}
