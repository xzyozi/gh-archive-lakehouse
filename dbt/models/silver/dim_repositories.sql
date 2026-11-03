{{
    config(
        materialized='incremental',
        unique_key='repo_name',
        cluster_by=["repo_name"],
        description='Repository dimension table with incremental creation date tracking (DS-001 2.4, 論点②)'
    )
}}

with create_events as (
    -- 当日分の CreateEvent (repository) から開設日時を抽出
    select
        repo_name,
        min(event_at) as repo_created_at
    from {{ ref('stg_github_events') }}
    where event_type = 'CreateEvent'
      and json_value(payload.ref_type) = 'repository'
    {% if is_incremental() %}
      and event_date >= date_sub(current_date(), interval 3 day)
    {% endif %}
    group by repo_name
),

active_repos as (
    -- 当日活動のあった全リポジトリ
    select distinct
        repo_name
    from {{ ref('stg_github_events') }}
    {% if is_incremental() %}
    where event_date >= date_sub(current_date(), interval 3 day)
    {% endif %}
),

existing_records as (
    -- 既存テーブルから開設日時およびメタデータを引き継ぐ (インクリメンタル累積保持)
    {% if is_incremental() %}
    select
        repo_name,
        repo_created_at,
        primary_language,
        topics,
        description,
        total_stars,
        total_forks
    from {{ this }}
    {% else %}
    select
        cast(null as string) as repo_name,
        cast(null as timestamp) as repo_created_at,
        cast(null as string) as primary_language,
        cast([] as array<string>) as topics,
        cast(null as string) as description,
        cast(null as int64) as total_stars,
        cast(null as int64) as total_forks
    where false
    {% endif %}
)

select
    a.repo_name,
    coalesce(e.primary_language, cast(null as string)) as primary_language,
    coalesce(e.topics, cast([] as array<string>)) as topics,
    coalesce(e.description, cast(null as string)) as description,
    -- 既存の開設日を最優先し、未保持なら新規 CreateEvent の値で補完
    coalesce(e.repo_created_at, c.repo_created_at) as repo_created_at,
    e.total_stars,
    e.total_forks,
    current_timestamp() as metadata_updated_at
from active_repos a
left join create_events c on a.repo_name = c.repo_name
left join existing_records e on a.repo_name = e.repo_name
