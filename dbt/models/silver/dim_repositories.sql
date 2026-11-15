{{
    config(
        materialized='incremental',
        unique_key='repo_name',
        cluster_by=["repo_name"],
        description='Repository dimension table joining repo_metadata with incremental CreateEvent tracking (DS-001 2.4②, DD-002 7)'
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
      and {{ incremental_window('event_date') }}
    {% endif %}
    group by repo_name
),

active_repos as (
    -- 当日活動のあった全リポジトリ
    select distinct
        repo_name
    from {{ ref('stg_github_events') }}
    {% if is_incremental() %}
    where {{ incremental_window('event_date') }}
    {% endif %}
),

existing_records as (
    -- 既存テーブルから CreateEvent 由来の開設日時のみを引き継ぐ (インクリメンタル累積保持)
    {% if is_incremental() %}
    select
        repo_name,
        repo_created_at
    from {{ this }}
    {% else %}
    select
        cast(null as string) as repo_name,
        cast(null as timestamp) as repo_created_at
    where false
    {% endif %}
),

repo_metadata as (
    -- Airflow Enrichment が更新する専用テーブル (DD-002 7)
    select
        repo_name,
        primary_language,
        topics,
        description,
        repo_created_at,
        total_stars,
        total_forks,
        metadata_updated_at
    from {{ source('silver', 'repo_metadata') }}
)

select
    a.repo_name,
    r.primary_language,
    coalesce(r.topics, cast([] as array<string>)) as topics,
    r.description,
    -- API 由来の開設日を優先し、未補完時は CreateEvent 由来の累積値を使用
    coalesce(r.repo_created_at, e.repo_created_at, c.repo_created_at) as repo_created_at,
    r.total_stars,
    r.total_forks,
    r.metadata_updated_at
from active_repos a
left join create_events c on a.repo_name = c.repo_name
left join existing_records e on a.repo_name = e.repo_name
left join repo_metadata r on a.repo_name = r.repo_name
