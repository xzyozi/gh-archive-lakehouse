{{
    config(
        materialized='incremental',
        unique_key='event_id',
        partition_by={
            "field": "event_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=["repo_name", "event_type"],
        partition_expiration_days=90,
        description='Cleansed GitHub events normalized fact table (DS-001 2.4)'
    )
}}

select
    event_id,
    event_type,
    actor_login,
    repo_name,
    json_value(payload.action) as event_action,
    event_at,
    event_date
from {{ ref('stg_github_events') }}

{% if is_incremental() %}
where event_date >= date_sub(current_date(), interval {{ var('lookback_days') }} day)
{% endif %}
