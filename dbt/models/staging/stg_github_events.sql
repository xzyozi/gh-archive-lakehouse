{{
    config(
        materialized='view',
        description='Bronze raw events view with column aliases'
    )
}}

select
    id as event_id,
    type as event_type,
    actor_login,
    repo_id,
    repo_name,
    payload,
    created_at as event_at,
    event_date
from {{ source('bronze', 'stg_github_events') }}
where id is not null
  and type is not null
  and repo_name is not null
  and created_at is not null
  and event_date is not null
