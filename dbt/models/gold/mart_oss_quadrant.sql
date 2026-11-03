{{
    config(
        materialized='incremental',
        unique_key=['target_date', 'repo_name'],
        partition_by={
            "field": "target_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=["quadrant_segment", "repo_name"],
        description='CHAOSS Attention x Engagement 4-quadrant gold mart (DS-001 2.5, 2.6)'
    )
}}

with daily_events as (
    -- 当日分のイベントファクト
    select
        event_date as target_date,
        repo_name,
        event_id,
        event_type
    from {{ ref('fct_github_events') }}
    {% if is_incremental() %}
    where event_date >= date_sub(current_date(), interval 3 day)
    {% endif %}
),

daily_aggregates as (
    -- リポジトリ単位の日次イベント別集計 (論点③: COUNT(DISTINCT event_id) による重複排除)
    select
        target_date,
        repo_name,
        count(distinct if(event_type = 'WatchEvent', event_id, null)) as daily_stars,
        count(distinct if(event_type = 'ForkEvent', event_id, null)) as daily_forks,
        count(distinct if(event_type = 'IssuesEvent', event_id, null)) as daily_issues,
        count(distinct if(event_type = 'PullRequestEvent', event_id, null)) as daily_prs
    from daily_events
    group by target_date, repo_name
),

enriched_with_dim as (
    -- dim_repositories と結合して開設経過日数およびメタデータを補完 (論点②・④)
    select
        a.target_date,
        a.repo_name,
        d.primary_language,
        if(d.topics is not null and array_length(d.topics) > 0, array_to_string(d.topics, ', '), null) as topics_display,
        -- repo_created_at から経過日数を算出 (未取得時は NULL)
        if(d.repo_created_at is not null, date_diff(a.target_date, date(d.repo_created_at), day), null) as repo_age_days,
        a.daily_stars,
        a.daily_forks,
        a.daily_issues,
        a.daily_prs,
        -- 異常値検知フラグ (DS-001 2.6⑤: 足切り前集計で評価)
        (a.daily_stars >= 10 and (a.daily_forks + a.daily_prs) = 0) as is_suspicious_star_surge,
        (a.daily_prs >= 50 and a.daily_stars = 0) as is_bot_generated_prs,
        (d.repo_created_at is not null
            and date_diff(a.target_date, date(d.repo_created_at), day) <= 7
            and a.daily_stars >= 10) as is_newborn_buzz,
        -- Engagement Depth 加重スコア: Fork*3 + PR*2 + Issue*1 (TEST-002確定値)
        (a.daily_forks * 3) + (a.daily_prs * 2) + (a.daily_issues * 1) as raw_engagement_score
    from daily_aggregates a
    left join {{ ref('dim_repositories') }} d on a.repo_name = d.repo_name
),

screened_population as (
    -- ① 母集団スクリーニング (DS-001 2.6①: daily_stars >= 3 かつ アクティビティ >= 1)
    select
        *
    from enriched_with_dim
    where daily_stars >= 3
      and (daily_forks + daily_issues + daily_prs) >= 1
),

percentiles as (
    -- ② Attention Velocity & ③ Engagement Depth パーセンタイル計算 (DS-001 2.6②, ③)
    select
        target_date,
        repo_name,
        primary_language,
        topics_display,
        repo_age_days,
        daily_stars,
        daily_forks,
        daily_issues,
        daily_prs,
        percent_rank() over (
            partition by target_date order by daily_stars asc
        ) as attention_velocity,
        percent_rank() over (
            partition by target_date order by raw_engagement_score asc
        ) as engagement_depth,
        is_suspicious_star_surge,
        is_bot_generated_prs,
        is_newborn_buzz
    from screened_population
)

-- ④ 4象限セグメンテーション境界値 (Cutoff = 0.80, DS-001 2.6④)
select
    target_date,
    repo_name,
    primary_language,
    topics_display,
    repo_age_days,
    daily_stars,
    daily_forks,
    daily_issues,
    daily_prs,
    round(attention_velocity, 4) as attention_velocity,
    round(engagement_depth, 4) as engagement_depth,
    case
        when attention_velocity >= 0.80 and engagement_depth >= 0.80 then 'Hyper-Growth'
        when attention_velocity >= 0.80 and engagement_depth < 0.80 then 'Viral Spotlight'
        when attention_velocity < 0.80 and engagement_depth >= 0.80 then 'Silent Utility'
        else 'Niche / Emerging'
    end as quadrant_segment,
    is_suspicious_star_surge,
    is_bot_generated_prs,
    is_newborn_buzz
from percentiles
