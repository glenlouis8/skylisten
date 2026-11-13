{{ config(materialized='table') }}

with complete_days as (
    select date
    from {{ ref('posts_per_hour') }}
    group by date
    having count(*) = 24
),

daily as (
    select date, tag, authors
    from {{ ref('hashtags_per_day') }}
    where date in (select date from complete_days)
),

with_prev as (
    select
        d.date,
        d.tag,
        d.authors,
        coalesce(p.authors, 0) as prev_authors
    from daily d
    left join daily p
        on p.tag = d.tag and p.date = d.date - 1
)

select
    date,
    tag,
    authors,
    prev_authors,
    round(authors::double / (prev_authors + 5), 2) as spike_score
from with_prev
where authors >= 50
  and date > (select min(date) from complete_days)
order by date, spike_score desc