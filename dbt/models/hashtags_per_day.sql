{{ config(materialized='table') }}

with tags as (
    select
        date,
        author_hash,
        unnest(list_distinct(regexp_extract_all(lower(text), '#\w+'))) as tag
    from read_parquet('../data/silver/posts/*/*/*.parquet', hive_partitioning = true)
)

select
    date,
    tag,
    count(*) as posts,
    count(distinct author_hash) as authors
from tags
group by date, tag
having count(distinct author_hash) >= 5
order by date, authors desc