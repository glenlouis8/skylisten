{{ config(materialized='table') }}

with brands as (
    select * from (values ('nvidia'), ('apple'), ('tesla')) as t(brand)
),

posts as (
    select date, text
    from read_parquet('../data/silver/posts/*/*/*.parquet', hive_partitioning = true)
)

select
    posts.date,
    brands.brand,
    count(*) as mentions
from posts
join brands
    on regexp_matches(posts.text, '\b' || brands.brand || '\b', 'i')
group by posts.date, brands.brand
order by posts.date, brands.brand