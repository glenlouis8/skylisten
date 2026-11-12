select
    date,
    hour,
    count(*) as posts
from read_parquet('../data/silver/posts/*/*/*.parquet', hive_partitioning = true)
group by date, hour
order by date, hour

{{ config(materialized='table') }}