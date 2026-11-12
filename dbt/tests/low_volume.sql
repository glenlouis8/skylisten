select *
from {{ ref('posts_per_hour') }}
where posts < 30000