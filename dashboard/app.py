import duckdb
import streamlit as st

st.title("SkyListen")

con = duckdb.connect("data/gold.duckdb", read_only=True)
df = con.sql("""
    select date, brand, mentions
    from brand_mentions_per_day
    where date in (
        select date from posts_per_hour group by date having count(*) = 24
    )
    order by date
    """).df()
con.close()  # let go of the database so dbt can write to it

st.line_chart(df, x="date", y="mentions", color="brand")
