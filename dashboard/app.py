import duckdb
import streamlit as st

st.title("SkyListen")

con = duckdb.connect("data/gold.duckdb", read_only=True)

brands = con.sql("""
    select date, brand, mentions
    from brand_mentions_per_day
    where date in (
        select date from posts_per_hour group by date having count(*) = 24
    )
    order by date
    """).df()

latest = con.sql("select max(date) from trending_tags").fetchall()[0][0]

trending = con.sql(f"""
    select tag, authors, prev_authors, spike_score
    from trending_tags
    where date = '{latest}'
    order by spike_score desc
    limit 15
    """).df()

popular = con.sql(f"""
    select tag, authors
    from hashtags_per_day
    where date = '{latest}'
    order by authors desc
    limit 15
    """).df()

con.close()  # let go of the database so dbt can write to it

st.subheader(f"Trending hashtags, {latest}")
st.caption(
    "Spike score = people using the tag today vs yesterday. Counts different people, not posts, so bots don't count extra."
)
st.dataframe(trending, hide_index=True)

st.subheader(f"Most used hashtags, {latest}")
st.bar_chart(popular, x="tag", y="authors")

st.subheader("Brand mentions per day")
st.line_chart(brands, x="date", y="mentions", color="brand")
