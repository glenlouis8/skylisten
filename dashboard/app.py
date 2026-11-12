import duckdb
import streamlit as st

st.title("SkyListen")

con = duckdb.connect("data/gold.duckdb", read_only=True)
df = con.sql(
    "select date, brand, mentions from brand_mentions_per_day order by date"
).df()

st.line_chart(df, x="date", y="mentions", color="brand")
