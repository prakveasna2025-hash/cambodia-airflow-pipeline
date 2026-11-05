"""
Cambodia Data Warehouse Dashboard

Run: uv run streamlit run dashboard/app.py

Requires the warehouse Postgres to be running on localhost:5434
(container `cambodia_dw_postgres`, host port 5434 → container 5432).
"""

import os

import altair as alt
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Cambodia Data Warehouse",
    page_icon="🇰🇭",
    layout="wide",
)

DB_URL = os.environ.get(
    "WAREHOUSE_DB_URL",
    "postgresql+psycopg2://airflow:changeme@127.0.0.1:5434/airflow",
)


# ---------------------------------------------------------------------------
# Cached resources — connection pool (once per session)
# ---------------------------------------------------------------------------
@st.cache_resource
def get_engine():
    """Pooled SQLAlchemy engine — created once, reused across reruns."""
    return create_engine(DB_URL, pool_size=5, max_overflow=2)


# ---------------------------------------------------------------------------
# Cached data loaders (TTL 5 minutes)
# ---------------------------------------------------------------------------
@st.cache_data(ttl=300)
def load_kpis():
    """Top-line metrics — single round trip."""
    sql = text("""
        SELECT
            (SELECT COUNT(*) FROM dbt_dev_marts.dim_province)          AS provinces,
            (SELECT SUM(population) FROM dbt_dev_marts.fact_population) AS total_population,
            (SELECT MAX(population) FROM dbt_dev_marts.fact_population) AS largest_province,
            (SELECT COUNT(*) FROM dbt_dev_marts.fact_exchange_rate)     AS trading_days,
            (SELECT MIN(date) FROM dbt_dev_marts.fact_exchange_rate)    AS first_date,
            (SELECT MAX(date) FROM dbt_dev_marts.fact_exchange_rate)    AS last_date,
            (SELECT ROUND(AVG(exchange_rate)::numeric, 2)
               FROM dbt_dev_marts.fact_exchange_rate)                   AS avg_exchange_rate
    """)
    with get_engine().connect() as conn:
        return conn.execute(sql).mappings().one()


@st.cache_data(ttl=300)
def load_population():
    """Province-level population — 25 rows from province_year_summary."""
    sql = text("""
        SELECT province_code, province_name, population_total, year
        FROM dbt_dev_marts.province_year_summary
        ORDER BY population_total DESC
    """)
    return pd.read_sql(sql, get_engine())


@st.cache_data(ttl=300)
def load_exchange_rate():
    """Daily exchange rate with year attributes from dim_date."""
    sql = text("""
        SELECT
            f.date,
            d.year,
            d.month,
            d.quarter,
            f.purchase,
            f.sale,
            f.midpoint,
            f.exchange_rate
        FROM dbt_dev_marts.fact_exchange_rate f
        JOIN dbt_dev_marts.dim_date d ON f.date_key = d.date_key
        ORDER BY f.date
    """)
    return pd.read_sql(sql, get_engine())


@st.cache_data(ttl=300)
def load_yearly_summary():
    """Aggregated exchange-rate stats per year."""
    sql = text("""
        SELECT
            d.year,
            COUNT(*)                                  AS trading_days,
            ROUND(AVG(f.exchange_rate)::numeric, 2)   AS avg_rate,
            MIN(f.exchange_rate)                      AS min_rate,
            MAX(f.exchange_rate)                      AS max_rate,
            MAX(f.exchange_rate) - MIN(f.exchange_rate) AS volatility
        FROM dbt_dev_marts.fact_exchange_rate f
        JOIN dbt_dev_marts.dim_date d ON f.date_key = d.date_key
        GROUP BY d.year
        ORDER BY d.year
    """)
    return pd.read_sql(sql, get_engine())


# ---------------------------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------------------------
def sidebar_filters(df_exchange: pd.DataFrame, df_yearly: pd.DataFrame):
    st.sidebar.header("Filters")

    years = sorted(df_yearly["year"].unique().tolist())
    year_range = st.sidebar.slider(
        "Year range",
        min_value=int(years[0]),
        max_value=int(years[-1]),
        value=(int(years[0]), int(years[-1])),
    )

    top_n = st.sidebar.slider(
        "Provinces to show (by population)",
        min_value=5,
        max_value=25,
        value=10,
    )

    return year_range, top_n


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    st.title("🇰🇭 Cambodia Data Warehouse")
    st.caption(
        "Population (2024 snapshot) and USD/KHR exchange rates (2003–2023), "
        "modelled through dbt into dimensions, facts, and a summary mart."
    )

    # Load data (cached)
    kpis = load_kpis()
    df_pop = load_population()
    df_fx = load_exchange_rate()
    df_yearly = load_yearly_summary()

    # Sidebar
    year_range, top_n = sidebar_filters(df_fx, df_yearly)

    # Filter exchange data by year range
    df_fx_filtered = df_fx[
        (df_fx["year"] >= year_range[0]) & (df_fx["year"] <= year_range[1])
    ]
    df_yearly_filtered = df_yearly[
        (df_yearly["year"] >= year_range[0]) & (
            df_yearly["year"] <= year_range[1])
    ]

    # ----------------------------------------------------------------
    # KPI row
    # ----------------------------------------------------------------
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Provinces", f"{kpis['provinces']:,}")
    col2.metric("Total population", f"{kpis['total_population']:,}")
    col3.metric(
        "Exchange-rate range",
        f"{kpis['first_date']} → {kpis['last_date']}",
        help=f"{kpis['trading_days']:,} trading days",
    )
    col4.metric("Average rate (KHR/USD)", f"{kpis['avg_exchange_rate']:,}")

    st.divider()

    # ----------------------------------------------------------------
    # Population section
    # ----------------------------------------------------------------
    st.header("Population by province (2024)")

    top_provinces = df_pop.head(top_n).copy()

    chart_pop = (
        alt.Chart(top_provinces)
        .mark_bar()
        .encode(
            x=alt.X("population_total:Q", title="Population"),
            y=alt.Y("province_name:N", sort="-x", title="Province"),
            tooltip=[
                alt.Tooltip("province_name:N", title="Province"),
                alt.Tooltip("province_code:N", title="Code"),
                alt.Tooltip("population_total:Q", title="Population", format=","),
            ],
        )
        .properties(height=max(300, top_n * 24))
    )
    st.altair_chart(chart_pop, width="stretch")

    # Share of national population
    total_pop = df_pop["population_total"].sum()
    df_pop["share_pct"] = (
        df_pop["population_total"] / total_pop * 100).round(2)

    st.subheader("Province share of national population")
    st.dataframe(
        df_pop[["province_code", "province_name", "population_total", "share_pct"]],
        column_config={
            "province_code": "Code",
            "province_name": "Province",
            "population_total": st.column_config.NumberColumn("Population", format="%d"),
            "share_pct": st.column_config.NumberColumn("Share (%)", format="%.2f"),
        },
        hide_index=True,
        width="stretch",
    )

    st.divider()

    # ----------------------------------------------------------------
    # Exchange-rate trends
    # ----------------------------------------------------------------
    st.header("USD/KHR exchange rate")

    tab1, tab2, tab3 = st.tabs(["Time series", "Yearly summary", "Volatility"])

    with tab1:
        st.caption(
            "Daily reference rate. Weekend and weekday rates are near-identical — NBC publishes a reference rate, not a market rate.")
        chart_fx = (
            alt.Chart(df_fx_filtered)
            .mark_line()
            .encode(
                x=alt.X("date:T", title="Date"),
                y=alt.Y("exchange_rate:Q", title="KHR per USD",
                        scale=alt.Scale(zero=False)),
                tooltip=[
                    alt.Tooltip("date:T", title="Date"),
                    alt.Tooltip("exchange_rate:Q", title="Rate", format=","),
                    alt.Tooltip("purchase:Q", title="Purchase"),
                    alt.Tooltip("sale:Q", title="Sale"),
                ],
            )
            .properties(height=400)
        )
        st.altair_chart(chart_fx, width="stretch")

    with tab2:
        st.caption("Yearly averages, min, and max — aggregated through dim_date.")
        chart_avg = (
            alt.Chart(df_yearly_filtered)
            .mark_line(point=True)
            .encode(
                x=alt.X("year:O", title="Year"),
                y=alt.Y("avg_rate:Q", title="Average KHR/USD",
                        scale=alt.Scale(zero=False)),
                tooltip=[
                    alt.Tooltip("year:O", title="Year"),
                    alt.Tooltip("avg_rate:Q", title="Avg", format=","),
                    alt.Tooltip("min_rate:Q", title="Min"),
                    alt.Tooltip("max_rate:Q", title="Max"),
                    alt.Tooltip("trading_days:Q", title="Days"),
                ],
            )
            .properties(height=400)
        )
        st.altair_chart(chart_avg, width="stretch")

        st.dataframe(
            df_yearly_filtered,
            column_config={
                "year": "Year",
                "trading_days": st.column_config.NumberColumn("Trading days", format="%d"),
                "avg_rate": st.column_config.NumberColumn("Avg", format="%.2f"),
                "min_rate": st.column_config.NumberColumn("Min", format="%d"),
                "max_rate": st.column_config.NumberColumn("Max", format="%d"),
                "volatility": st.column_config.NumberColumn("Volatility", format="%d"),
            },
            hide_index=True,
            width="stretch",
        )

    with tab3:
        st.caption(
            "Volatility = max − min of the daily rate within the year. 2010 had the widest spread.")
        chart_vol = (
            alt.Chart(df_yearly_filtered)
            .mark_bar()
            .encode(
                x=alt.X("year:O", title="Year"),
                y=alt.Y("volatility:Q", title="Max − min (KHR)"),
                color=alt.Color("volatility:Q", scale=alt.Scale(
                    scheme="oranges"), legend=None),
                tooltip=[
                    alt.Tooltip("year:O", title="Year"),
                    alt.Tooltip("volatility:Q", title="Volatility"),
                ],
            )
            .properties(height=400)
        )
        st.altair_chart(chart_vol, width="stretch")

    st.divider()

    # ----------------------------------------------------------------
    # Data completeness note (the 49 missing days)
    # ----------------------------------------------------------------
    with st.expander("⚠️ Data completeness — 49 documented missing days"):
        st.markdown("""
        The exchange-rate source has **49 missing days**, fully documented on Day 3:
        - **April 2008 is entirely absent** (~30 days)
        - Month-end days in late 2007 / early 2008
        - ~14 scattered days

        These were detected by two independent methods:
        1. Python `max() - min() - len()` verification (Day 3)
        2. `LEFT JOIN dim_date → fact_exchange_rate` (Day 23)

        Both agree on the same 49-day gap. The `dim_date` dimension acts as a
        completeness spec — a fact table cannot reveal its own gaps.
        """)

    st.caption(
        "Source: raw.population_by_province (25 rows, 2024) and "
        "raw.exchange_rate (7,531 rows, 2003–2023). "
        "Modelled via dbt: staging → intermediate → dimensions + facts + summary."
    )


if __name__ == "__main__":
    main()