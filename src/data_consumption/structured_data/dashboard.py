"""
Interactive dashboard – exploitation_zone.collisions_weather (ClickHouse).

Two charts:
  1. Monthly collision trend by borough  (multi-select filter)
  2. Top 10 contributing factors         (single-borough filter)

Run locally (ClickHouse must be reachable at localhost:8123):
    pip install streamlit plotly clickhouse-connect
    streamlit run src/data_consumption/dashboard.py
"""

import os

import clickhouse_connect
import pandas as pd
import plotly.express as px
import streamlit as st

_CH = dict(
    host=os.getenv("CLICKHOUSE_HOST", "localhost"),
    port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
    username=os.getenv("CLICKHOUSE_USER", "default"),
    password=os.getenv("CLICKHOUSE_PASSWORD", "clickhouse"),
)


def _query(sql: str) -> pd.DataFrame:
    client = clickhouse_connect.get_client(**_CH)
    try:
        return client.query_df(sql)
    finally:
        client.close()


@st.cache_data
def load_monthly() -> pd.DataFrame:
    df = _query("""
        SELECT
            toStartOfMonth(crash_date)     AS month,
            borough,
            count()                        AS collisions,
            sum(number_of_persons_injured) AS total_injured
        FROM exploitation_zone.collisions_weather
        WHERE borough != ''
        GROUP BY month, borough
        ORDER BY month, borough
    """)
    df["month"] = pd.to_datetime(df["month"])
    return df


@st.cache_data
def load_factors() -> pd.DataFrame:
    return _query("""
        SELECT
            borough,
            contributing_factor_vehicle_1  AS factor,
            count()                        AS collisions
        FROM exploitation_zone.collisions_weather
        WHERE contributing_factor_vehicle_1 NOT IN ('', 'Unspecified')
          AND borough != ''
        GROUP BY borough, factor
        ORDER BY borough, collisions DESC
    """)


# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="NYC Collisions Dashboard",
    page_icon="🚦",
    layout="wide",
)

st.title("NYC Traffic Collisions Dashboard")
st.caption("Data: `exploitation_zone.collisions_weather` · ClickHouse")
st.divider()

df_monthly = load_monthly()
df_factors = load_factors()
boroughs = sorted(df_monthly["borough"].unique().tolist())

# ── Chart 1: monthly trend ────────────────────────────────────────────────────
st.subheader("Tendencia mensual de accidentes por borough")

selected_boroughs = st.multiselect(
    "Selecciona uno o más boroughs:",
    options=boroughs,
    default=boroughs,
)

subset_monthly = df_monthly[df_monthly["borough"].isin(selected_boroughs or boroughs)]

fig1 = px.line(
    subset_monthly,
    x="month",
    y="collisions",
    color="borough",
    markers=True,
    labels={"month": "Mes", "collisions": "Nº de accidentes", "borough": "Borough"},
    color_discrete_sequence=px.colors.qualitative.Set2,
)
fig1.update_layout(
    hovermode="x unified",
    legend_title_text="Borough",
    plot_bgcolor="white",
    paper_bgcolor="white",
)
fig1.update_xaxes(showgrid=True, gridcolor="#eee")
fig1.update_yaxes(showgrid=True, gridcolor="#eee")

st.plotly_chart(fig1, use_container_width=True)

st.divider()

# ── Chart 2: contributing factors ─────────────────────────────────────────────
st.subheader("Top 10 factores contribuyentes a los accidentes")

borough_choice = st.selectbox(
    "Filtrar por borough:",
    options=["Todos los boroughs"] + boroughs,
)

if borough_choice == "Todos los boroughs":
    subset_factors = (
        df_factors.groupby("factor", as_index=False)["collisions"]
        .sum()
        .nlargest(10, "collisions")
        .sort_values("collisions")
    )
    chart_title = "Top 10 factores – Todos los boroughs"
else:
    subset_factors = (
        df_factors[df_factors["borough"] == borough_choice]
        .nlargest(10, "collisions")
        .sort_values("collisions")
    )
    chart_title = f"Top 10 factores – {borough_choice}"

fig2 = px.bar(
    subset_factors,
    x="collisions",
    y="factor",
    orientation="h",
    title=chart_title,
    labels={"collisions": "Nº de accidentes", "factor": "Factor contribuyente"},
    color="collisions",
    color_continuous_scale="Reds",
)
fig2.update_layout(
    showlegend=False,
    coloraxis_showscale=False,
    plot_bgcolor="white",
    paper_bgcolor="white",
)
fig2.update_xaxes(showgrid=True, gridcolor="#eee")
fig2.update_yaxes(showgrid=False)

st.plotly_chart(fig2, use_container_width=True)
