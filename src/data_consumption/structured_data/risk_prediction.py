"""
High-Risk Area Prediction – exploitation_zone.collisions_weather (ClickHouse).

Combines accident records with weather variables (precipitation, visibility proxy,
temperature, humidity, wind) and traffic camera counts to predict whether a
collision results in injury or death using Random Forest and XGBoost.

Target (binary):
    is_high_risk = 1  if number_of_persons_injured > 0 OR number_of_persons_killed > 0

Features:
    Weather  : avg_precip_prob, avg_temperature, avg_wind_speed_mph,
               avg_dewpoint_celsius, avg_humidity
    Traffic  : cam_avg_total_traffic, cam_avg_pedestrian
    Temporal : hour_of_day, day_of_week
    Spatial  : borough (label-encoded)

Run:
    streamlit run src/data_consumption/structured_data/risk_prediction.py
"""

import os

import clickhouse_connect
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import classification_report, roc_auc_score, roc_curve
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier

# ── ClickHouse connection ──────────────────────────────────────────────────────
_CH = dict(
    host=os.getenv("CLICKHOUSE_HOST", "localhost"),
    port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
    username=os.getenv("CLICKHOUSE_USER", "default"),
    password=os.getenv("CLICKHOUSE_PASSWORD", "clickhouse"),
)

FEATURE_COLS = [
    "avg_precip_prob",
    "avg_temperature",
    "avg_wind_speed_mph",
    "avg_dewpoint_celsius",
    "avg_humidity",
    "cam_avg_total_traffic",
    "cam_avg_pedestrian",
    "hour_of_day",
    "day_of_week",
    "borough_enc",
]

FEATURE_LABELS = {
    "avg_precip_prob":      "Precipitation prob.",
    "avg_temperature":      "Temperature (°F)",
    "avg_wind_speed_mph":   "Wind speed (mph)",
    "avg_dewpoint_celsius":  "Dew point (°C)",
    "avg_humidity":         "Humidity (%)",
    "cam_avg_total_traffic": "Camera total traffic",
    "cam_avg_pedestrian":   "Camera pedestrians",
    "hour_of_day":          "Hour of day",
    "day_of_week":          "Day of week",
    "borough_enc":          "Borough",
}

BOROUGH_ORDER = ["BRONX", "BROOKLYN", "MANHATTAN", "QUEENS", "STATEN ISLAND"]


# ── Data loading ───────────────────────────────────────────────────────────────
@st.cache_data(show_spinner="Loading data from ClickHouse…")
def load_data() -> pd.DataFrame:
    client = clickhouse_connect.get_client(**_CH)
    try:
        df = client.query_df("""
            SELECT
                crash_date,
                crash_time,
                borough,
                number_of_persons_injured,
                number_of_persons_killed,
                avg_temperature,
                avg_wind_speed_mph,
                avg_dewpoint_celsius,
                avg_precip_prob,
                avg_humidity,
                cam_avg_total_traffic,
                cam_avg_pedestrian
            FROM exploitation_zone.collisions_weather
            WHERE borough != ''
        """)
    finally:
        client.close()
    return df


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Target
    df["is_high_risk"] = (
        (df["number_of_persons_injured"] > 0) | (df["number_of_persons_killed"] > 0)
    ).astype(int)

    # Temporal
    df["hour_of_day"] = df["crash_time"].str.split(":").str[0].astype(float)
    df["crash_date"] = pd.to_datetime(df["crash_date"])
    df["day_of_week"] = df["crash_date"].dt.dayofweek  # 0=Mon … 6=Sun

    # Spatial encoding — fixed mapping so sliders stay consistent
    borough_map = {b: i for i, b in enumerate(BOROUGH_ORDER)}
    df["borough_upper"] = df["borough"].str.upper()
    df["borough_enc"] = df["borough_upper"].map(borough_map).fillna(-1)
    df = df[df["borough_enc"] >= 0]  # drop unknowns

    return df


@st.cache_data(show_spinner="Training models…")
def train_models(df: pd.DataFrame):
    X = df[FEATURE_COLS].copy()
    y = df["is_high_risk"]

    imputer = SimpleImputer(strategy="median")
    X_imp = imputer.fit_transform(X)

    X_tr, X_te, y_tr, y_te = train_test_split(
        X_imp, y, test_size=0.2, random_state=42, stratify=y
    )

    rf = RandomForestClassifier(n_estimators=200, max_depth=8, random_state=42, n_jobs=-1)
    rf.fit(X_tr, y_tr)

    xgb = XGBClassifier(
        n_estimators=200, max_depth=6, learning_rate=0.1,
        use_label_encoder=False, eval_metric="logloss",
        random_state=42, n_jobs=-1,
    )
    xgb.fit(X_tr, y_tr)

    results = {}
    for name, model in [("Random Forest", rf), ("XGBoost", xgb)]:
        proba = model.predict_proba(X_te)[:, 1]
        pred  = model.predict(X_te)
        fpr, tpr, _ = roc_curve(y_te, proba)
        results[name] = {
            "model":  model,
            "report": classification_report(y_te, pred, output_dict=True),
            "auc":    roc_auc_score(y_te, proba),
            "fpr":    fpr,
            "tpr":    tpr,
            "importances": model.feature_importances_,
        }

    return results, imputer, X_te, y_te


# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="NYC Collision Risk Prediction",
    page_icon="⚠️",
    layout="wide",
)

st.title("Predicción de Áreas de Alto Riesgo en Accidentes de NYC")
st.caption(
    "Fuente: `exploitation_zone.collisions_weather` · ClickHouse  |  "
    "Modelos: Random Forest & XGBoost"
)
st.markdown(
    "Combina registros de accidentes con variables meteorológicas (precipitación, "
    "visibilidad, viento) y tráfico para aprender patrones de **riesgo de colisión "
    "con víctimas**."
)
st.divider()

# ── Load & prepare ─────────────────────────────────────────────────────────────
raw_df = load_data()
df     = engineer_features(raw_df)

col_info1, col_info2, col_info3 = st.columns(3)
col_info1.metric("Total registros", f"{len(df):,}")
col_info2.metric("Alto riesgo (con víctimas)", f"{df['is_high_risk'].sum():,}")
col_info3.metric(
    "Tasa de alto riesgo",
    f"{df['is_high_risk'].mean()*100:.1f} %",
)

st.divider()

# ── Train ──────────────────────────────────────────────────────────────────────
results, imputer, X_te, y_te = train_models(df)

# ── Model selection ────────────────────────────────────────────────────────────
st.subheader("Comparación de Modelos")
model_name = st.radio(
    "Selecciona el modelo para analizar:",
    options=list(results.keys()),
    horizontal=True,
)
res = results[model_name]

# ── Metrics row ────────────────────────────────────────────────────────────────
m1, m2, m3, m4 = st.columns(4)
rep = res["report"]
m1.metric("AUC-ROC",   f"{res['auc']:.3f}")
m2.metric("Accuracy",  f"{rep['accuracy']:.3f}")
m3.metric("Precision (alto riesgo)", f"{rep['1']['precision']:.3f}")
m4.metric("Recall (alto riesgo)",    f"{rep['1']['recall']:.3f}")

st.divider()

# ── ROC + Feature importance ───────────────────────────────────────────────────
col_roc, col_imp = st.columns(2)

with col_roc:
    st.subheader("Curva ROC")
    fig_roc = go.Figure()
    for name, r in results.items():
        fig_roc.add_trace(go.Scatter(
            x=r["fpr"], y=r["tpr"],
            mode="lines",
            name=f"{name} (AUC={r['auc']:.3f})",
        ))
    fig_roc.add_trace(go.Scatter(
        x=[0, 1], y=[0, 1], mode="lines",
        line=dict(dash="dash", color="gray"),
        showlegend=False,
    ))
    fig_roc.update_layout(
        xaxis_title="Tasa de Falsos Positivos",
        yaxis_title="Tasa de Verdaderos Positivos",
        plot_bgcolor="white", paper_bgcolor="white",
        legend=dict(x=0.6, y=0.1),
    )
    fig_roc.update_xaxes(showgrid=True, gridcolor="#eee", range=[0, 1])
    fig_roc.update_yaxes(showgrid=True, gridcolor="#eee", range=[0, 1])
    st.plotly_chart(fig_roc, use_container_width=True)

with col_imp:
    st.subheader("Importancia de Variables")
    imp_df = (
        pd.DataFrame({
            "feature":    [FEATURE_LABELS.get(f, f) for f in FEATURE_COLS],
            "importance": res["importances"],
        })
        .sort_values("importance")
    )
    fig_imp = px.bar(
        imp_df,
        x="importance",
        y="feature",
        orientation="h",
        color="importance",
        color_continuous_scale="Blues",
        labels={"importance": "Importancia", "feature": "Variable"},
    )
    fig_imp.update_layout(
        showlegend=False, coloraxis_showscale=False,
        plot_bgcolor="white", paper_bgcolor="white",
    )
    fig_imp.update_xaxes(showgrid=True, gridcolor="#eee")
    fig_imp.update_yaxes(showgrid=False)
    st.plotly_chart(fig_imp, use_container_width=True)

st.divider()

# ── Borough risk analysis ──────────────────────────────────────────────────────
st.subheader("Tasa Real de Alto Riesgo por Borough")

borough_stats = (
    df.groupby("borough_upper")
    .agg(
        total=("is_high_risk", "count"),
        high_risk=("is_high_risk", "sum"),
        avg_precip=("avg_precip_prob", "mean"),
        avg_traffic=("cam_avg_total_traffic", "mean"),
    )
    .assign(risk_rate=lambda d: d["high_risk"] / d["total"] * 100)
    .reset_index()
    .rename(columns={"borough_upper": "Borough"})
    .sort_values("risk_rate", ascending=True)
)

fig_bor = px.bar(
    borough_stats,
    x="risk_rate",
    y="Borough",
    orientation="h",
    color="risk_rate",
    color_continuous_scale="Reds",
    labels={"risk_rate": "% colisiones con víctimas", "Borough": "Borough"},
    text=borough_stats["risk_rate"].map("{:.1f}%".format),
)
fig_bor.update_layout(
    showlegend=False, coloraxis_showscale=False,
    plot_bgcolor="white", paper_bgcolor="white",
)
fig_bor.update_traces(textposition="outside")
fig_bor.update_xaxes(showgrid=True, gridcolor="#eee")
st.plotly_chart(fig_bor, use_container_width=True)

st.divider()

# ── Interactive predictor ──────────────────────────────────────────────────────
st.subheader("Simulador Interactivo de Riesgo")
st.markdown(
    "Ajusta las condiciones meteorológicas y de tráfico para ver la probabilidad "
    "predicha de alto riesgo en cada borough."
)

c1, c2, c3 = st.columns(3)
with c1:
    inp_precip   = st.slider("Probabilidad de precipitación (%)", 0, 100, 20)
    inp_temp     = st.slider("Temperatura (°F)", -10, 110, 65)
    inp_wind     = st.slider("Velocidad del viento (mph)", 0, 60, 10)
with c2:
    inp_dewpoint = st.slider("Punto de rocío (°C)", -20, 30, 10)
    inp_humidity = st.slider("Humedad (%)", 0, 100, 60)
    inp_traffic  = st.slider("Tráfico total (cámaras)", 0, 500, 100)
with c3:
    inp_ped      = st.slider("Peatones detectados (cámaras)", 0, 200, 30)
    inp_hour     = st.slider("Hora del día", 0, 23, 8)
    inp_dow      = st.selectbox(
        "Día de la semana",
        options=list(range(7)),
        format_func=lambda d: ["Lunes","Martes","Miércoles","Jueves","Viernes","Sábado","Domingo"][d],
        index=0,
    )

rows = []
for i, borough in enumerate(BOROUGH_ORDER):
    rows.append([
        inp_precip / 100.0,  # stored as 0-1 fraction in the table
        float(inp_temp),
        float(inp_wind),
        float(inp_dewpoint),
        float(inp_humidity),
        float(inp_traffic),
        float(inp_ped),
        float(inp_hour),
        float(inp_dow),
        float(i),
    ])

X_sim = imputer.transform(np.array(rows))
proba_sim = res["model"].predict_proba(X_sim)[:, 1]

sim_df = pd.DataFrame({
    "Borough":          BOROUGH_ORDER,
    "Riesgo predicho (%)": (proba_sim * 100).round(1),
})

fig_sim = px.bar(
    sim_df.sort_values("Riesgo predicho (%)"),
    x="Riesgo predicho (%)",
    y="Borough",
    orientation="h",
    color="Riesgo predicho (%)",
    color_continuous_scale="RdYlGn_r",
    text=sim_df.sort_values("Riesgo predicho (%)")["Riesgo predicho (%)"].map("{:.1f}%".format),
    range_color=[0, 100],
)
fig_sim.update_layout(
    showlegend=False, coloraxis_showscale=True,
    plot_bgcolor="white", paper_bgcolor="white",
    coloraxis_colorbar_title="Riesgo (%)",
)
fig_sim.update_traces(textposition="outside")
fig_sim.update_xaxes(showgrid=True, gridcolor="#eee", range=[0, 110])
st.plotly_chart(fig_sim, use_container_width=True)

# Highest-risk borough call-out
top_borough = sim_df.loc[sim_df["Riesgo predicho (%)"].idxmax(), "Borough"]
top_prob    = sim_df["Riesgo predicho (%)"].max()
st.info(
    f"Con las condiciones seleccionadas, **{top_borough}** es el borough con mayor "
    f"probabilidad de colisión con víctimas: **{top_prob:.1f} %**  "
    f"(modelo: {model_name})"
)
