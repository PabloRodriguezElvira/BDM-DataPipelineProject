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
    streamlit run src/data_consumption/structured_data/risk_prediction_dashboard.py
"""

import os

import clickhouse_connect
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from pymongo import MongoClient
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import classification_report, roc_auc_score, roc_curve
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
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
    "avg_precip_prob":       "Precipitation prob.",
    "avg_temperature":       "Temperature (°F)",
    "avg_wind_speed_mph":    "Wind speed (mph)",
    "avg_dewpoint_celsius":  "Dew point (°C)",
    "avg_humidity":          "Humidity (%)",
    "cam_avg_total_traffic": "Camera total traffic",
    "cam_avg_pedestrian":    "Camera pedestrians",
    "hour_of_day":           "Hour of day",
    "day_of_week":           "Day of week",
    "borough_enc":           "Borough",
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
            "model":       model,
            "report":      classification_report(y_te, pred, output_dict=True),
            "auc":         roc_auc_score(y_te, proba),
            "fpr":         fpr,
            "tpr":         tpr,
            "importances": model.feature_importances_,
        }

    return results, imputer, X_te, y_te


# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="NYC Collision Risk Prediction",
    page_icon="⚠️",
    layout="wide",
)

st.title("NYC Collision High-Risk Area Prediction")
st.caption(
    "Source: `exploitation_zone.collisions_weather` · ClickHouse  |  "
    "Models: Random Forest & XGBoost"
)
st.markdown(
    "Combines collision records with weather variables (precipitation, visibility, wind) "
    "and traffic data to learn patterns of **collision risk with casualties**."
)
st.divider()

# ── Load & prepare ─────────────────────────────────────────────────────────────
raw_df = load_data()
df     = engineer_features(raw_df)

col_info1, col_info2, col_info3 = st.columns(3)
col_info1.metric("Total records",              f"{len(df):,}")
col_info2.metric("High risk (with casualties)", f"{df['is_high_risk'].sum():,}")
col_info3.metric("High-risk rate",              f"{df['is_high_risk'].mean()*100:.1f} %")

st.divider()

# ── Train ──────────────────────────────────────────────────────────────────────
results, imputer, X_te, y_te = train_models(df)

# ── Model selection ────────────────────────────────────────────────────────────
st.subheader("Model Comparison")
model_name = st.radio(
    "Select model to analyse:",
    options=list(results.keys()),
    horizontal=True,
)
res = results[model_name]

# ── Metrics row ────────────────────────────────────────────────────────────────
m1, m2, m3, m4 = st.columns(4)
rep = res["report"]
m1.metric("AUC-ROC",                  f"{res['auc']:.3f}")
m2.metric("Accuracy",                 f"{rep['accuracy']:.3f}")
m3.metric("Precision (high risk)",    f"{rep['1']['precision']:.3f}")
m4.metric("Recall (high risk)",       f"{rep['1']['recall']:.3f}")

st.divider()

# ── ROC + Feature importance ───────────────────────────────────────────────────
col_roc, col_imp = st.columns(2)

with col_roc:
    st.subheader("ROC Curve")
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
        xaxis_title="False Positive Rate",
        yaxis_title="True Positive Rate",
        plot_bgcolor="white", paper_bgcolor="white",
        legend=dict(x=0.6, y=0.1),
    )
    fig_roc.update_xaxes(showgrid=True, gridcolor="#eee", range=[0, 1])
    fig_roc.update_yaxes(showgrid=True, gridcolor="#eee", range=[0, 1])
    st.plotly_chart(fig_roc, use_container_width=True)

with col_imp:
    st.subheader("Feature Importance")
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
        labels={"importance": "Importance", "feature": "Feature"},
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
st.subheader("Actual High-Risk Rate by Borough")

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
    labels={"risk_rate": "% collisions with casualties", "Borough": "Borough"},
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
st.subheader("Interactive Risk Simulator")
st.markdown(
    "Adjust weather and traffic conditions to see the predicted high-risk "
    "probability for each borough."
)

c1, c2, c3 = st.columns(3)
with c1:
    inp_precip   = st.slider("Precipitation probability (%)", 0, 100, 20)
    inp_temp     = st.slider("Temperature (°F)", -10, 110, 65)
    inp_wind     = st.slider("Wind speed (mph)", 0, 60, 10)
with c2:
    inp_dewpoint = st.slider("Dew point (°C)", -20, 30, 10)
    inp_humidity = st.slider("Humidity (%)", 0, 100, 60)
    inp_traffic  = st.slider("Total traffic (cameras)", 0, 500, 100)
with c3:
    inp_ped  = st.slider("Pedestrians detected (cameras)", 0, 200, 30)
    inp_hour = st.slider("Hour of day", 0, 23, 8)
    inp_dow  = st.selectbox(
        "Day of the week",
        options=list(range(7)),
        format_func=lambda d: ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"][d],
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
    "Borough":           BOROUGH_ORDER,
    "Predicted risk (%)": (proba_sim * 100).round(1),
})

fig_sim = px.bar(
    sim_df.sort_values("Predicted risk (%)"),
    x="Predicted risk (%)",
    y="Borough",
    orientation="h",
    color="Predicted risk (%)",
    color_continuous_scale="RdYlGn_r",
    text=sim_df.sort_values("Predicted risk (%)")["Predicted risk (%)"].map("{:.1f}%".format),
    range_color=[0, 100],
)
fig_sim.update_layout(
    showlegend=False, coloraxis_showscale=True,
    plot_bgcolor="white", paper_bgcolor="white",
    coloraxis_colorbar_title="Risk (%)",
)
fig_sim.update_traces(textposition="outside")
fig_sim.update_xaxes(showgrid=True, gridcolor="#eee", range=[0, 110])
st.plotly_chart(fig_sim, use_container_width=True)

# Highest-risk borough call-out
top_borough = sim_df.loc[sim_df["Predicted risk (%)"].idxmax(), "Borough"]
top_prob    = sim_df["Predicted risk (%)"].max()
st.info(
    f"Under the selected conditions, **{top_borough}** is the borough with the highest "
    f"probability of a collision with casualties: **{top_prob:.1f} %**  "
    f"(model: {model_name})"
)

st.divider()

# ── Camera Traffic Clustering ──────────────────────────────────────────────────
_MONGO = dict(
    host=os.getenv("MONGO_HOST", "localhost"),
    port=int(os.getenv("MONGO_PORT", "27017")),
    username=os.getenv("MONGO_USER", "admin"),
    password=os.getenv("MONGO_PASSWORD", "mongo_pass"),
)

VEHICLE_COLS   = ["MotorBike", "Bike", "LMV", "Auto", "LCV", "e-Rickshaw"]
DETECTION_COLS = ["MotorBike", "Pedestrian", "Bike", "LMV", "Auto", "LCV", "e-Rickshaw"]


@st.cache_data(show_spinner="Loading camera data from MongoDB…")
def load_camera_data() -> pd.DataFrame:
    uri = (
        f"mongodb://{_MONGO['username']}:{_MONGO['password']}"
        f"@{_MONGO['host']}:{_MONGO['port']}/"
    )
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    try:
        records = list(
            client["exploitation_zone"]["camera_aggregates"].find({}, {"_id": 0})
        )
        return pd.DataFrame(records) if records else pd.DataFrame()
    finally:
        client.close()


st.subheader("Camera Traffic Study · K-Means Clustering")
st.markdown(
    "We use object detection metadata from traffic cameras to group areas by "
    "**congestion level** using K-Means and identify **unusual vehicle behaviour** "
    "in cameras that deviate from their cluster centroid."
)

try:
    cam_df = load_camera_data()
    cam_error = None
except Exception as exc:
    cam_df = pd.DataFrame()
    cam_error = exc

if cam_error is not None:
    st.warning(f"Could not connect to MongoDB: {cam_error}")
elif cam_df.empty:
    st.info("No camera data found in MongoDB. Run the trusted zone pipeline first.")
else:
    avail_detect  = [c for c in DETECTION_COLS if c in cam_df.columns]
    avail_vehicle = [c for c in VEHICLE_COLS    if c in cam_df.columns]
    id_col = "camera_id_original" if "camera_id_original" in cam_df.columns else "camera_id"

    agg_dict: dict = {c: "mean" for c in avail_detect + ["total_traffic"] if c in cam_df.columns}
    if "borough" in cam_df.columns:
        agg_dict["borough"] = "first"

    cam_agg = cam_df.groupby(id_col).agg(agg_dict).reset_index()

    k_val = st.slider("Number of clusters (k)", min_value=2, max_value=6, value=3, key="kmeans_k")

    cluster_features = avail_detect + ["total_traffic"]
    X_raw    = cam_agg[cluster_features].fillna(0.0).values
    scaler   = StandardScaler()
    X_scaled = scaler.fit_transform(X_raw)

    kmeans = KMeans(n_clusters=k_val, random_state=42, n_init=10)
    cam_agg["cluster"] = kmeans.fit_predict(X_scaled)

    # Distance to centroid as anomaly score
    centroids = kmeans.cluster_centers_
    cam_agg["anomaly_score"] = np.array([
        np.linalg.norm(X_scaled[i] - centroids[int(cam_agg["cluster"].iloc[i])])
        for i in range(len(cam_agg))
    ])
    anomaly_threshold = float(np.percentile(cam_agg["anomaly_score"], 90))
    cam_agg["unusual"] = cam_agg["anomaly_score"] > anomaly_threshold

    # Label clusters by ascending mean total_traffic
    rank_order = cam_agg.groupby("cluster")["total_traffic"].mean().sort_values().index.tolist()
    if k_val == 3:
        level_names = ["Low Congestion", "Medium Congestion", "High Congestion"]
    else:
        level_names = [f"Level {i + 1}" for i in range(k_val)]
    label_map = {cid: level_names[rank] for rank, cid in enumerate(rank_order)}
    cam_agg["cluster_label"] = cam_agg["cluster"].map(label_map)

    # PCA 2D projection
    pca    = PCA(n_components=2)
    coords = pca.fit_transform(X_scaled)
    cam_agg["PC1"] = coords[:, 0]
    cam_agg["PC2"] = coords[:, 1]
    var_exp = pca.explained_variance_ratio_

    col_scat, col_prof = st.columns(2)

    with col_scat:
        st.markdown(
            f"**PCA Projection** — variance explained: "
            f"PC1 {var_exp[0]*100:.1f} %, PC2 {var_exp[1]*100:.1f} %"
        )
        hover_fields = [c for c in [id_col, "cluster_label", "total_traffic", "Pedestrian", "anomaly_score"] if c in cam_agg.columns]
        fig_scat = px.scatter(
            cam_agg,
            x="PC1", y="PC2",
            color="cluster_label",
            symbol="unusual",
            symbol_map={True: "x", False: "circle"},
            hover_data=hover_fields,
            color_discrete_sequence=px.colors.qualitative.Set2,
            labels={"cluster_label": "Cluster", "unusual": "Unusual"},
        )
        fig_scat.update_traces(marker_size=11)
        fig_scat.update_layout(plot_bgcolor="white", paper_bgcolor="white")
        fig_scat.update_xaxes(showgrid=True, gridcolor="#eee")
        fig_scat.update_yaxes(showgrid=True, gridcolor="#eee")
        st.plotly_chart(fig_scat, use_container_width=True)

    with col_prof:
        st.markdown("**Average detections per cluster and object type**")
        profile = (
            cam_agg.groupby("cluster_label")[avail_detect]
            .mean()
            .reset_index()
            .melt(id_vars="cluster_label", var_name="Object type", value_name="Detections/frame")
        )
        fig_prof = px.bar(
            profile,
            x="cluster_label", y="Detections/frame",
            color="Object type", barmode="group",
            color_discrete_sequence=px.colors.qualitative.Pastel,
            labels={"cluster_label": "Cluster"},
        )
        fig_prof.update_layout(plot_bgcolor="white", paper_bgcolor="white")
        fig_prof.update_xaxes(showgrid=False)
        fig_prof.update_yaxes(showgrid=True, gridcolor="#eee")
        st.plotly_chart(fig_prof, use_container_width=True)

    # Borough composition per cluster
    if "borough" in cam_agg.columns:
        st.markdown("**Geographic distribution of cameras by cluster**")
        bcomp = (
            cam_agg.groupby(["cluster_label", "borough"])
            .size()
            .reset_index(name="No. cameras")
        )
        fig_bcomp = px.bar(
            bcomp,
            x="cluster_label", y="No. cameras",
            color="borough", barmode="stack",
            color_discrete_sequence=px.colors.qualitative.Bold,
            labels={"cluster_label": "Cluster", "borough": "Borough"},
        )
        fig_bcomp.update_layout(plot_bgcolor="white", paper_bgcolor="white")
        fig_bcomp.update_xaxes(showgrid=False)
        fig_bcomp.update_yaxes(showgrid=True, gridcolor="#eee")
        st.plotly_chart(fig_bcomp, use_container_width=True)

    # Unusual cameras table
    st.markdown(
        f"**Cameras with unusual vehicle behaviour** "
        f"— top 10% by distance to centroid (threshold: {anomaly_threshold:.3f})"
    )
    unusual_df = cam_agg[cam_agg["unusual"]].sort_values("anomaly_score", ascending=False)
    display_cols = (
        [id_col]
        + (["borough"] if "borough" in cam_agg.columns else [])
        + ["cluster_label", "total_traffic", "anomaly_score"]
        + avail_detect
    )
    display_cols = [c for c in display_cols if c in unusual_df.columns]
    rename_map = {
        id_col: "Camera", "borough": "Borough",
        "cluster_label": "Cluster", "total_traffic": "Total traffic",
        "anomaly_score": "Anomaly score",
    }
    if unusual_df.empty:
        st.info("No unusual cameras detected with the current parameters.")
    else:
        st.dataframe(
            unusual_df[display_cols].rename(columns=rename_map).reset_index(drop=True),
            use_container_width=True,
        )
        st.caption(f"Unusual cameras detected: {len(unusual_df)} of {len(cam_agg)}")
