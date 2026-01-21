"""
🛡️ Fraud Detection Dashboard (with Interactive Map)
- Source: tlekdw_fraud.fraudcaseresult
- Branch master: tlekdw_common.dim_branch
- Streamlit Cloud friendly (Python 3.13): SQLAlchemy + psycopg3
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
import time

from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

# =========================
# Page config
# =========================
st.set_page_config(
    page_title="Fraud Detection Dashboard",
    page_icon="🛡️",
    layout="wide"
)

# =========================
# Style
# =========================
st.markdown("""
<style>
.main-header {
    font-size: 2.5rem;
    font-weight: bold;
    color: #FF4B4B;
    text-align: center;
    margin-bottom: 2rem;
}
</style>
""", unsafe_allow_html=True)

# =========================
# Sidebar
# =========================
with st.sidebar:
    st.title("⚙️ Settings")

    st.subheader("📊 Database")
    db_host = st.text_input("Host", value="n8n.madt.pro")
    db_port = st.text_input("Port", value="5432")
    db_name = st.text_input("Database", value="tlex_suki_db")
    db_user = st.text_input("Username", value="alex888")
    db_password = st.text_input("Password", type="password", value="is2025")

    st.divider()

    st.subheader("🔍 Filters")
    time_range = st.selectbox(
        "Time Range",
        ["Last 24 Hours", "Last 7 Days", "Last 30 Days", "All Time"]
    )

    fraud_types = st.multiselect(
        "Fraud Types",
        ["inventory_fraud", "customer_staff_collusion",
         "late_night_high_spend", "queue_low_value_anomaly",
         "branch_operational_risk", "All"],
        default=["All"]
    )

    st.divider()
    auto_refresh = st.checkbox("Enable Auto Refresh", value=True)
    refresh_interval = st.slider("Interval (seconds)", 10, 120, 30)

    if st.button("🔄 Refresh Now"):
        st.cache_data.clear()
        st.rerun()

# =========================
# Header
# =========================
st.markdown('<div class="main-header">🛡️ Fraud Detection Dashboard</div>', unsafe_allow_html=True)

# =========================
# Engine (SQLAlchemy + psycopg3)
# =========================
def get_engine():
    # กัน password มีอักขระพิเศษ
    pw = quote_plus(db_password)
    url = f"postgresql+psycopg://{db_user}:{pw}@{db_host}:{int(db_port)}/{db_name}"
    return create_engine(url, pool_pre_ping=True)

# =========================
# Load fraud + branch data
# =========================
@st.cache_data(ttl=30)
def load_fraud_data(time_range: str, fraud_types: tuple):
    engine = get_engine()

    time_filters = {
        "Last 24 Hours": "f.\"timeInvestigation\" >= NOW() - INTERVAL '24 hours'",
        "Last 7 Days": "f.\"timeInvestigation\" >= NOW() - INTERVAL '7 days'",
        "Last 30 Days": "f.\"timeInvestigation\" >= NOW() - INTERVAL '30 days'",
        "All Time": "1=1"
    }
    time_filter = time_filters.get(time_range, "1=1")

    fraud_filter = ""
    params = {}

    # ทำ filter แบบปลอดภัย (ไม่ต่อ string)
    if fraud_types and "All" not in fraud_types:
        fraud_filter = " AND f.fraudtype = ANY(:fraud_list) "
        params["fraud_list"] = list(fraud_types)

    sql = f"""
    SELECT
        f.*,
        substring(f."Branch" from E'\\((B[0-9]+)\\)') AS branch_id,
        b.branch_name,
        b.province,
        b.latitude,
        b.longitude
    FROM tlekdw_fraud.fraudcaseresult f
    LEFT JOIN tlekdw_common.dim_branch b
      ON substring(f."Branch" from E'\\((B[0-9]+)\\)') = b.branch_id
    WHERE {time_filter}
    {fraud_filter}
    ORDER BY f."timeInvestigation" DESC
    LIMIT 2000;
    """

    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn, params=params)

    # Severity
    if "Reason Fraud" in df.columns:
        reason = df["Reason Fraud"].astype(str)
        df["severity"] = "LOW"
        df.loc[reason.str.contains("CLOSE|KEEP", case=False, na=False), "severity"] = "HIGH"
        df.loc[reason.str.contains("suspected", case=False, na=False), "severity"] = "MEDIUM"
    else:
        df["severity"] = "LOW"

    return df

# =========================
# Load
# =========================
with st.spinner("Loading fraud data..."):
    df = load_fraud_data(time_range, tuple(fraud_types))

if df.empty:
    st.warning("⚠️ No fraud data found.")
    st.stop()

# =========================
# Summary
# =========================
st.subheader("📊 Summary Statistics")
c1, c2, c3, c4 = st.columns(4)

with c1:
    st.metric("🚨 Total Cases", len(df))

with c2:
    high_risk = int((df["severity"] == "HIGH").sum()) if "severity" in df.columns else 0
    st.metric("⚠️ High Risk", high_risk)

with c3:
    if "timeInvestigation" in df.columns:
        today = int((pd.to_datetime(df["timeInvestigation"], errors="coerce").dt.date == datetime.now().date()).sum())
    else:
        today = 0
    st.metric("📅 Today", today)

with c4:
    st.metric("🏪 Affected Branches", int(df["branch_id"].nunique()) if "branch_id" in df.columns else 0)

st.divider()

# =========================
# MAP
# =========================
st.subheader("🗺️ Fraud Map (click point to drill down)")

map_df = df.dropna(subset=["latitude", "longitude"]).copy()

if map_df.empty:
    st.warning("⚠️ ไม่มีพิกัด (latitude/longitude) จาก dim_branch — ตรวจว่า join ติดหรือไม่")
else:
    fig_map = px.scatter_mapbox(
        map_df,
        lat="latitude",
        lon="longitude",
        size="fraudamount" if "fraudamount" in map_df.columns else None,
        color="fraudtype" if "fraudtype" in map_df.columns else None,
        hover_name="branch_name" if "branch_name" in map_df.columns else None,
        hover_data=[c for c in ["Branch", "branch_id", "fraudtype", "severity", "fraudamount", "timeInvestigation"] if c in map_df.columns],
        zoom=10,
        height=520
    )
    fig_map.update_layout(mapbox_style="open-street-map", margin=dict(l=0, r=0, t=0, b=0))

    selection = st.plotly_chart(fig_map, use_container_width=True, on_select="rerun")

    st.divider()

    # =========================
    # Drilldown from map
    # =========================
    st.subheader("📌 Selected Branch Fraud Cases")

    if selection and selection.get("selection") and selection["selection"].get("points"):
        idx = selection["selection"]["points"][0]["pointIndex"]
        picked = map_df.iloc[idx]

        picked_branch_id = picked.get("branch_id", None)
        picked_branch_name = picked.get("branch_name", "")

        st.info(f"Branch: {picked_branch_name} ({picked_branch_id})")

        if picked_branch_id is not None and "branch_id" in df.columns:
            sub = df[df["branch_id"] == picked_branch_id].copy()
        else:
            sub = df.copy()

        st.dataframe(sub.head(300), use_container_width=True)
    else:
        st.info("👉 Click a branch on the map to drill down")

st.divider()

# =========================
# Fraud by type / province
# =========================
col1, col2 = st.columns(2)

with col1:
    st.subheader("Fraud by Type")
    if "fraudtype" in df.columns:
        st.plotly_chart(px.pie(df, names="fraudtype", hole=0.4), use_container_width=True)
    else:
        st.info("ไม่มีคอลัมน์ fraudtype")

with col2:
    st.subheader("Fraud by Province")
    if "province" in df.columns:
        vc = df["province"].astype(str).value_counts().head(10).reset_index()
        vc.columns = ["province", "count"]
        st.plotly_chart(px.bar(vc, x="count", y="province", orientation="h"), use_container_width=True)
    else:
        st.info("ไม่มีคอลัมน์ province")

st.divider()

# =========================
# Recent cases
# =========================
st.subheader("📋 Recent Fraud Cases")

show_cols = [
    "No", "branch_id", "branch_name", "province",
    "fraudtype", "fraudamount", "severity",
    "Reason Fraud", "timeInvestigation"
]
show_cols = [c for c in show_cols if c in df.columns]

st.dataframe(df[show_cols].head(200), use_container_width=True)

# =========================
# Auto refresh
# =========================
if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()
