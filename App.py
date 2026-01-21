"""
🛡️ Fraud Detection Dashboard (with Interactive Map)
- Source: tlekdw_fraud.fraudcaseresult
- Branch master: tlekdw_common.dim_branch
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
import time

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
# DB Connection
# =========================
def get_conn():
    import psycopg2
    return psycopg2.connect(
        host=db_host,
        port=int(db_port),
        database=db_name,
        user=db_user,
        password=db_password,
    )

# =========================
# Load fraud + branch data
# =========================
@st.cache_data(ttl=30)
def load_fraud_data(time_range, fraud_types):
    conn = get_conn()

    time_filters = {
        "Last 24 Hours": "f.\"timeInvestigation\" >= NOW() - INTERVAL '24 hours'",
        "Last 7 Days": "f.\"timeInvestigation\" >= NOW() - INTERVAL '7 days'",
        "Last 30 Days": "f.\"timeInvestigation\" >= NOW() - INTERVAL '30 days'",
        "All Time": "1=1"
    }
    time_filter = time_filters.get(time_range, "1=1")

    fraud_filter = ""
    if fraud_types and "All" not in fraud_types:
        fraud_list = ",".join([f"'{f}'" for f in fraud_types])
        fraud_filter = f" AND f.fraudtype IN ({fraud_list}) "

    sql = f"""
    SELECT
        f.*,
        substring(f."Branch" from '\\((B[0-9]+)\\)') AS branch_id,
        b.branch_name,
        b.province,
        b.latitude,
        b.longitude
    FROM tlekdw_fraud.fraudcaseresult f
    LEFT JOIN tlekdw_common.dim_branch b
      ON substring(f."Branch" from '\\((B[0-9]+)\\)') = b.branch_id
    WHERE {time_filter}
    {fraud_filter}
    ORDER BY f."timeInvestigation" DESC
    LIMIT 2000;
    """

    df = pd.read_sql(sql, conn)
    conn.close()

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
    high_risk = (df["severity"] == "HIGH").sum()
    st.metric("⚠️ High Risk", high_risk)

with c3:
    today = (pd.to_datetime(df["timeInvestigation"]).dt.date == datetime.now().date()).sum()
    st.metric("📅 Today", today)

with c4:
    st.metric("🏪 Affected Branches", df["branch_id"].nunique())

st.divider()

# =========================
# MAP
# =========================
st.subheader("🗺️ Fraud Map (click point to drill down)")

map_df = df.dropna(subset=["latitude", "longitude"]).copy()

fig_map = px.scatter_mapbox(
    map_df,
    lat="latitude",
    lon="longitude",
    size="fraudamount",
    color="fraudtype",
    hover_name="branch_name",
    hover_data=["Branch", "fraudtype", "severity", "fraudamount", "timeInvestigation"],
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
    st.info(f"Branch: {picked['branch_name']} ({picked['branch_id']})")

    sub = df[df["branch_id"] == picked["branch_id"]].copy()
    st.dataframe(sub.head(300), use_container_width=True)
else:
    st.info("👉 Click a branch on the map to drill down")

# =========================
# Fraud by type / province
# =========================
col1, col2 = st.columns(2)

with col1:
    st.subheader("Fraud by Type")
    st.plotly_chart(px.pie(df, names="fraudtype", hole=0.4), use_container_width=True)

with col2:
    st.subheader("Fraud by Province")
    st.plotly_chart(px.bar(df["province"].value_counts().head(10)), use_container_width=True)

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
