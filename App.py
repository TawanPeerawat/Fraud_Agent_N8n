"""
🛡️ Fraud Detection Dashboard (with Interactive Map)
- Source: tlekdw_fraud.fraudcaseresult
- Branch master: tlekdw_common.dim_branch
- Streamlit Cloud friendly (Python 3.13): SQLAlchemy + psycopg3
- Robust: extract branch_id in pandas, then merge with dim_branch
"""

import re
import time
from datetime import datetime
from urllib.parse import quote_plus

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text


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
# Engine
# =========================
def get_engine():
    pw = quote_plus(db_password)
    url = f"postgresql+psycopg://{db_user}:{pw}@{db_host}:{int(db_port)}/{db_name}"
    return create_engine(url, pool_pre_ping=True)

# =========================
# Helpers
# =========================
def extract_branch_id(branch_text: str):
    """
    fraud table Branch looks like: '... (B0042)'
    """
    if branch_text is None:
        return None
    m = re.search(r"\((B\d+)\)", str(branch_text))
    return m.group(1) if m else None

def compute_severity(df: pd.DataFrame) -> pd.DataFrame:
    if "Reason Fraud" in df.columns:
        r = df["Reason Fraud"].astype(str)
        df["severity"] = "LOW"
        df.loc[r.str.contains("CLOSE|KEEP", case=False, na=False), "severity"] = "HIGH"
        df.loc[r.str.contains("suspected", case=False, na=False), "severity"] = "MEDIUM"
    else:
        df["severity"] = "LOW"
    return df

# =========================
# Load data (robust)
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

    # safe filter (psycopg3 + sqlalchemy)
    if fraud_types and "All" not in fraud_types:
        fraud_filter = " AND f.fraudtype = ANY(:fraud_list) "
        params["fraud_list"] = list(fraud_types)

    # 1) Load fraud only (no regex / no join)
    sql_fraud = f"""
    SELECT
        f.*
    FROM tlekdw_fraud.fraudcaseresult f
    WHERE {time_filter}
    {fraud_filter}
    ORDER BY f.\"timeInvestigation\" DESC
    LIMIT 2000;
    """

    with engine.connect() as conn:
        fraud_df = pd.read_sql(text(sql_fraud), conn, params=params)

    if fraud_df.empty:
        return fraud_df

    # 2) Extract branch_id in pandas
    if "Branch" in fraud_df.columns:
        fraud_df["branch_id"] = fraud_df["Branch"].apply(extract_branch_id)
    else:
        fraud_df["branch_id"] = None

    # 3) Pull dim_branch for only involved branch_id
    branch_ids = fraud_df["branch_id"].dropna().astype(str).unique().tolist()

    if branch_ids:
        sql_branch = """
        SELECT
            branch_id,
            branch_name,
            province,
            latitude,
            longitude
        FROM tlekdw_common.dim_branch
        WHERE branch_id = ANY(:branch_ids);
        """
        with engine.connect() as conn:
            branch_df = pd.read_sql(text(sql_branch), conn, params={"branch_ids": branch_ids})

        # 4) Merge
        out = fraud_df.merge(branch_df, on="branch_id", how="left")
    else:
        # no ids extracted => still return fraud_df
        out = fraud_df.copy()
        out["branch_name"] = None
        out["province"] = None
        out["latitude"] = None
        out["longitude"] = None

    out = compute_severity(out)
    return out

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
    st.metric("⚠️ High Risk", int((df["severity"] == "HIGH").sum()))

with c3:
    if "timeInvestigation" in df.columns:
        t = pd.to_datetime(df["timeInvestigation"], errors="coerce")
        today = int((t.dt.date == datetime.now().date()).sum())
    else:
        today = 0
    st.metric("📅 Today", today)

with c4:
    st.metric("🏪 Affected Branches", int(df["branch_id"].nunique()))

st.divider()

# =========================
# MAP
# =========================
st.subheader("🗺️ Fraud Map (click point to drill down)")

map_df = df.dropna(subset=["latitude", "longitude"]).copy()

if map_df.empty:
    st.warning("⚠️ ไม่เจอ latitude/longitude หลัง merge dim_branch (เช็คว่า dim_branch มีพิกัดครบไหม)")
    st.dataframe(df[["Branch", "branch_id", "branch_name", "province"]].head(50), use_container_width=True)
else:
    # ensure numeric for size
    if "fraudamount" in map_df.columns:
        map_df["fraudamount_num"] = pd.to_numeric(map_df["fraudamount"], errors="coerce").fillna(0)
        size_col = "fraudamount_num"
    else:
        size_col = None

    fig_map = px.scatter_mapbox(
        map_df,
        lat="latitude",
        lon="longitude",
        size=size_col,
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

        sub = df[df["branch_id"] == picked_branch_id].copy() if picked_branch_id else df.copy()
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
