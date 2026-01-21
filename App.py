"""
🛡️ Simple Fraud Detection Dashboard
แสดงข้อมูล Fraud Cases แบบ Real-time จาก Database + n8n webhook
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
# Custom CSS
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
    .alert-box {
        padding: 15px;
        border-left: 5px solid #FF4B4B;
        background-color: #fff5f5;
        margin: 10px 0;
        border-radius: 5px;
    }
</style>
""", unsafe_allow_html=True)

# =========================
# Session state
# =========================
if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = datetime.now()

# =========================
# Sidebar
# =========================
with st.sidebar:
    st.title("⚙️ Settings")

    st.subheader("📊 Database")
    db_host = st.text_input("Host", value="n8n.madt.pro")
    db_port = st.text_input("Port", value="5432")

    # ค่า default แนะนำให้ตรงกับที่คุณใช้งานจริง (แก้ได้จาก sidebar)
    db_name = st.text_input("Database", value="tlex_suki_db")
    db_user = st.text_input("Username", value="alex888")
    db_password = st.text_input("Password", type="password", value="is2025")

    st.divider()

    st.subheader("🔄 Auto Refresh")
    auto_refresh = st.checkbox("Enable Auto Refresh", value=True)
    refresh_interval = st.slider("Interval (seconds)", 10, 120, 30)

    st.divider()

    st.subheader("🔍 Filters")
    time_range = st.selectbox(
        "Time Range",
        ["Last 24 Hours", "Last 7 Days", "Last 30 Days", "All Time"],
        index=0
    )

    fraud_types = st.multiselect(
        "Fraud Types",
        ["branch_risk_exposure", "customer_staff_collusion",
         "late_night_high_spend", "operational_risk",
         "queue_low_value_anomaly", "All"],
        default=["All"]
    )

    if st.button("🔄 Refresh Now"):
        # ล้าง cache ข้อมูลเพื่อให้ดึงใหม่ทันที
        st.cache_data.clear()
        st.rerun()

    st.divider()
    st.caption(f"Last updated: {st.session_state.last_refresh.strftime('%H:%M:%S')}")


# =========================
# Header
# =========================
st.markdown('<div class="main-header">🛡️ Fraud Detection Dashboard</div>', unsafe_allow_html=True)


# =========================
# DB Connection (ไม่ cache เพื่อไม่ค้างค่าเดิม)
# =========================
def get_database_connection(host: str, port: str, database: str, user: str, password: str):
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=host,
            port=int(port),
            database=database,
            user=user,
            password=password,
        )
        return conn
    except Exception as e:
        st.error(f"❌ Database connection failed: {e}")
        return None


# =========================
# Load data (cache เฉพาะผลลัพธ์ df)
# =========================
@st.cache_data(ttl=30)
def load_fraud_data(host: str, port: str, database: str, user: str, password: str,
                    time_range: str, fraud_types: tuple):
    """
    ดึงข้อมูลจาก: tlekdw_fraud.fraudcaseresult  (ตาม DBeaver ของคุณ)
    - ไม่เดาชื่อคอลัมน์: SELECT * ก่อน แล้วค่อยจัดรูปใน pandas
    - สร้าง severity จาก Reason Fraud ถ้ามีคอลัมน์นี้
    """
    conn = get_database_connection(host, port, database, user, password)
    if not conn:
        return pd.DataFrame()

    # time filter (จะใช้ได้เมื่อคอลัมน์ชื่อ timeInvestigation มีจริง)
    time_filters = {
        "Last 24 Hours": "\"timeInvestigation\" >= NOW() - INTERVAL '24 hours'",
        "Last 7 Days": "\"timeInvestigation\" >= NOW() - INTERVAL '7 days'",
        "Last 30 Days": "\"timeInvestigation\" >= NOW() - INTERVAL '30 days'",
        "All Time": "1=1",
    }
    time_filter = time_filters.get(time_range, "1=1")

    # 1) ลอง query แบบมี timeInvestigation ก่อน (กรณีคอลัมน์มีจริง)
    query1 = f"""
    SELECT *
    FROM tlekdw_fraud.fraudcaseresult
    WHERE {time_filter}
    ORDER BY "timeInvestigation" DESC
    LIMIT 1000;
    """

    # 2) fallback ถ้าไม่มีคอลัมน์ timeInvestigation
    query2 = """
    SELECT *
    FROM tlekdw_fraud.fraudcaseresult
    LIMIT 1000;
    """

    try:
        df = pd.read_sql(query1, conn)
    except Exception:
        df = pd.read_sql(query2, conn)

    conn.close()

    if df.empty:
        return df

    # Filter fraud_types (ทำงานเมื่อมีคอลัมน์ fraudtype)
    if "fraudtype" in df.columns and fraud_types and ("All" not in fraud_types):
        df = df[df["fraudtype"].astype(str).isin(list(fraud_types))].copy()

    # Create severity (ทำงานเมื่อมี "Reason Fraud")
    if "Reason Fraud" in df.columns:
        reason = df["Reason Fraud"].astype(str)
        df["severity"] = "LOW"
        df.loc[reason.str.contains("CLOSE|KEEP", case=False, na=False), "severity"] = "HIGH"
        df.loc[reason.str.contains("suspected", case=False, na=False), "severity"] = "MEDIUM"
    else:
        df["severity"] = "LOW"

    return df


# =========================
# Load data
# =========================
with st.spinner("Loading fraud data..."):
    df = load_fraud_data(
        db_host, db_port, db_name, db_user, db_password,
        time_range, tuple(fraud_types)
    )

if df.empty:
    st.warning("⚠️ No fraud data found. Please check database connection / table / filters.")
    st.stop()

st.session_state.last_refresh = datetime.now()

# =========================
# Summary Statistics
# =========================
st.subheader("📊 Summary Statistics")
c1, c2, c3, c4 = st.columns(4)

with c1:
    st.metric("🚨 Total Cases", len(df))

with c2:
    high_risk = int((df["severity"] == "HIGH").sum()) if "severity" in df.columns else 0
    st.metric("⚠️ High Risk", high_risk, delta=f"{round(high_risk/max(len(df),1)*100,1)}%")

with c3:
    if "timeInvestigation" in df.columns:
        today_cases = int((pd.to_datetime(df["timeInvestigation"], errors="coerce").dt.date == datetime.now().date()).sum())
    else:
        today_cases = 0
    st.metric("📅 Today", today_cases)

with c4:
    if "Branch" in df.columns:
        st.metric("🏪 Affected Branches", df["Branch"].nunique())
    else:
        st.metric("🏪 Affected Branches", 0)

st.divider()

# =========================
# Charts
# =========================
col1, col2 = st.columns(2)

with col1:
    st.subheader("📊 Fraud by Type")
    if "fraudtype" in df.columns:
        fraud_counts = df["fraudtype"].astype(str).value_counts()
        fig_pie = px.pie(values=fraud_counts.values, names=fraud_counts.index, title="Fraud Type Distribution", hole=0.4)
        st.plotly_chart(fig_pie, use_container_width=True)
    else:
        st.info("ไม่มีคอลัมน์ fraudtype")

with col2:
    st.subheader("📈 Fraud by Zone")
    if "zone_name" in df.columns:
        zone_counts = df["zone_name"].astype(str).value_counts().head(10)
        fig_bar = px.bar(x=zone_counts.values, y=zone_counts.index, orientation='h', title="Top 10 Zones",
                         labels={'x': 'Cases', 'y': 'Zone'})
        st.plotly_chart(fig_bar, use_container_width=True)
    else:
        st.info("ไม่มีคอลัมน์ zone_name")

# Trend
st.subheader("📉 Fraud Trend")
if "timeInvestigation" in df.columns:
    dtt = pd.to_datetime(df["timeInvestigation"], errors="coerce")
    tmp = df.assign(_date=dtt.dt.date).dropna(subset=["_date"])
    daily_counts = tmp.groupby("_date").size().reset_index(name="count")
    fig_line = px.line(daily_counts, x="_date", y="count", title="Daily Fraud Cases", markers=True)
    st.plotly_chart(fig_line, use_container_width=True)
else:
    st.info("ไม่มีคอลัมน์ timeInvestigation จึงทำกราฟ trend ไม่ได้")

st.divider()

# =========================
# Recent Cases Table
# =========================
st.subheader("📋 Recent Fraud Cases")

# เลือกคอลัมน์ที่มีจริงเท่านั้น (กันพัง)
preferred_cols = [
    "No", "Branch", "zone_name", "Reason Fraud",
    "fraudtype", "fraudamount", "Fraudstep", "severity", "timeInvestigation"
]
show_cols = [c for c in preferred_cols if c in df.columns]
if not show_cols:
    show_cols = list(df.columns)[:12]

left, right = st.columns([3, 1])
with left:
    st.write(f"Showing {len(df)} cases")
with right:
    if st.button("📥 Export CSV"):
        csv = df.to_csv(index=False)
        st.download_button(
            label="Download",
            data=csv,
            file_name=f"fraud_cases_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )

st.dataframe(df[show_cols].head(50), use_container_width=True)

st.divider()

# =========================
# Live Alerts
# =========================
st.subheader("🔴 Live Fraud Alerts")

high_priority = df[df["severity"] == "HIGH"].head(5) if "severity" in df.columns else pd.DataFrame()

if not high_priority.empty:
    for idx, row in high_priority.iterrows():
        title_branch = row["Branch"] if "Branch" in high_priority.columns else "Unknown Branch"
        title_type = row["fraudtype"] if "fraudtype" in high_priority.columns else "Unknown Type"
        title_time = row["timeInvestigation"] if "timeInvestigation" in high_priority.columns else ""

        with st.expander(f"🚨 {title_branch} - {title_type} ({title_time})", expanded=(idx == high_priority.index[0])):
            if "Branch" in high_priority.columns: st.write(f"**Branch:** {row['Branch']}")
            if "zone_name" in high_priority.columns: st.write(f"**Zone:** {row['zone_name']}")
            if "Reason Fraud" in high_priority.columns: st.write(f"**Reason:** {row['Reason Fraud']}")
            if "fraudamount" in high_priority.columns: st.write(f"**Amount:** {row['fraudamount']}")
            st.error("⚠️ HIGH RISK")
else:
    st.success("✅ No high-risk cases detected!")

# =========================
# Auto-refresh
# =========================
if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()
