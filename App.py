"""
🛡️ Simple Fraud Detection Dashboard
แสดงข้อมูล Fraud Cases แบบ Real-time จาก Database + n8n webhook
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time

# Page config
st.set_page_config(
    page_title="Fraud Detection Dashboard",
    page_icon="🛡️",
    layout="wide"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #FF4B4B;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 10px;
        color: white;
        text-align: center;
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

# Initialize session state
if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = datetime.now()

# Sidebar
with st.sidebar:
    st.title("⚙️ Settings")
    
    # Database connection
    st.subheader("📊 Database")
    db_host = st.text_input("Host", value="n8n.madt.pro")
    db_port = st.text_input("Port", value="5432")
    db_name = st.text_input("Database", value="user7_db")
    db_user = st.text_input("Username", value="bu7_user")
    db_password = st.text_input("Password", type="password", value=")4+vroymo(u47wO$6tNo")
    
    st.divider()
    
    # Refresh settings
    st.subheader("🔄 Auto Refresh")
    auto_refresh = st.checkbox("Enable Auto Refresh", value=True)
    refresh_interval = st.slider("Interval (seconds)", 10, 120, 30)
    
    st.divider()
    
    # Filters
    st.subheader("🔍 Filters")
    time_range = st.selectbox(
        "Time Range",
        ["Last 24 Hours", "Last 7 Days", "Last 30 Days", "All Time"]
    )
    
    fraud_types = st.multiselect(
        "Fraud Types",
        ["branch_risk_exposure", "customer_staff_collusion", 
         "late_night_high_spend", "operational_risk", "All"],
        default=["All"]
    )
    
    if st.button("🔄 Refresh Now"):
        st.rerun()
    
    st.divider()
    st.caption(f"Last updated: {st.session_state.last_refresh.strftime('%H:%M:%S')}")

# Header
st.markdown('<div class="main-header">🛡️ Fraud Detection Dashboard</div>', unsafe_allow_html=True)

# Database connection helper
@st.cache_resource
def get_database_connection():
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )
        return conn
    except Exception as e:
        st.error(f"❌ Database connection failed: {e}")
        return None

# Load data from database
@st.cache_data(ttl=30)
def load_fraud_data():
    """Load fraud cases from database"""
    try:
        conn = get_database_connection()
        if not conn:
            return pd.DataFrame()
        
        # Calculate time filter
        time_filters = {
            "Last 24 Hours": "\"timeInvestigation\" >= NOW() - INTERVAL '24 hours'",
            "Last 7 Days": "\"timeInvestigation\" >= NOW() - INTERVAL '7 days'",
            "Last 30 Days": "\"timeInvestigation\" >= NOW() - INTERVAL '30 days'",
            "All Time": "1=1"
        }
        
        time_filter = time_filters.get(time_range, "1=1")
        
        query = f"""
        SELECT 
            "No",
            "Fraudcases.",
            "Branch",
            "zone_name",
            "Reason Fraud",
            "fraudtype",
            "timeInvestigation",
            "fraudamount",
            "Fraudstep",
            CASE 
                WHEN "Reason Fraud" LIKE '%CLOSE%' OR "Reason Fraud" LIKE '%KEEP%' 
                THEN 'HIGH'
                WHEN "Reason Fraud" LIKE '%suspected%' 
                THEN 'MEDIUM'
                ELSE 'LOW'
            END as severity
        FROM tlekdw_fraud.fraudcasesresult
        WHERE {time_filter}
        ORDER BY "timeInvestigation" DESC
        LIMIT 1000;
        """
        
        df = pd.read_sql(query, conn)
        conn.close()
        
        return df
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()

# Load data
with st.spinner("Loading fraud data..."):
    df = load_fraud_data()

if df.empty:
    st.warning("⚠️ No fraud data found. Please check database connection.")
    st.stop()

# Update last refresh time
st.session_state.last_refresh = datetime.now()

# === SECTION 1: STATISTICS CARDS ===
st.subheader("📊 Summary Statistics")

col1, col2, col3, col4 = st.columns(4)

with col1:
    total_cases = len(df)
    st.metric("🚨 Total Cases", total_cases)

with col2:
    high_risk = len(df[df['severity'] == 'HIGH'])
    st.metric("⚠️ High Risk", high_risk, delta=f"{round(high_risk/max(total_cases,1)*100,1)}%")

with col3:
    today_cases = len(df[pd.to_datetime(df['timeInvestigation']).dt.date == datetime.now().date()])
    st.metric("📅 Today", today_cases)

with col4:
    unique_branches = df['Branch'].nunique()
    st.metric("🏪 Affected Branches", unique_branches)

st.divider()

# === SECTION 2: CHARTS ===
col1, col2 = st.columns(2)

with col1:
    st.subheader("📊 Fraud by Type")
    fraud_counts = df['fraudtype'].value_counts()
    fig_pie = px.pie(
        values=fraud_counts.values,
        names=fraud_counts.index,
        title="Fraud Type Distribution",
        hole=0.4
    )
    st.plotly_chart(fig_pie, use_container_width=True)

with col2:
    st.subheader("📈 Fraud by Zone")
    zone_counts = df['zone_name'].value_counts().head(10)
    fig_bar = px.bar(
        x=zone_counts.values,
        y=zone_counts.index,
        orientation='h',
        title="Top 10 Zones",
        labels={'x': 'Cases', 'y': 'Zone'}
    )
    st.plotly_chart(fig_bar, use_container_width=True)

# Trend Chart
st.subheader("📉 Fraud Trend")
df['date'] = pd.to_datetime(df['timeInvestigation']).dt.date
daily_counts = df.groupby('date').size().reset_index(name='count')
fig_line = px.line(
    daily_counts,
    x='date',
    y='count',
    title='Daily Fraud Cases',
    markers=True
)
st.plotly_chart(fig_line, use_container_width=True)

st.divider()

# === SECTION 3: RECENT CASES TABLE ===
st.subheader("📋 Recent Fraud Cases")

# Display options
col1, col2 = st.columns([3, 1])
with col1:
    st.write(f"Showing {len(df)} cases")
with col2:
    if st.button("📥 Export CSV"):
        csv = df.to_csv(index=False)
        st.download_button(
            label="Download",
            data=csv,
            file_name=f"fraud_cases_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )

# Table
st.dataframe(
    df[[
        'No', 'Branch', 'zone_name', 'Reason Fraud',
        'fraudtype', 'fraudamount', 'Fraudstep', 'severity', 'timeInvestigation'
    ]].head(50),
    use_container_width=True,
    column_config={
        "timeInvestigation": st.column_config.DatetimeColumn(
            "Investigation Time",
            format="YYYY-MM-DD HH:mm:ss"
        ),
        "fraudamount": st.column_config.NumberColumn(
            "Amount",
            format="%.2f"
        ),
        "severity": st.column_config.Column(
            "Severity",
            width="small"
        )
    }
)

st.divider()

# === SECTION 4: REAL-TIME ALERTS ===
st.subheader("🔴 Live Fraud Alerts")

# Show latest 5 high-priority cases
high_priority = df[df['severity'] == 'HIGH'].head(5)

if not high_priority.empty:
    for idx, row in high_priority.iterrows():
        with st.expander(f"🚨 {row['Branch']} - {row['fraudtype']} ({row['timeInvestigation']})", expanded=(idx == high_priority.index[0])):
            col1, col2 = st.columns([2, 1])
            with col1:
                st.write(f"**Branch:** {row['Branch']}")
                st.write(f"**Zone:** {row['zone_name']}")
                st.write(f"**Reason:** {row['Reason Fraud']}")
                st.write(f"**Amount:** {row['fraudamount']}")
            with col2:
                st.error("⚠️ HIGH RISK")
                st.write(f"**Type:** {row['fraudtype']}")
                st.write(f"**Step:** {row['Fraudstep']}")
else:
    st.success("✅ No high-risk cases detected!")

# Auto-refresh functionality
if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()
