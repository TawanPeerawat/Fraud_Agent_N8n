"""
🛡️ T-Lex Fraud Monitoring Dashboard
With Interactive Map + Bubble Visualization
"""

import re
import time
from datetime import datetime
from urllib.parse import quote_plus

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text

# Page Config
st.set_page_config(
    page_title="T-Lex Fraud Monitoring",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        background: linear-gradient(135deg, #0DA192 0%, #1E3A8A 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        margin-bottom: 2rem;
    }
    div[data-testid="metric-container"] {
        background: linear-gradient(135deg, #E0F2F1 0%, #B2EBF2 100%);
        border-radius: 10px;
        padding: 15px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #F0F9FF 0%, #E0F2F1 100%);
    }
    .stButton>button {
        background: linear-gradient(135deg, #0DA192 0%, #1E3A8A 100%);
        color: white;
        border-radius: 5px;
        border: none;
        padding: 0.5rem 1rem;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)

# Sidebar
with st.sidebar:
    st.title("⚙️ Configuration")
    
    with st.expander("📊 Database Connection", expanded=False):
        db_host = st.text_input("Host", value="n8n.madt.pro")
        db_port = st.text_input("Port", value="5432")
        db_name = st.text_input("Database", value="tlex_suki_db")
        db_user = st.text_input("Username", value="alex888")
        db_password = st.text_input("Password", type="password", value="is2025")
    
    st.divider()
    st.subheader("🔍 Filters")
    
    time_range = st.selectbox(
        "Time Range",
        ["Last 24 Hours", "Last 7 Days", "Last 30 Days", "Last 90 Days", "All Time"],
        index=1
    )
    
    fraud_types = st.multiselect(
        "Fraud Types",
        [
            "branch_risk_exposure",
            "customer_staff_collusion",
            "late_night_high_spend",
            "queue_low_value_anomaly",
            "inventory_fraud"
        ],
        default=[]
    )
    
    severity_filter = st.multiselect("Severity", ["HIGH", "MEDIUM", "LOW"], default=[])
    
    st.divider()
    
    col1, col2 = st.columns(2)
    with col1:
        auto_refresh = st.checkbox("Auto Refresh", value=False)
    with col2:
        refresh_interval = st.number_input("Interval (s)", 30, 600, 60)
    
    if st.button("🔄 Refresh Now", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# Header
st.markdown('<div class="main-header">🛡️ T-Lex Fraud Monitoring Dashboard</div>', unsafe_allow_html=True)

# Database Functions
@st.cache_resource
def get_engine(host, port, db, user, password):
    pw = quote_plus(password)
    url = f"postgresql+psycopg://{user}:{pw}@{host}:{int(port)}/{db}"
    return create_engine(url, pool_pre_ping=True)

def parse_reason_fraud(reason):
    if pd.isna(reason):
        return {}
    
    data = {}
    reason_str = str(reason)
    
    patterns = {
        'risk_score': r'RiskScore:\s*(\d+)',
        'ebitda': r'EBITDA:\s*(\d+)%',
        'server': r'Server:\s*(\w+)',
        'tx_id': r'TX:\s*(TX\d+)',
    }
    
    for key, pattern in patterns.items():
        match = re.search(pattern, reason_str)
        if match:
            value = match.group(1).strip()
            if key in ['risk_score', 'ebitda']:
                data[key] = int(value)
            else:
                data[key] = value
    
    return data

def extract_branch_id(branch_text):
    if pd.isna(branch_text):
        return None
    m = re.search(r'\((B\d+)\)', str(branch_text))
    return m.group(1) if m else None

@st.cache_data(ttl=60)
def load_fraud_data(time_range, fraud_types, severity_filter, host, port, db, user, password):
    engine = get_engine(host, port, db, user, password)
    
    time_filters = {
        "Last 24 Hours": '"timeInvestigation" >= NOW() - INTERVAL \'24 hours\'',
        "Last 7 Days": '"timeInvestigation" >= NOW() - INTERVAL \'7 days\'',
        "Last 30 Days": '"timeInvestigation" >= NOW() - INTERVAL \'30 days\'',
        "Last 90 Days": '"timeInvestigation" >= NOW() - INTERVAL \'90 days\'',
        "All Time": "1=1"
    }
    time_filter = time_filters.get(time_range, "1=1")
    
    fraud_filter = ""
    if fraud_types:
        fraud_list = "(" + ",".join(f"'{ft}'" for ft in fraud_types) + ")"
        fraud_filter = f" AND fraudtype IN {fraud_list}"
    
    sql = text(f"""
        SELECT 
            "No", "Fraudcases_count", "Branch", "zone_name", "Reason Fraud",
            fraudtype, "timeInvestigation", fraudamount, "Fraudster"
        FROM tlekdw_fraud.fraudcaseresult
        WHERE {time_filter} {fraud_filter}
        ORDER BY "timeInvestigation" DESC
        LIMIT 10000
    """)
    
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)
    
    if df.empty:
        return df
    
    df['branch_id'] = df['Branch'].apply(extract_branch_id)
    
    parsed_data = df['Reason Fraud'].apply(parse_reason_fraud)
    parsed_df = pd.DataFrame(parsed_data.tolist())
    df = pd.concat([df, parsed_df], axis=1)
    
    df['severity'] = 'LOW'
    if 'risk_score' in df.columns:
        df.loc[df['risk_score'] >= 50, 'severity'] = 'HIGH'
        df.loc[(df['risk_score'] >= 30) & (df['risk_score'] < 50), 'severity'] = 'MEDIUM'
    
    if severity_filter:
        df = df[df['severity'].isin(severity_filter)]
    
    # Load branch master
    branch_ids = df['branch_id'].dropna().unique().tolist()
    if branch_ids:
        branch_sql = "(" + ",".join(f"'{bid}'" for bid in branch_ids) + ")"
        sql_branch = text(f"""
            SELECT branch_id, branch_name, province, latitude, longitude
            FROM tlekdw_operation.dim_branch
            WHERE branch_id IN {branch_sql}
        """)
        with engine.connect() as conn:
            branch_df = pd.read_sql(sql_branch, conn)
        df = df.merge(branch_df, on='branch_id', how='left')
    
    df['timeInvestigation'] = pd.to_datetime(df['timeInvestigation'])
    df['date'] = df['timeInvestigation'].dt.date
    df['hour'] = df['timeInvestigation'].dt.hour
    df['day_of_week'] = df['timeInvestigation'].dt.day_name()
    
    return df

# Load Data
with st.spinner("🔍 Loading fraud data..."):
    df = load_fraud_data(
        time_range,
        tuple(fraud_types) if fraud_types else (),
        tuple(severity_filter) if severity_filter else (),
        db_host, db_port, db_name, db_user, db_password
    )

if df.empty:
    st.warning("⚠️ No fraud data found")
    st.stop()

# KPI Cards
st.subheader("📊 Summary Statistics")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("🚨 Total Cases", f"{len(df):,}")

with col2:
    high_risk = (df['severity'] == 'HIGH').sum()
    st.metric("⚠️ High Risk", f"{high_risk:,}")

with col3:
    total_amount = df['fraudamount'].sum()
    st.metric("💰 Total Amount", f"฿{total_amount:,.0f}")

with col4:
    branches = df['branch_id'].nunique()
    st.metric("🏪 Affected Branches", f"{branches}")

st.divider()

# Fraud Map
st.subheader("🗺️ Fraud Map (click point to drill down)")

map_df = df.dropna(subset=['latitude', 'longitude']).copy()

if not map_df.empty:
    # Size by fraud amount
    if 'fraudamount' in map_df.columns:
        map_df['fraudamount_num'] = pd.to_numeric(map_df['fraudamount'], errors='coerce').fillna(0)
        max_amount = map_df['fraudamount_num'].max()
        if max_amount > 0:
            map_df['bubble_size'] = (map_df['fraudamount_num'] / max_amount * 80) + 15
        else:
            map_df['bubble_size'] = 20
    else:
        map_df['bubble_size'] = 20
    
    # Color by fraud type
    fraud_colors = {
        'branch_risk_exposure': '#0DA192',
        'customer_staff_collusion': '#1E3A8A',
        'late_night_high_spend': '#8B5CF6',
        'queue_low_value_anomaly': '#F59E0B',
        'inventory_fraud': '#EC4899'
    }
    
    fig_map = px.scatter_mapbox(
        map_df,
        lat='latitude',
        lon='longitude',
        size='bubble_size',
        color='fraudtype',
        hover_name='branch_name',
        hover_data={
            'Branch': True,
            'branch_id': True,
            'fraudtype': True,
            'severity': True,
            'fraudamount': ':,.2f',
            'province': True,
            'latitude': False,
            'longitude': False,
            'bubble_size': False
        },
        color_discrete_map=fraud_colors,
        zoom=5,
        height=600
    )
    
    fig_map.update_layout(
        mapbox_style='open-street-map',
        margin=dict(l=0, r=0, t=0, b=0),
        legend=dict(orientation="h", yanchor="bottom", y=-0.1, xanchor="center", x=0.5)
    )
    
    map_selection = st.plotly_chart(fig_map, use_container_width=True, on_select='rerun', key='fraud_map')
    
    # Drilldown
    st.subheader("📌 Selected Branch Details")
    
    if map_selection and map_selection.get('selection') and map_selection['selection'].get('points'):
        points = map_selection['selection']['points']
        if points:
            idx = points[0]['pointIndex']
            selected = map_df.iloc[idx]
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Branch", selected.get('branch_name', 'N/A'))
            
            with col2:
                st.metric("Province", selected.get('province', 'N/A'))
            
            with col3:
                st.metric("Fraud Type", selected.get('fraudtype', 'N/A'))
            
            with col4:
                st.metric("Amount", f"฿{selected.get('fraudamount', 0):,.2f}")
            
            # Show all cases for this branch
            branch_id = selected.get('branch_id')
            if branch_id:
                branch_cases = df[df['branch_id'] == branch_id].copy()
                st.write(f"### All Cases for {selected.get('branch_name', 'this branch')} ({len(branch_cases)} cases)")
                
                st.dataframe(
                    branch_cases[['No', 'timeInvestigation', 'fraudtype', 'fraudamount', 'severity']],
                    use_container_width=True,
                    height=300
                )
    else:
        st.info("👉 Click a point on the map to see branch details")
else:
    st.warning("⚠️ No location data available")

st.divider()

# Charts
col1, col2 = st.columns(2)

with col1:
    st.subheader("📊 Fraud by Type")
    type_counts = df['fraudtype'].value_counts()
    fig_type = px.pie(
        values=type_counts.values,
        names=type_counts.index,
        hole=0.5,
        color_discrete_sequence=['#0DA192', '#1E3A8A', '#8B5CF6', '#F59E0B', '#EF4444']
    )
    fig_type.update_layout(height=400, showlegend=True)
    st.plotly_chart(fig_type, use_container_width=True)

with col2:
    st.subheader("📊 Fraud by Province")
    if 'province' in df.columns:
        province_df = df['province'].value_counts().head(10).reset_index()
        province_df.columns = ['Province', 'Count']
        fig_prov = px.bar(
            province_df,
            x='Count',
            y='Province',
            orientation='h',
            color='Count',
            color_continuous_scale='Teal'
        )
        fig_prov.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig_prov, use_container_width=True)

st.divider()

# Recent Cases
st.subheader("📋 Recent Fraud Cases")

show_cols = ['No', 'timeInvestigation', 'Branch', 'zone_name', 'province', 'fraudtype', 'fraudamount', 'severity']
show_cols = [c for c in show_cols if c in df.columns]

st.dataframe(
    df[show_cols].head(100),
    use_container_width=True,
    height=400,
    column_config={
        "timeInvestigation": st.column_config.DatetimeColumn("Time", format="DD/MM/YY HH:mm"),
        "fraudamount": st.column_config.NumberColumn("Amount (฿)", format="%.2f")
    }
)

# Export
if st.button("📥 Export to CSV"):
    csv = df.to_csv(index=False)
    st.download_button(
        "Download CSV",
        csv,
        f"fraud_cases_{datetime.now().strftime('%Y%m%d')}.csv",
        "text/csv"
    )

# Auto Refresh
if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()
