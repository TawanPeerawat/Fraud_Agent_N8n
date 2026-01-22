"""
🛡️ T-Lex Fraud Monitoring Dashboard
8 Fraud Types: Internal (5) + External (3)
Full Charts + Map + Taxonomy
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

# =========================
# Fraud Type Taxonomy
# =========================
FRAUD_TAXONOMY = {
    'INTERNAL': {
        'employee_transaction_manipulation': {
            'name': 'Employee Transaction Manipulation',
            'name_th': 'พนักงานแทรกแซงธุรกรรม',
            'color': '#EF4444',
            'icon': '🔴'
        },
        'customer_staff_collusion': {
            'name': 'Employee–Customer Collusion',
            'name_th': 'พนักงานสมรู้ร่วมคิดกับลูกค้า',
            'color': '#F59E0B',
            'icon': '🟠'
        },
        'inventory_fraud': {
            'name': 'Inventory Fraud / Shrinkage',
            'name_th': 'ต้นทุน-สต็อกผิดปกติ',
            'color': '#8B5CF6',
            'icon': '🟣'
        },
        'branch_risk_exposure': {
            'name': 'Branch Financial Manipulation',
            'name_th': 'การบิดเบือนตัวเลขการเงิน',
            'color': '#EC4899',
            'icon': '🟢'
        },
        'branch_operational_risk': {
            'name': 'Operational / Productivity Fraud',
            'name_th': 'ประสิทธิภาพสาขาผิดปกติ',
            'color': '#06B6D4',
            'icon': '🔵'
        }
    },
    'EXTERNAL': {
        'promotion_abuse': {
            'name': 'Promotion Abuse',
            'name_th': 'การโกงแคมเปญ/ส่วนลด',
            'color': '#10B981',
            'icon': '🟡'
        },
        'late_night_high_spend': {
            'name': 'External Transaction Fraud',
            'name_th': 'พฤติกรรมธุรกรรมผิดธรรมชาติ',
            'color': '#3B82F6',
            'icon': '🔷'
        },
        'queue_low_value_anomaly': {
            'name': 'External Complaint Fraud',
            'name_th': 'ใช้ระบบร้องเรียนเพื่อเอาผลประโยชน์',
            'color': '#6366F1',
            'icon': '🔶'
        }
    }
}

FRAUD_COLORS = {}
FRAUD_NAMES = {}
for category in FRAUD_TAXONOMY.values():
    for key, info in category.items():
        FRAUD_COLORS[key] = info['color']
        FRAUD_NAMES[key] = info['name']

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
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        background: linear-gradient(135deg, #E0F2F1 0%, #B2EBF2 100%);
        border-radius: 5px;
        padding: 10px 20px;
    }
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #0DA192 0%, #1E3A8A 100%);
        color: white;
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
    
    st.write("**Internal Fraud (🟥)**")
    internal_types = list(FRAUD_TAXONOMY['INTERNAL'].keys())
    internal_selected = st.multiselect(
        "Select Internal Fraud Types",
        internal_types,
        format_func=lambda x: FRAUD_TAXONOMY['INTERNAL'][x]['name'],
        default=[]
    )
    
    st.write("**External Fraud (🟦)**")
    external_types = list(FRAUD_TAXONOMY['EXTERNAL'].keys())
    external_selected = st.multiselect(
        "Select External Fraud Types",
        external_types,
        format_func=lambda x: FRAUD_TAXONOMY['EXTERNAL'][x]['name'],
        default=[]
    )
    
    fraud_types = internal_selected + external_selected
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
    
    st.divider()
    
    if st.checkbox("📚 Show Fraud Taxonomy"):
        st.write("### 🟥 Internal Fraud")
        for key, info in FRAUD_TAXONOMY['INTERNAL'].items():
            st.markdown(f"**{info['icon']} {info['name']}**")
            st.caption(info['name_th'])
        
        st.write("### 🟦 External Fraud")
        for key, info in FRAUD_TAXONOMY['EXTERNAL'].items():
            st.markdown(f"**{info['icon']} {info['name']}**")
            st.caption(info['name_th'])

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
        'customer_key': r'CustomerKey:\s*(\d+)',
        'wait_time': r'Wait:\s*(\d+)m',
        'amount': r'Amount:\s*([\d.]+)',
    }
    
    for key, pattern in patterns.items():
        match = re.search(pattern, reason_str)
        if match:
            value = match.group(1).strip()
            if key in ['risk_score', 'ebitda', 'customer_key', 'wait_time']:
                data[key] = int(value)
            elif key in ['amount']:
                data[key] = float(value)
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
    
    df['fraud_category'] = df['fraudtype'].apply(
        lambda x: 'Internal' if x in FRAUD_TAXONOMY['INTERNAL'] else 'External'
    )
    
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
st.subheader("📊 Key Metrics")
col1, col2, col3, col4, col5 = st.columns(5)

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
    st.metric("🏪 Branches", f"{branches}")
with col5:
    internal_count = (df['fraud_category'] == 'Internal').sum()
    external_count = (df['fraud_category'] == 'External').sum()
    st.metric("🟥 Internal / 🟦 External", f"{internal_count} / {external_count}")

st.divider()

# Fraud Map
st.subheader("🗺️ Fraud Map (click point to drill down)")
map_df = df.dropna(subset=['latitude', 'longitude']).copy()

if not map_df.empty:
    if 'fraudamount' in map_df.columns:
        map_df['fraudamount_num'] = pd.to_numeric(map_df['fraudamount'], errors='coerce').fillna(0)
        max_amount = map_df['fraudamount_num'].max()
        if max_amount > 0:
            map_df['bubble_size'] = (map_df['fraudamount_num'] / max_amount * 80) + 15
        else:
            map_df['bubble_size'] = 20
    else:
        map_df['bubble_size'] = 20
    
    fig_map = px.scatter_mapbox(
        map_df,
        lat='latitude',
        lon='longitude',
        size='bubble_size',
        color='fraudtype',
        hover_name='branch_name',
        hover_data={
            'Branch': True,
            'fraudtype': True,
            'severity': True,
            'fraudamount': ':,.2f',
            'province': True,
            'latitude': False,
            'longitude': False,
            'bubble_size': False
        },
        color_discrete_map=FRAUD_COLORS,
        zoom=5,
        height=600
    )
    
    fig_map.update_layout(
        mapbox_style='open-street-map',
        margin=dict(l=0, r=0, t=0, b=0),
        legend=dict(orientation="h", yanchor="bottom", y=-0.1, xanchor="center", x=0.5)
    )
    
    map_selection = st.plotly_chart(fig_map, use_container_width=True, on_select='rerun', key='fraud_map')
    
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
                st.metric("Fraud Type", FRAUD_NAMES.get(selected.get('fraudtype'), 'N/A'))
            with col4:
                st.metric("Amount", f"฿{selected.get('fraudamount', 0):,.2f}")
            
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

# Row 1: Trend + Distribution
col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("📈 Fraud Trend Over Time")
    trend_df = df.groupby(['date', 'fraudtype']).size().reset_index(name='count')
    
    fig_trend = px.area(
        trend_df,
        x='date',
        y='count',
        color='fraudtype',
        title='Daily Fraud Cases by Type',
        color_discrete_map=FRAUD_COLORS
    )
    fig_trend.update_layout(height=400, hovermode='x unified')
    st.plotly_chart(fig_trend, use_container_width=True)

with col2:
    st.subheader("🎯 Fraud Type Distribution")
    type_counts = df['fraudtype'].value_counts()
    
    fig_type = px.pie(
        values=type_counts.values,
        names=[FRAUD_NAMES.get(x, x) for x in type_counts.index],
        hole=0.5,
        color=type_counts.index,
        color_discrete_map=FRAUD_COLORS
    )
    fig_type.update_layout(height=400)
    st.plotly_chart(fig_type, use_container_width=True)

st.divider()

# Row 2: Top Branches + Province
col1, col2 = st.columns(2)

with col1:
    st.subheader("🏪 Top 15 Branches by Fraud Cases")
    if 'branch_name' in df.columns:
        top_branches = df.groupby('branch_name').agg({
            'No': 'count',
            'fraudamount': 'sum',
            'severity': lambda x: (x == 'HIGH').sum()
        }).reset_index()
        top_branches.columns = ['Branch', 'Cases', 'Total Amount', 'High Risk']
        top_branches = top_branches.sort_values('Cases', ascending=True).tail(15)
        
        fig_branches = go.Figure()
        fig_branches.add_trace(go.Bar(
            y=top_branches['Branch'],
            x=top_branches['Cases'],
            orientation='h',
            marker=dict(
                color=top_branches['High Risk'],
                colorscale=[[0, '#B2EBF2'], [0.5, '#0DA192'], [1, '#EF4444']],
                showscale=True,
                colorbar=dict(title="High Risk")
            ),
            text=top_branches['Cases'],
            textposition='outside'
        ))
        fig_branches.update_layout(height=500)
        st.plotly_chart(fig_branches, use_container_width=True)

with col2:
    st.subheader("🗺️ Fraud by Province")
    if 'province' in df.columns:
        province_df = df.groupby('province').size().reset_index(name='count')
        province_df = province_df.sort_values('count', ascending=True).tail(15)
        
        fig_province = go.Figure()
        fig_province.add_trace(go.Bar(
            y=province_df['province'],
            x=province_df['count'],
            orientation='h',
            marker=dict(color='#0DA192'),
            text=province_df['count'],
            textposition='outside'
        ))
        fig_province.update_layout(height=500)
        st.plotly_chart(fig_province, use_container_width=True)

st.divider()

# Row 3: Severity + Amount
col1, col2 = st.columns(2)

with col1:
    st.subheader("⚡ Severity Breakdown by Type")
    severity_type = pd.crosstab(df['fraudtype'], df['severity'])
    severity_type = severity_type[[col for col in ['HIGH', 'MEDIUM', 'LOW'] if col in severity_type.columns]]
    
    fig_severity = go.Figure()
    colors = {'HIGH': '#EF4444', 'MEDIUM': '#F59E0B', 'LOW': '#00CC00'}
    
    for severity in ['HIGH', 'MEDIUM', 'LOW']:
        if severity in severity_type.columns:
            fig_severity.add_trace(go.Bar(
                name=severity,
                x=severity_type.index,
                y=severity_type[severity],
                marker_color=colors[severity]
            ))
    
    fig_severity.update_layout(height=400, barmode='stack')
    st.plotly_chart(fig_severity, use_container_width=True)

with col2:
    st.subheader("💵 Fraud Amount Distribution")
    fig_amount = go.Figure()
    fig_amount.add_trace(go.Box(
        y=df['fraudamount'],
        marker_color='#0DA192',
        boxmean='sd'
    ))
    fig_amount.update_layout(height=400, showlegend=False)
    st.plotly_chart(fig_amount, use_container_width=True)

st.divider()

# Time Heatmap
st.subheader("🕐 Fraud Pattern: Day of Week × Hour")
heatmap_data = df.groupby(['day_of_week', 'hour']).size().reset_index(name='count')
heatmap_pivot = heatmap_data.pivot(index='day_of_week', columns='hour', values='count').fillna(0)

day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
heatmap_pivot = heatmap_pivot.reindex([d for d in day_order if d in heatmap_pivot.index])

fig_heatmap = go.Figure(data=go.Heatmap(
    z=heatmap_pivot.values,
    x=heatmap_pivot.columns,
    y=heatmap_pivot.index,
    colorscale='Teal',
    showscale=True,
    colorbar=dict(title="Cases")
))
fig_heatmap.update_layout(height=400)
st.plotly_chart(fig_heatmap, use_container_width=True)

st.divider()

# Detailed Analysis Tabs
st.subheader("🔍 Detailed Analysis")

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📋 All Cases",
    "🏢 Branch Risk",
    "🤝 Collusion",
    "🌙 High Spend",
    "⏱️ Queue Anomaly"
])

with tab1:
    st.write("### All Fraud Cases")
    show_cols = ['No', 'timeInvestigation', 'Branch', 'zone_name', 'province', 'fraudtype', 'fraudamount', 'severity', 'Fraudster']
    for col in ['risk_score', 'ebitda', 'server', 'tx_id', 'customer_key']:
        if col in df.columns:
            show_cols.append(col)
    
    show_cols = [c for c in show_cols if c in df.columns]
    display_df = df[show_cols].copy()
    
    st.dataframe(display_df, use_container_width=True, height=500)
    
    if st.button("📥 Export All to CSV"):
        csv = df.to_csv(index=False)
        st.download_button("Download CSV", csv, f"fraud_cases_{datetime.now().strftime('%Y%m%d')}.csv", "text/csv")

with tab2:
    branch_risk = df[df['fraudtype'] == 'branch_risk_exposure'].copy()
    if not branch_risk.empty:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Cases", f"{len(branch_risk):,}")
        with col2:
            if 'risk_score' in branch_risk.columns:
                st.metric("Avg Risk Score", f"{branch_risk['risk_score'].mean():.1f}")
        with col3:
            if 'ebitda' in branch_risk.columns:
                st.metric("Avg EBITDA", f"{branch_risk['ebitda'].mean():.1f}%")
        st.dataframe(branch_risk.head(50), use_container_width=True)
    else:
        st.info("No branch risk cases found")

with tab3:
    collusion = df[df['fraudtype'] == 'customer_staff_collusion'].copy()
    if not collusion.empty:
        col1, col2 = st.columns([1, 2])
        with col1:
            st.metric("Total Cases", f"{len(collusion):,}")
            if 'server' in collusion.columns:
                st.write("### Top Servers")
                st.bar_chart(collusion['server'].value_counts().head(10))
        with col2:
            st.dataframe(collusion.head(30), use_container_width=True)
    else:
        st.info("No collusion cases found")

with tab4:
    high_spend = df[df['fraudtype'] == 'late_night_high_spend'].copy()
    if not high_spend.empty:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Cases", f"{len(high_spend):,}")
        with col2:
            if 'amount' in high_spend.columns:
                st.metric("Avg Amount", f"฿{high_spend['amount'].mean():,.0f}")
        with col3:
            st.metric("Max Amount", f"฿{high_spend['fraudamount'].max():,.0f}")
        st.dataframe(high_spend.head(50), use_container_width=True)
    else:
        st.info("No high spend cases found")

with tab5:
    queue = df[df['fraudtype'] == 'queue_low_value_anomaly'].copy()
    if not queue.empty:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Cases", f"{len(queue):,}")
        with col2:
            if 'wait_time' in queue.columns:
                st.metric("Avg Wait Time", f"{queue['wait_time'].mean():.0f} min")
        with col3:
            max_wait = queue['wait_time'].max() if 'wait_time' in queue.columns else 0
            st.metric("Max Wait Time", f"{max_wait:.0f} min")
        st.dataframe(queue.head(50), use_container_width=True)
    else:
        st.info("No queue anomaly cases found")

# Auto Refresh
if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()
