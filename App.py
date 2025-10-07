import streamlit as st
import requests
import pandas as pd

st.set_page_config(page_title="Promo Fraud Detector", layout="wide")

st.title("📊 Fraud Detection Dashboard – Promo Code Abuse")

# Webhook URL ที่เชื่อมกับ n8n
webhook_url = "https://n8n.madt.pro/webhook/d07b6b27-ec85-4432-a7dd-9f838914d89e"

# ปุ่มเพื่อโหลดข้อมูล
if st.button("🔍 Run Fraud Analysis"):
    st.info("⌛ Sending request to Agentic AI via n8n...")

    try:
        response = requests.post(webhook_url)
        if response.status_code == 200:
            data = response.json()

            # สร้าง DataFrame
            df = pd.DataFrame(data)

            # แสดงผลรวม
            st.subheader("📌 Summary Stats")
            col1, col2, col3 = st.columns(3)
            col1.metric("🧾 Total Records", len(df))
            col2.metric("🚨 Fraud Cases", df['fraud_flag'].value_counts().get("Y", 0))
            col3.metric("✅ Non-Fraud Cases", df['fraud_flag'].value_counts().get("N", 0))

            # Filter Fraud Cases
            fraud_cases = df[df['fraud_flag'] == "Y"]

            # แสดงตาราง fraud
            st.subheader("🚩 Detected Fraud Cases")
            st.dataframe(fraud_cases)

            # Group by fraud reason
            fraud_grouped = fraud_cases.groupby(["fraud_reason", "summary_reason_group"]).size().reset_index(name="count")
            st.subheader("📌 Fraud Reasons Summary")
            st.dataframe(fraud_grouped)

            # ดาวน์โหลด
            csv = fraud_cases.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download Fraud Cases as CSV",
                data=csv,
                file_name="fraud_cases.csv",
                mime='text/csv',
            )
        else:
            st.error(f"❌ Request failed with status code: {response.status_code}")
    except Exception as e:
        st.error(f"🚫 Error occurred: {str(e)}")
