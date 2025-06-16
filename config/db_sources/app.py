import streamlit as st
import pandas as pd
import json
import psycopg2
from pathlib import Path
from datetime import datetime

# Title
st.title("🧹 Data Quality Checker")

# Load DB credentials from secrets
db_info = {
    "host": st.secrets["host"],
    "port": st.secrets["port"],
    "database": st.secrets["database"],
    "username": st.secrets["username"],
    "password": st.secrets["password"]
}

# Connect to database
try:
    conn = psycopg2.connect(
        dbname=db_info["database"],
        user=db_info["username"],
        password=db_info["password"],
        host=db_info["host"],
        port=db_info["port"]
    )
    st.success("✅ Connected to the database")
except Exception as e:
    st.error(f"❌ Connection failed: {e}")
    st.stop()

# Example: Fetch recent DQ config status
try:
    cur = conn.cursor()
    cur.execute("SELECT id, status, created_at FROM dq_rule_config ORDER BY created_at DESC LIMIT 1")
    result = cur.fetchone()
    if result:
        st.write("### Latest DQ Run")
        st.json({
            "id": result[0],
            "status": result[1],
            "created_at": result[2].strftime("%Y-%m-%d %H:%M:%S")
        })
    else:
        st.warning("No DQ records found.")
    cur.close()
except Exception as e:
    st.error(f"❌ Error fetching data: {e}")

conn.close()
