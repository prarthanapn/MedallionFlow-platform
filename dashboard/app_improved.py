import streamlit as st
import pandas as pd
import os
import requests
import time
import plotly.express as px
from streamlit_extras.metric_cards import metric_cards
from streamlit_extras.switch_page_button import switch_page_button
from datetime import datetime

# Page config
st.set_page_config(
    page_title="Data Pipeline Dashboard",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
.main-header {
    font-size: 3rem;
    color: #1f77b4;
    text-align: center;
    margin-bottom: 2rem;
}
.metric-card {
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    padding: 1rem;
    border-radius: 10px;
    color: white;
}
</style>
""", unsafe_allow_html=True)

# Paths
DATA_DIR = "/app/data"
BRONZE_PATH = os.path.join(DATA_DIR, "delta", "bronze")
SILVER_PATH = os.path.join(DATA_DIR, "delta", "silver")
GOLD_PATH = os.path.join(DATA_DIR, "delta", "gold")
INPUT_PATH = os.path.join(DATA_DIR, "input")

st.markdown('<h1 class="main-header">🚀 Data Pipeline Dashboard</h1>', unsafe_allow_html=True)

# Sidebar
with st.sidebar:
    st.header("📊 Controls")
    
    # File upload
    uploaded_file = st.file_uploader("📁 Upload CSV/JSON", type=["csv", "json"])
    filename = uploaded_file.name if uploaded_file else None
    
    if uploaded_file is not None:
        file_path = os.path.join(INPUT_PATH, filename)
        with open(file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        st.success(f"✅ {filename} uploaded!")
    
    # Trigger DAG
    dag_id = "event_driven_data_pipeline"
    if st.button("🔥 Trigger Pipeline", type="primary") and filename:
        with st.spinner("Triggering DAG..."):
            conf = {"file_name": filename}
            response = requests.post(
                f"http://airflow-webserver:8080/api/v1/dags/{dag_id}/dagRuns",
                auth=("admin", "admin"),
                json={"conf": conf}
            )
            if response.status_code == 200:
                st.success("✅ DAG triggered!")
                st.session_state.dag_run_id = response.json()["dag_run_id"]
                st.rerun()
            else:
                st.error(f"❌ Failed: {response.text}")

# Airflow Status
if 'dag_run_id' in st.session_state:
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Latest DAG Run", st.session_state.dag_run_id)
    with col2:
        if st.button("Check Status"):
            status = requests.get(
                f"http://airflow-webserver:8080/api/v1/dags/{dag_id}/dagRuns/{st.session_state.dag_run_id}",
                auth=("admin", "admin")
            ).json()
            st.metric("Status", status["state"] or "Running")

# Data layer status with enhanced metrics
st.header("📈 Layer Status")
def read_delta_table(path):
    try:
        parquet_files = [os.path.join(root, file) for root, _, files in os.walk(path) for file in files if file.endswith('.parquet')]
        if parquet_files:
            df = pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True)
            return df
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error reading {path}: {e}")
        return pd.DataFrame()

def get_layer_info(path, name):
    df = read_delta_table(path)
    last_updated = df["_ingest_timestamp"].max().strftime('%Y-%m-%d %H:%M') if not df.empty and "_ingest_timestamp" in df.columns else "N/A"
    cols = list(df.columns)[:5] if not df.empty else []
    return {"name": name, "rows": len(df), "last_updated": last_updated, "columns": cols, "df": df}

bronze = get_layer_info(BRONZE_PATH, "Bronze 🗡️")
silver = get_layer_info(SILVER_PATH, "Silver ⚡")
gold = get_layer_info(GOLD_PATH, "Gold 🏆")

# Metrics cards
cards = [
    {"label": bronze["name"], "value": f"{bronze['rows']:,}", "delta": "rows"},
    {"label": silver["name"], "value": f"{silver['rows']:,}", "delta": "rows"},
    {"label": gold["name"], "value": f"{gold['rows']:,}", "delta": "rows"}
]
metric_cards(cards, orientation="horizontal")

col1, col2, col3 = st.columns(3)
with col1:
    st.info(f"**Last Update:** {bronze['last_updated']}")
    st.caption(", ".join(bronze["columns"]))
with col2:
    st.info(f"**Last Update:** {silver['last_updated']}")
    st.caption(", ".join(silver["columns"]))
with col3:
    st.info(f"**Last Update:** {gold['last_updated']}")
    st.caption(", ".join(gold["columns"]))

# Charts
st.header("📊 Visualizations")
df_all = pd.DataFrame({
    "Layer": ["Bronze", "Silver", "Gold"],
    "Rows": [bronze['rows'], silver['rows'], gold['rows']]
})
fig_bar = px.bar(df_all, x="Layer", y="Rows", title="Row Counts", color="Rows", color_continuous_scale="viridis")
st.plotly_chart(fig_bar, use_container_width=True)

# Sample data
st.header("🔍 Sample Data")
layer_options = {"Bronze 🗡️": BRONZE_PATH, "Silver ⚡": SILVER_PATH, "Gold 🏆": GOLD_PATH}
selected_layer = st.selectbox("Select Layer", list(layer_options.keys()))
df_sample = read_delta_table(layer_options[selected_layer])
if not df_sample.empty:
    st.dataframe(df_sample.head(10), use_container_width=True)
else:
    st.warning("No data available for this layer yet.")

# Auto-refresh
time.sleep(2)
st.rerun()
