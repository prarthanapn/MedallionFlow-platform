import streamlit as st
import pandas as pd
import os
import requests
import time
import json
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
.lineage-card {
    background-color: #1e1e1e;
    border: 1px solid #333;
    border-radius: 8px;
    padding: 20px;
    margin-bottom: 10px;
    box-shadow: 0 4px 6px rgba(0,0,0,0.3);
    text-align: center;
    transition: transform 0.2s;
}
.lineage-card:hover {
    transform: translateY(-2px);
}
.arrow-down {
    text-align: center;
    font-size: 24px;
    color: #666;
    margin: 10px 0;
}
.status-badge {
    padding: 4px 8px;
    border-radius: 12px;
    font-size: 0.8em;
    font-weight: bold;
}
.status-success { background-color: #28a745; color: white; }
.status-failed { background-color: #dc3545; color: white; }
.status-pending { background-color: #ffc107; color: black; }
</style>
""", unsafe_allow_html=True)

# Paths
DATA_DIR = "/app/data"
BRONZE_PATH = os.path.join(DATA_DIR, "delta", "bronze")
SILVER_PATH = os.path.join(DATA_DIR, "delta", "silver")
GOLD_PATH = os.path.join(DATA_DIR, "delta", "gold")
INPUT_PATH = os.path.join(DATA_DIR, "input")
METADATA_DIR = "/app/metadata"

# Sidebar
with st.sidebar:
    st.header("Navigation")
    page = st.radio("", ["📊 Dashboard", "📁 Data Explorer", "📈 Metrics", "🔄 Pipeline Status", "🌐 Data Lineage"])
    
    st.divider()
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

if page == "📊 Dashboard":
    st.markdown('<h1 class="main-header">🚀 Data Pipeline Dashboard</h1>', unsafe_allow_html=True)
    
    # Airflow Status
    if 'dag_run_id' in st.session_state:
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Latest DAG Run", st.session_state.dag_run_id)
        with col2:
            if st.button("Check Status"):
                try:
                    status = requests.get(
                        f"http://airflow-webserver:8080/api/v1/dags/{dag_id}/dagRuns/{st.session_state.dag_run_id}",
                        auth=("admin", "admin")
                    ).json()
                    st.metric("Status", status.get("state", "Running"))
                except Exception as e:
                    st.error("Could not fetch status.")

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
import streamlit as st
import pandas as pd
import os
import requests
import time
import json
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
.lineage-card {
    background-color: #1e1e1e;
    border: 1px solid #333;
    border-radius: 8px;
    padding: 20px;
    margin-bottom: 10px;
    box-shadow: 0 4px 6px rgba(0,0,0,0.3);
    text-align: center;
    transition: transform 0.2s;
}
.lineage-card:hover {
    transform: translateY(-2px);
}
.arrow-down {
    text-align: center;
    font-size: 24px;
    color: #666;
    margin: 10px 0;
}
.status-badge {
    padding: 4px 8px;
    border-radius: 12px;
    font-size: 0.8em;
    font-weight: bold;
}
.status-success { background-color: #28a745; color: white; }
.status-failed { background-color: #dc3545; color: white; }
.status-pending { background-color: #ffc107; color: black; }
</style>
""", unsafe_allow_html=True)

# Paths
DATA_DIR = "/app/data"
BRONZE_PATH = os.path.join(DATA_DIR, "delta", "bronze")
SILVER_PATH = os.path.join(DATA_DIR, "delta", "silver")
GOLD_PATH = os.path.join(DATA_DIR, "delta", "gold")
INPUT_PATH = os.path.join(DATA_DIR, "input")
METADATA_DIR = "/app/metadata"

# Sidebar
with st.sidebar:
    st.header("Navigation")
    page = st.radio("", ["📊 Dashboard", "📁 Data Explorer", "📈 Metrics", "🔄 Pipeline Status", "🌐 Data Lineage"])
    
    st.divider()
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

if page == "📊 Dashboard":
    st.markdown('<h1 class="main-header">🚀 Data Pipeline Dashboard</h1>', unsafe_allow_html=True)
    
    # Airflow Status
    if 'dag_run_id' in st.session_state:
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Latest DAG Run", st.session_state.dag_run_id)
        with col2:
            if st.button("Check Status"):
                try:
                    status = requests.get(
                        f"http://airflow-webserver:8080/api/v1/dags/{dag_id}/dagRuns/{st.session_state.dag_run_id}",
                        auth=("admin", "admin")
                    ).json()
                    st.metric("Status", status.get("state", "Running"))
                except Exception as e:
                    st.error("Could not fetch status.")

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

elif page == "🔄 Pipeline Status":
    st.markdown('<h1 class="main-header">🔄 Pipeline Status</h1>', unsafe_allow_html=True)
    
    def load_metadata(layer):
        try:
            path = os.path.join(METADATA_DIR, f"{layer}.json")
            if os.path.exists(path):
                with open(path, "r") as f:
                    return json.load(f)
        except Exception:
            pass
        return None

    m_bronze = load_metadata("bronze")
    m_silver = load_metadata("silver")
    m_gold = load_metadata("gold")
    
    st.header("Visual Pipeline Flow")
    
    # Custom CSS for the user's requested visual flow
    st.markdown("""
    <style>
    .flow-container {
        display: flex;
        flex-direction: column;
        align-items: center;
        font-family: monospace;
        background-color: #1e1e1e;
        padding: 20px;
        border-radius: 10px;
        color: #d4d4d4;
    }
    .flow-box {
        border: 1px solid #d4d4d4;
        padding: 10px 20px;
        text-align: left;
        border-radius: 5px;
        margin: 5px 0;
        width: 300px;
        background-color: #252526;
    }
    .flow-arrow {
        text-align: center;
        margin: 5px 0;
    }
    .flow-text {
        color: #9cdcfe;
        text-align: center;
        font-size: 0.9em;
        margin: 5px 0;
    }
    </style>
    """, unsafe_allow_html=True)
    
    b_stat = m_bronze.get('status', 'Pending') if m_bronze else 'Pending'
    b_rows = m_bronze.get('output_rows', 0) if m_bronze else 0
    b_time = m_bronze.get('duration', 'N/A') if m_bronze else 'N/A'
    source = m_bronze.get('source', 'input_data.csv') if m_bronze else 'input_data.csv'
    
    s_stat = m_silver.get('status', 'Pending') if m_silver else 'Pending'
    s_rows = m_silver.get('rows_after', 0) if m_silver else 0
    s_time = m_silver.get('duration', 'N/A') if m_silver else 'N/A'
    s_dup = m_silver.get('duplicates_removed', 0) if m_silver else 0
    s_null = m_silver.get('nulls_filled', 0) if m_silver else 0
    
    g_stat = m_gold.get('status', 'Pending') if m_gold else 'Pending'
    g_time = m_gold.get('duration', 'N/A') if m_gold else 'N/A'
    
    # Gold doesn't explicitly track output rows right now, use silver's output or just 'N/A'
    g_rows = s_rows if g_stat == 'Success' else 0
    
    flow_html = f"""
    <div class="flow-container">
        <div>{source}</div>
        <div class="flow-arrow">│<br/>▼</div>
        <div class="flow-box">
            <b>Bronze Layer</b><br/>
            Status: {b_stat}<br/>
            Rows: {b_rows}<br/>
            Time: {b_time}
        </div>
        <div class="flow-arrow">│</div>
        <div class="flow-text">Removed {s_dup} duplicate(s)<br/>Filled {s_null} null value(s)</div>
        <div class="flow-arrow">│<br/>▼</div>
        <div class="flow-box">
            <b>Silver Layer</b><br/>
            Status: {s_stat}<br/>
            Rows: {s_rows}<br/>
            Time: {s_time}
        </div>
        <div class="flow-arrow">│</div>
        <div class="flow-text">Generated KPIs</div>
        <div class="flow-arrow">│<br/>▼</div>
        <div class="flow-box">
            <b>Gold Layer</b><br/>
            Status: {g_stat}<br/>
            Rows: {g_rows}<br/>
            Time: {g_time}
        </div>
    </div>
    """
    st.markdown(flow_html, unsafe_allow_html=True)
    
    st.divider()
    st.subheader("Pipeline Run History")
    history_dir = os.path.join(METADATA_DIR, "history")
    if os.path.exists(history_dir):
        files = sorted(os.listdir(history_dir), reverse=True)
        history_records = []
        for f in files:
            try:
                with open(os.path.join(history_dir, f), "r") as file:
                    data = json.load(file)
                    history_records.append({
                        "Time": data.get("start_time"),
                        "Layer": data.get("layer"),
                        "Status": data.get("status"),
                        "Duration": data.get("duration")
                    })
            except:
                pass
        if history_records:
            st.dataframe(pd.DataFrame(history_records), use_container_width=True)
        else:
            st.info("No run history available.")
    else:
        st.info("No run history available.")

elif page == "🌐 Data Lineage":
    st.markdown('<h1 class="main-header">🌐 Data Lineage (Metadata JSONs)</h1>', unsafe_allow_html=True)
    st.write("Below are the raw metadata JSON files generated by the pipeline layers.")
    
    def read_json_file(layer):
        path = os.path.join(METADATA_DIR, f"{layer}.json")
        if os.path.exists(path):
            with open(path, "r") as f:
                return f.read()
        return None

    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.subheader("Bronze Metadata")
        b_data = read_json_file("bronze")
        if b_data:
            st.code(b_data, language="json")
        else:
            st.info("bronze.json not found")
            
    with col2:
        st.subheader("Silver Metadata")
        s_data = read_json_file("silver")
        if s_data:
            st.code(s_data, language="json")
        else:
            st.info("silver.json not found")
            
    with col3:
        st.subheader("Gold Metadata")
        g_data = read_json_file("gold")
        if g_data:
            st.code(g_data, language="json")
        else:
            st.info("gold.json not found")       
else:
    st.info("This section is under construction. Please use the Dashboard or Data Lineage pages.")

# Auto-refresh
time.sleep(2)
st.rerun()
