import json
import os
import shutil

import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st


# Detect if we are running inside Docker
IS_DOCKER = os.environ.get("AIRFLOW_URL") is not None

if IS_DOCKER:
    DATA_DIR = "/app/data"
    BRONZE_PATH = os.path.join(DATA_DIR, "delta", "bronze")
    SILVER_PATH = os.path.join(DATA_DIR, "delta", "silver")
    GOLD_PATH = os.path.join(DATA_DIR, "delta", "gold")
    INPUT_PATH = os.path.join(DATA_DIR, "input")
    METRICS_DIR = os.path.join(DATA_DIR, "delta", "metrics")
    METADATA_DIR = "/app/metadata"
else:
    # Running locally on host machine
    BASE_DIR = os.path.abspath(os.path.dirname(__file__))
    PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, ".."))
    
    DATA_DIR = os.path.join(PROJECT_ROOT, "delta_lake")
    BRONZE_PATH = os.path.join(DATA_DIR, "bronze")
    SILVER_PATH = os.path.join(DATA_DIR, "silver")
    GOLD_PATH = os.path.join(DATA_DIR, "gold")
    INPUT_PATH = os.path.join(PROJECT_ROOT, "input_data")
    METRICS_DIR = os.path.join(DATA_DIR, "metrics")
    METADATA_DIR = os.path.join(PROJECT_ROOT, "metadata")

QUEUE_FILE_PATH = os.path.join(INPUT_PATH, "upload_queue.json")

AIRFLOW_URL = os.getenv("AIRFLOW_URL", "http://airflow-webserver:8080")
AIRFLOW_USERNAME = "admin"
AIRFLOW_PASSWORD = "admin"

DAG_OPTIONS = {
    "Event-Driven Pipeline": {
        "dag_id": "event_driven_data_pipeline",
        "mode": "instant",
        "description": "Triggers immediately after upload.",
    },
    "Scheduled Pipeline": {
        "dag_id": "scheduled_data_pipeline",
        "mode": "queue",
        "description": "Adds the file to the queue for the next scheduled run.",
    },
}

LAYER_CONFIG = {
    "Bronze": {
        "path": BRONZE_PATH,
        "class_name": "bronze",
        "accent": "#C8A97E",
        "icon": "database",
        "description": "Raw ingestion records",
    },
    "Silver": {
        "path": SILVER_PATH,
        "class_name": "silver",
        "accent": "#E6D5B8",
        "icon": "filter",
        "description": "Validated and cleaned records",
    },
    "Gold": {
        "path": GOLD_PATH,
        "class_name": "gold",
        "accent": "#D9C27A",
        "icon": "award",
        "description": "Analytics-ready curated records",
    },
}

STATUS_COLORS = {
    "success": "#BFA37A",
    "running": "#C8A97E",
    "queued": "#D9C27A",
    "failed": "#9A7B5F",
    "warning": "#C8A97E",
}

st.set_page_config(
    page_title="MedallionFlow Platform",
    page_icon=":material/monitoring:",
    layout="wide",
    initial_sidebar_state="expanded",
)


def icon_svg(name: str, size: int = 18, stroke: str = "currentColor") -> str:
    icons = {
        "dashboard": '<path d="M3 13h8V3H3Zm10 8h8V11h-8Zm0-18v6h8V3ZM3 21h8v-6H3Z"/>',
        "database": '<ellipse cx="12" cy="5" rx="7" ry="3"/><path d="M5 5v6c0 1.7 3.1 3 7 3s7-1.3 7-3V5"/><path d="M5 11v6c0 1.7 3.1 3 7 3s7-1.3 7-3v-6"/>',
        "filter": '<path d="M4 5h16l-6 7v5l-4 2v-7Z"/>',
        "award": '<circle cx="12" cy="8" r="4"/><path d="M8 12v7l4-2 4 2v-7"/>',
        "upload": '<path d="M12 16V4"/><path d="m7 9 5-5 5 5"/><path d="M5 20h14"/>',
        "activity": '<path d="M3 12h4l2-5 4 10 2-5h6"/>',
        "refresh": '<path d="M20 11a8 8 0 1 0 2 5.3"/><path d="M20 4v7h-7"/>',
        "trash": '<path d="M4 7h16"/><path d="M10 11v6"/><path d="M14 11v6"/><path d="M6 7l1 13h10l1-13"/><path d="M9 7V4h6v3"/>',
        "check": '<path d="m5 13 4 4L19 7"/>',
        "warning": '<path d="M12 4 3 20h18Z"/><path d="M12 9v4"/><path d="M12 17h.01"/>',
        "play": '<path d="m8 6 10 6-10 6Z"/>',
        "stop": '<path d="M7 7h10v10H7z"/>',
        "clock": '<circle cx="12" cy="12" r="9"/><path d="M12 7v6l4 2"/>',
        "table": '<path d="M4 5h16v14H4z"/><path d="M4 10h16"/><path d="M9 5v14"/><path d="M15 5v14"/>',
        "folder": '<path d="M3 6h6l2 2h10v10a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2Z"/><path d="M3 6V5a2 2 0 0 1 2-2h4l2 2h8a2 2 0 0 1 2 2v1"/>',
        "sparkles": '<path d="m12 3 1.5 4.5L18 9l-4.5 1.5L12 15l-1.5-4.5L6 9l4.5-1.5Z"/><path d="m5 16 .8 2.2L8 19l-2.2.8L5 22l-.8-2.2L2 19l2.2-.8Z"/><path d="m19 15 .6 1.6L21 17l-1.4.4L19 19l-.6-1.6L17 17l1.4-.4Z"/>',
        "open": '<path d="M14 4h6v6"/><path d="M10 14 20 4"/><path d="M20 14v5a1 1 0 0 1-1 1H5a1 1 0 0 1-1-1V5a1 1 0 0 1 1-1h5"/>',
    }
    path = icons.get(name, icons["dashboard"])
    return (
        f'<svg viewBox="0 0 24 24" width="{size}" height="{size}" '
        f'fill="none" stroke="{stroke}" stroke-width="1.8" '
        f'stroke-linecap="round" stroke-linejoin="round" '
        f'xmlns="http://www.w3.org/2000/svg">{path}</svg>'
    )


st.markdown(
    """
    <style>
    :root {
        --bg-gradient: linear-gradient(135deg, #1f1c2c 0%, #0e153a 100%);
        --panel: rgba(255, 255, 255, 0.1);
        --panel-soft: rgba(255, 255, 255, 0.05);
        --text: #ffffff;
        --muted: #a0a5b5;
        --line: rgba(255, 255, 255, 0.15);
        --shadow: 0 8px 32px rgba(0, 0, 0, 0.25);
        --brand: rgba(255, 255, 255, 0.15);
        --brand-deep: rgba(255, 255, 255, 0.25);
        --good: rgba(217, 194, 122, 0.3);
        --warn: rgba(200, 169, 126, 0.3);
        --bad: rgba(154, 123, 95, 0.3);
    }

    /* Soft Gradient Background with Blobs */
    .stApp {
        background: var(--bg-gradient);
        background-attachment: fixed;
    }
    
    .stApp::before {
        content: '';
        position: fixed;
        top: -10%; left: -10%;
        width: 60vw; height: 60vw;
        border-radius: 50%;
        background: radial-gradient(circle, rgba(138, 43, 226, 0.15) 0%, transparent 60%);
        filter: blur(100px);
        z-index: -1;
    }
    .stApp::after {
        content: '';
        position: fixed;
        bottom: -10%; right: -10%;
        width: 60vw; height: 60vw;
        border-radius: 50%;
        background: radial-gradient(circle, rgba(0, 191, 255, 0.15) 0%, transparent 60%);
        filter: blur(100px);
        z-index: -1;
    }

    /* Sidebar Glass */
    [data-testid="stSidebar"] {
        background: rgba(20, 20, 30, 0.4) !important;
        backdrop-filter: blur(24px) !important;
        -webkit-backdrop-filter: blur(24px) !important;
        border-right: 1px solid var(--line) !important;
        box-shadow: 2px 0 32px rgba(0,0,0,0.3);
    }

    [data-testid="stSidebar"] * {
        color: var(--text);
    }

    /* Nav items */
    [data-testid="stSidebar"] [role="radiogroup"] > label {
        background: transparent;
        border: 1px solid transparent;
        border-radius: 16px;
        padding: 0.6rem 0.8rem;
        margin-bottom: 0.4rem;
        transition: all 0.3s ease;
    }

    [data-testid="stSidebar"] [role="radiogroup"] > label:hover {
        background: rgba(255, 255, 255, 0.1);
        border: 1px solid rgba(255, 255, 255, 0.15);
    }

    [data-testid="stSidebar"] [role="radiogroup"] > label[data-baseweb="radio"] input:checked + div {
        font-weight: 700;
        letter-spacing: 0.5px;
    }

    /* Active Nav Item Glow */
    [data-testid="stSidebar"] [role="radiogroup"] > label:has(input:checked) {
        background: rgba(255, 255, 255, 0.15);
        backdrop-filter: blur(12px);
        -webkit-backdrop-filter: blur(12px);
        border-color: rgba(255, 255, 255, 0.3);
        box-shadow: 0 4px 20px rgba(255, 255, 255, 0.1);
    }

    /* Hero */
    .hero {
        background: rgba(255, 255, 255, 0.1);
        backdrop-filter: blur(24px);
        -webkit-backdrop-filter: blur(24px);
        border: 1px solid rgba(255, 255, 255, 0.2);
        border-radius: 24px;
        padding: 28px 30px;
        color: var(--text);
        box-shadow: var(--shadow);
        margin-bottom: 1.5rem;
    }

    .hero-top, .section-title, .side-heading, .mini-inline {
        display: flex;
        align-items: center;
        gap: 0.75rem;
    }
    
    /* Icon Containers */
    .hero-top svg, .section-title svg, .side-heading svg, .metric-title-row svg {
        background: rgba(255, 255, 255, 0.15);
        border-radius: 50%;
        padding: 6px;
        box-sizing: content-box;
        backdrop-filter: blur(8px);
        -webkit-backdrop-filter: blur(8px);
    }

    .hero-kicker, .section-kicker, .panel-label, .metric-label, .status-meta, .upload-note, .footer-note, .detail-label, .run-id, .run-subtitle {
        color: var(--muted);
        font-size: 0.95rem;
        letter-spacing: 0.05em;
        text-transform: uppercase;
        font-weight: 600;
    }

    .hero .hero-kicker, .hero .hero-copy {
        color: var(--text);
    }

    .hero h1, .section-title h3, .side-heading h3 {
        margin: 0;
        color: var(--text);
        font-weight: 800;
    }

    .hero h1 {
        font-size: 2.35rem;
        line-height: 1.1;
        margin-top: 0.4rem;
        margin-bottom: 0.6rem;
    }

    .hero-copy {
        max-width: 760px;
        font-size: 1rem;
        line-height: 1.6;
    }

    .hero-badge, .status-badge, .file-chip, .run-file {
        display: inline-flex;
        align-items: center;
        gap: 0.45rem;
        border-radius: 999px;
        padding: 0.35rem 0.8rem;
        font-size: 0.85rem;
        font-weight: 600;
        background: rgba(255, 255, 255, 0.12);
        backdrop-filter: blur(12px);
        -webkit-backdrop-filter: blur(12px);
        color: var(--text);
        border: 1px solid rgba(255, 255, 255, 0.25);
        box-shadow: 0 4px 12px rgba(0,0,0,0.1);
    }

    .section-shell, .panel-card, .upload-card, .status-card, .run-card, .detail-card, .dag-option, div[data-testid="stMetric"], .side-panel {
        background: var(--panel);
        backdrop-filter: blur(16px);
        -webkit-backdrop-filter: blur(16px);
        border: 1px solid var(--line);
        border-radius: 20px;
        box-shadow: var(--shadow);
    }

    .section-shell {
        padding: 1.2rem 1.3rem;
        margin-bottom: 1rem;
    }

    /* Metric Tiles (Bronze, Silver, Gold) */
    .metric-tile {
        border-radius: 22px;
        padding: 1.15rem;
        color: var(--text);
        box-shadow: var(--shadow);
        min-height: 168px;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        margin-bottom: 1rem;
        backdrop-filter: blur(20px);
        -webkit-backdrop-filter: blur(20px);
        border: 1px solid rgba(255, 255, 255, 0.25);
    }

    .metric-tile.bronze { background: linear-gradient(145deg, rgba(200, 169, 126, 0.3) 0%, rgba(184, 148, 109, 0.05) 100%); }
    .metric-tile.silver { background: linear-gradient(145deg, rgba(230, 213, 184, 0.3) 0%, rgba(217, 200, 168, 0.05) 100%); }
    .metric-tile.gold { background: linear-gradient(145deg, rgba(217, 194, 122, 0.3) 0%, rgba(200, 177, 104, 0.05) 100%); }

    .metric-title-row {
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        gap: 0.8rem;
    }

    .metric-value {
        font-size: 2.1rem;
        font-weight: 800;
        line-height: 1;
        margin-top: 0.5rem;
    }

    .metric-desc {
        color: var(--text);
        font-size: 0.9rem;
        line-height: 1.45;
        font-weight: 500;
        opacity: 0.9;
    }

    .panel-card, .upload-card, .status-card, .run-card {
        padding: 1rem 1.1rem;
        margin-bottom: 1rem;
    }

    .upload-card {
        background: rgba(255, 255, 255, 0.05);
        border-style: dashed;
        border-width: 2px;
        border-color: rgba(255, 255, 255, 0.3);
    }

    .status-badge {
        color: #ffffff;
        text-transform: capitalize;
        justify-content: center;
        min-width: 126px;
        text-shadow: 0 1px 2px rgba(0,0,0,0.3);
        border: none;
    }

    .side-heading {
        margin-bottom: 0.8rem;
    }

    .side-panel {
        padding: 0.9rem;
        margin-top: 0.8rem;
    }

    .file-chip {
        margin: 0 0.45rem 0.45rem 0;
    }

    .footer-note {
        text-align: center;
        padding: 0.5rem 0 0.25rem 0;
    }

    .run-title {
        display: flex;
        align-items: center;
        gap: 0.7rem;
        font-size: 1.15rem;
        font-weight: 800;
        color: var(--text);
        margin-bottom: 0.3rem;
    }

    .run-file {
        margin-top: 0.7rem;
    }

    .detail-card, .dag-option, div[data-testid="stMetric"] {
        padding: 1rem;
        margin-bottom: 1rem;
    }

    .detail-value {
        color: var(--text);
        font-size: 1.05rem;
        font-weight: 700;
        line-height: 1.45;
        word-break: break-word;
    }

    /* Glass Buttons */
    .stButton > button, .stDownloadButton > button, .stLinkButton > a {
        border-radius: 20px !important;
        min-height: 2.9rem;
        font-weight: 700 !important;
        background: rgba(255, 255, 255, 0.15) !important;
        backdrop-filter: blur(16px) !important;
        -webkit-backdrop-filter: blur(16px) !important;
        color: var(--text) !important;
        border: 1px solid rgba(255, 255, 255, 0.3) !important;
        box-shadow: 0 4px 16px rgba(0,0,0,0.1) !important;
        transition: all 0.3s ease !important;
    }

    .stButton > button:hover, .stDownloadButton > button:hover, .stLinkButton > a:hover {
        background: rgba(255, 255, 255, 0.25) !important;
        backdrop-filter: blur(20px) !important;
        -webkit-backdrop-filter: blur(20px) !important;
        border-color: rgba(255, 255, 255, 0.5) !important;
        box-shadow: 0 8px 24px rgba(255, 255, 255, 0.15) !important;
        color: #ffffff !important;
    }

    /* Selectors & Dropdowns */
    div[data-baseweb="select"] > div {
        background: rgba(255, 255, 255, 0.1) !important;
        backdrop-filter: blur(16px) !important;
        -webkit-backdrop-filter: blur(16px) !important;
        color: var(--text) !important;
        border: 1px solid var(--line) !important;
        border-radius: 16px !important;
    }

    div[data-baseweb="select"] input { color: var(--text) !important; }
    div[data-baseweb="select"] svg { fill: var(--text) !important; }

    div[data-baseweb="popover"], div[role="listbox"], ul[data-testid="stSelectboxVirtualDropdown"] {
        background: rgba(30, 30, 45, 0.8) !important;
        backdrop-filter: blur(24px) !important;
        -webkit-backdrop-filter: blur(24px) !important;
        border: 1px solid var(--line) !important;
        border-radius: 16px !important;
        box-shadow: var(--shadow) !important;
    }

    div[data-baseweb="popover"] *, ul[data-testid="stSelectboxVirtualDropdown"] li {
        color: var(--text) !important;
        background: transparent !important;
    }

    div[role="option"]:hover {
        background: rgba(255, 255, 255, 0.15) !important;
    }

    /* Typography Overrides */
    p, label, .stCaption, .stMarkdown, .stText, span, div {
        color: var(--text);
    }
    
    h1, h2, h3, h4, h5, h6 {
        color: var(--text) !important;
        font-weight: 800 !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def section_header(title: str, subtitle: str, icon: str) -> None:
    st.markdown(
        f"""
        <div class="section-shell">
            <div class="section-title">
                <div>{icon_svg(icon, size=20)}</div>
                <div>
                    <div class="section-kicker">{subtitle}</div>
                    <h3>{title}</h3>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_metric_tile(layer_name: str, count: int) -> None:
    config = LAYER_CONFIG[layer_name]
    st.markdown(
        f"""
        <div class="metric-tile {config["class_name"]}">
            <div class="metric-title-row">
                <div>
                    <div class="metric-label">{layer_name} Layer</div>
                    <div class="metric-value">{count:,}</div>
                </div>
                <div>{icon_svg(config["icon"], size=22, stroke="white")}</div>
            </div>
            <div class="metric-desc">{config["description"]}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_status_badge(state: str) -> str:
    normalized = (state or "unknown").lower()
    color = STATUS_COLORS.get(normalized, "#475569")
    return (
        f'<span class="status-badge" style="background:{color};">'
        f"{icon_svg('activity', size=14, stroke='white')}{normalized}</span>"
    )


def format_datetime(value: str, include_tz: bool = True) -> str:
    if not value:
        return "Not available"

    try:
        timestamp = pd.to_datetime(value, utc=True).tz_convert("Asia/Kolkata")
        pattern = "%d %b %Y, %I:%M:%S %p IST" if include_tz else "%d %b %Y, %I:%M:%S %p"
        return timestamp.strftime(pattern)
    except Exception:
        try:
            timestamp = pd.to_datetime(value)
            return timestamp.strftime("%d %b %Y, %I:%M:%S %p")
        except Exception:
            return str(value)


def friendly_run_name(run: dict, position: int) -> str:
    run_id = run.get("dag_run_id", "") or ""
    if run_id.startswith("manual__"):
        return f"Manual Run {position + 1}"
    if run_id.startswith("scheduled__"):
        return f"Scheduled Run {position + 1}"
    return f"Pipeline Run {position + 1}"


def render_detail_card(label: str, value: str) -> None:
    st.markdown(
        f"""
        <div class="detail-card">
            <div class="detail-label">{label}</div>
            <div class="detail-value">{value}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def load_upload_queue() -> list:
    try:
        if os.path.exists(QUEUE_FILE_PATH):
            with open(QUEUE_FILE_PATH, "r", encoding="utf-8") as file_handle:
                data = json.load(file_handle)
                if isinstance(data, list):
                    return data
    except Exception:
        return []
    return []


def save_upload_queue(entries: list) -> None:
    os.makedirs(INPUT_PATH, exist_ok=True)
    with open(QUEUE_FILE_PATH, "w", encoding="utf-8") as file_handle:
        json.dump(entries, file_handle, indent=2)


def enqueue_scheduled_files(file_names: list[str]) -> None:
    queue_entries = load_upload_queue()
    timestamp = pd.Timestamp.now(tz="Asia/Kolkata").isoformat()
    for file_name in file_names:
        queue_entries.append(
            {
                "file_name": file_name,
                "status": "queued",
                "target_dag": DAG_OPTIONS["Scheduled Pipeline"]["dag_id"],
                "queued_at": timestamp,
                "reserved_run_id": None,
                "processed_run_id": None,
            }
        )
    save_upload_queue(queue_entries)


def find_file_for_run(dag_id: str, run_id: str, run_conf: dict | None = None) -> str:
    if run_conf and run_conf.get("file_name"):
        return run_conf["file_name"]

    queue_entries = load_upload_queue()
    for entry in reversed(queue_entries):
        if entry.get("processed_run_id") == run_id or entry.get("reserved_run_id") == run_id:
            return entry.get("file_name", "Not available")

    if dag_id == DAG_OPTIONS["Scheduled Pipeline"]["dag_id"]:
        return "Queued file"
    return "Not available"


@st.cache_data(ttl=300)
def read_delta_table(path: str) -> pd.DataFrame:
    try:
        parquet_files = []
        for root, _, files in os.walk(path):
            for file_name in files:
                if file_name.endswith(".parquet"):
                    parquet_files.append(os.path.join(root, file_name))

        if not parquet_files:
            return pd.DataFrame()

        return pd.concat([pd.read_parquet(file_path) for file_path in parquet_files], ignore_index=True)
    except Exception:
        return pd.DataFrame()


def get_table_info(path: str) -> pd.DataFrame:
    if os.path.exists(path):
        return read_delta_table(path)
    return pd.DataFrame()


def get_layer_metrics(layer_name: str) -> dict:
    try:
        path = os.path.join(METRICS_DIR, f"{layer_name.lower()}_metrics.json")
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as file_handle:
                return json.load(file_handle)
    except Exception:
        return {}
    return {}


def get_last_updated(df: pd.DataFrame) -> str:
    if df.empty or "_ingest_timestamp" not in df.columns:
        return "Not available"
    try:
        timestamp = pd.to_datetime(df["_ingest_timestamp"]).max()
        if pd.isna(timestamp):
            return "Not available"
        return format_datetime(str(timestamp), include_tz=False)
    except Exception:
        return "Not available"


def trigger_airflow(dag_id: str, file_name: str) -> bool:
    try:
        response = requests.post(
            f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns",
            auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD),
            headers={"Content-Type": "application/json"},
            json={"conf": {"file_name": file_name}},
            timeout=10,
        )
        if response.status_code in [200, 201]:
            return True

        st.error(f"Airflow API error {response.status_code}")
        st.code(response.text)
        return False
    except Exception as exc:
        st.error(f"Airflow trigger failed: {exc}")
        return False


def stop_pipeline(dag_id: str, run_id: str) -> bool:
    try:
        response = requests.patch(
            f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns/{run_id}",
            auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD),
            headers={"Content-Type": "application/json"},
            json={"state": "failed"},
            timeout=10,
        )
        if response.status_code == 200:
            return True

        st.error(f"Failed to stop pipeline: {response.text}")
        return False
    except Exception as exc:
        st.error(f"Error stopping pipeline: {exc}")
        return False


def get_dag_runs(dag_id: str, limit: int = 10) -> list:
    try:
        response = requests.get(
            f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns?limit={limit}",
            auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        if response.status_code == 200:
            return response.json().get("dag_runs", [])

        st.error(f"Airflow API error {response.status_code}")
        return []
    except Exception:
        st.error("Unable to fetch pipeline status")
        return []


def get_dag_run_details(dag_id: str, run_id: str) -> dict:
    try:
        response = requests.get(
            f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns/{run_id}",
            auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        if response.status_code == 200:
            return response.json()
    except Exception:
        return {}
    return {}


bronze_df = get_table_info(BRONZE_PATH)
silver_df = get_table_info(SILVER_PATH)
gold_df = get_table_info(GOLD_PATH)

layer_data = {
    "Bronze": bronze_df,
    "Silver": silver_df,
    "Gold": gold_df,
}

total_records = sum(len(df) for df in layer_data.values())
active_layers = sum(1 for df in layer_data.values() if not df.empty)

st.markdown(
    f"""
    <div class="hero">
        <div class="hero-top">
            <span class="hero-badge">{icon_svg('sparkles', size=15, stroke='white')}MedallionFlow</span>
            <span class="hero-badge">{icon_svg('activity', size=15, stroke='white')}Pipeline Visibility</span>
        </div>
        <h1>MedallionFlow Platform</h1>
        <div class="hero-copy">
            Monitor medallion layers, upload source files, and trigger Airflow runs from one streamlined interface.
            This version removes emoji-based UI labels and uses cleaner visual sections with icon-driven cues.
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

with st.sidebar:
    st.markdown(
        f"""
        <div class="side-heading">
            <div>{icon_svg('dashboard', size=20, stroke='white')}</div>
            <div>
                <div class="hero-kicker">Control Center</div>
                <h3>MedallionFlow Console</h3>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    page = st.radio(
        "Navigate",
        ["Overview", "Data Layers", "Upload Data", "Pipeline Status", "Data Lineage"],
        label_visibility="collapsed",
    )

    st.markdown(
        f"""
        <div class="side-panel">
            <div class="mini-inline">
                <div>{icon_svg('refresh', size=16, stroke='white')}</div>
                <strong>Quick Snapshot</strong>
            </div>
            <div class="upload-note">Layers with data: {active_layers}/3</div>
            <div class="upload-note">Tracked records: {total_records:,}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        f"""
        <div class="side-panel">
            <div class="mini-inline">
                <div>{icon_svg('trash', size=16, stroke='white')}</div>
                <strong>System Controls</strong>
            </div>
            <div class="upload-note">Reset Delta layers and uploaded input files.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if st.button("Reset All Data", use_container_width=True):
        try:
            for path in [BRONZE_PATH, SILVER_PATH, GOLD_PATH, METRICS_DIR]:
                if os.path.exists(path):
                    shutil.rmtree(path)

            if os.path.exists(INPUT_PATH):
                for item in os.listdir(INPUT_PATH):
                    item_path = os.path.join(INPUT_PATH, item)
                    if os.path.isfile(item_path):
                        os.unlink(item_path)

            st.cache_data.clear()
            st.success("System data wiped successfully.")
            st.rerun()
        except Exception as exc:
            st.error(f"Error resetting data: {exc}")


if page == "Overview":
    section_header("Pipeline Overview", "Layer health at a glance", "activity")

    col1, col2, col3 = st.columns(3, gap="large")
    with col1:
        render_metric_tile("Bronze", len(bronze_df))
    with col2:
        render_metric_tile("Silver", len(silver_df))
    with col3:
        render_metric_tile("Gold", len(gold_df))

    info_col1, info_col2, info_col3 = st.columns(3, gap="large")
    for column, layer_name in zip([info_col1, info_col2, info_col3], layer_data.keys()):
        config = LAYER_CONFIG[layer_name]
        df = layer_data[layer_name]
        with column:
            st.markdown(
                f"""
                <div class="panel-card">
                    <div class="mini-inline">
                        <div>{icon_svg(config["icon"], size=18, stroke=config["accent"])}</div>
                        <strong>{layer_name} Snapshot</strong>
                    </div>
                    <div class="panel-label">Last updated: {get_last_updated(df)}</div>
                    <div class="panel-label">Columns: {len(df.columns) if not df.empty else 0}</div>
                    <div class="panel-label">Rows: {len(df):,}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    section_header("Pipeline Flow", "Record movement across the medallion architecture", "dashboard")

    flow_fig = go.Figure()
    flow_fig.add_trace(
        go.Scatter(
            x=list(layer_data.keys()),
            y=[len(bronze_df), len(silver_df), len(gold_df)],
            mode="lines+markers+text",
            text=[f"{len(bronze_df):,}", f"{len(silver_df):,}", f"{len(gold_df):,}"],
            textposition="top center",
            line=dict(color="#BFA37A", width=4),
            marker=dict(
                size=16,
                color=["#9a6b37", "#5f6f86", "#c7921d"],
                line=dict(color="white", width=2),
            ),
            hovertemplate="%{x}: %{y:,} records<extra></extra>",
        )
    )
    flow_fig.update_layout(
        height=360,
        margin=dict(l=20, r=20, t=20, b=20),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(255,255,255,0.92)",
        xaxis=dict(title="", showgrid=False),
        yaxis=dict(title="Records", gridcolor="#dbe4f0", zeroline=False),
    )
    st.plotly_chart(flow_fig, use_container_width=True)

elif page == "Data Layers":
    section_header("Explore Data Layers", "Inspect metrics, schema hints, and sample data", "table")

    layer = st.selectbox("Select Layer", ["Bronze", "Silver", "Gold"])
    df = layer_data[layer]
    metrics = get_layer_metrics(layer)
    config = LAYER_CONFIG[layer]

    summary_left, summary_right = st.columns([1.2, 2.2])
    with summary_left:
        st.markdown(
            f"""
            <div class="panel-card">
                <div class="mini-inline">
                    <div>{icon_svg(config["icon"], size=18, stroke=config["accent"])}</div>
                    <strong>{layer} Layer</strong>
                </div>
                <div class="panel-label">{config["description"]}</div>
                <div class="panel-label">Rows: {len(df):,}</div>
                <div class="panel-label">Columns: {len(df.columns) if not df.empty else 0}</div>
                <div class="panel-label">Last updated: {get_last_updated(df)}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with summary_right:
        if metrics:
            metric_columns = st.columns(len(metrics))
            for index, (key, value) in enumerate(metrics.items()):
                with metric_columns[index]:
                    st.metric(label=key.replace("_", " ").title(), value=value)
        else:
            st.metric("Tracked Records", f"{len(df):,}")

    if df.empty:
        st.warning("No data is available in this layer yet.")
    else:
        preview_col, schema_col = st.columns([2.6, 1.2])
        with preview_col:
            st.dataframe(df, use_container_width=True, height=420)
        with schema_col:
            st.markdown(
                f"""
                <div class="panel-card">
                    <div class="mini-inline">
                        <div>{icon_svg('table', size=18, stroke='#9A7B5F')}</div>
                        <strong>Columns</strong>
                    </div>
                """,
                unsafe_allow_html=True,
            )
            for column_name in df.columns[:12]:
                st.markdown(
                    f'<span class="file-chip">{column_name}</span>',
                    unsafe_allow_html=True,
                )
            if len(df.columns) > 12:
                st.caption(f"{len(df.columns) - 12} more columns not shown.")
            st.markdown("</div>", unsafe_allow_html=True)

elif page == "Upload Data":
    section_header("Upload Source Data", "Validate files before triggering a run", "upload")

    st.markdown(
        f"""
        <div class="upload-card">
            <div class="mini-inline">
                <div>{icon_svg('folder', size=19, stroke='#9A7B5F')}</div>
                <strong>Input Requirements</strong>
            </div>
            <div class="upload-note">Only CSV files are accepted. Choose whether the file should run immediately or wait for the scheduled queue.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    uploaded_files = st.file_uploader(
        "Choose CSV files",
        type="csv",
        accept_multiple_files=True,
    )

    dag_choice = st.selectbox("Choose Target DAG", list(DAG_OPTIONS.keys()))
    selected_dag = DAG_OPTIONS[dag_choice]
    st.markdown(
        f"""
        <div class="dag-option">
            <div class="mini-inline">
                <div>{icon_svg('activity', size=18, stroke='#BFA37A')}</div>
                <strong>{selected_dag["dag_id"]}</strong>
            </div>
            <div class="panel-label">{selected_dag["description"]}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if uploaded_files:
        valid_files = [file for file in uploaded_files if file.name.lower().endswith(".csv")]
        invalid_files = [file for file in uploaded_files if not file.name.lower().endswith(".csv")]

        if invalid_files:
            rejected = ", ".join(file.name for file in invalid_files)
            st.error(f"Invalid file type detected. Only CSV files are allowed: {rejected}")

        if valid_files:
            st.success(f"{len(valid_files)} valid CSV file(s) ready for upload.")
            
            # Debugging info
            st.info(f"Debug: IS_DOCKER = {IS_DOCKER}, Target Path = {INPUT_PATH}")
            
            st.markdown(
                "".join(
                    f'<span class="file-chip">{icon_svg("check", size=14, stroke="#1f8f5f")}{file.name}</span>'
                    for file in valid_files
                ),
                unsafe_allow_html=True,
            )

            running_runs = [run for run in get_dag_runs(selected_dag["dag_id"]) if run.get("state") == "running"]
            if selected_dag["mode"] == "instant" and running_runs:
                st.warning(
                    f"A pipeline run is already active ({running_runs[0]['dag_run_id']}). Wait for it to finish before starting another."
                )
            else:
                action_label = "Upload and Start Pipeline" if selected_dag["mode"] == "instant" else "Upload and Queue for Scheduled Run"
                if st.button(action_label, use_container_width=True):
                    try:
                        os.makedirs(INPUT_PATH, exist_ok=True)
    
                        for file in valid_files:
                            path = os.path.join(INPUT_PATH, file.name)
                            with open(path, "wb") as output_file:
                                output_file.write(file.getbuffer())
    
                        st.success(f"Valid CSV files uploaded successfully to {INPUT_PATH}.")
    
                        file_to_process = valid_files[0].name
                        if selected_dag["mode"] == "instant":
                            st.info(f"Triggering {selected_dag['dag_id']} for {file_to_process}.")
    
                            if trigger_airflow(selected_dag["dag_id"], file_to_process):
                                st.success("Pipeline started successfully.")
                                st.link_button("Open Airflow Dashboard", "http://localhost:8081", use_container_width=True)
                            else:
                                st.error("Failed to trigger the DAG run.")
                        else:
                            enqueue_scheduled_files([file.name for file in valid_files])
                            st.success("Files added to the scheduled queue.")
                            st.info("The scheduled DAG will pick these files up at its next scheduled run.")
                    except Exception as e:
                        st.error(f"Upload failed: {e}")

    queued_count = sum(
        1
        for entry in load_upload_queue()
        if entry.get("target_dag") == DAG_OPTIONS["Scheduled Pipeline"]["dag_id"]
        and entry.get("status") in {"queued", "reserved"}
    )
    st.caption(f"Scheduled queue depth: {queued_count}")

elif page == "Pipeline Status":
    section_header("Recent Pipeline Runs", "Track state, start time, and termination actions", "clock")

    dag_filter = st.selectbox("Choose DAG", ["All Pipelines", *DAG_OPTIONS.keys()])
    if dag_filter == "All Pipelines":
        runs = []
        for dag_config in DAG_OPTIONS.values():
            dag_id = dag_config["dag_id"]
            runs.extend((dag_id, run) for run in get_dag_runs(dag_id, limit=6))
        runs = sorted(runs, key=lambda item: item[1].get("start_date") or "", reverse=True)
    else:
        selected_dag_id = DAG_OPTIONS[dag_filter]["dag_id"]
        runs = [(selected_dag_id, run) for run in get_dag_runs(selected_dag_id, limit=10)]

    if not runs:
        st.info("No pipeline runs are available yet.")
    else:
        for index, (dag_id, run) in enumerate(runs):
            state = run.get("state", "unknown")
            dag_run_id = run.get("dag_run_id", "unknown-run")
            run_details = get_dag_run_details(dag_id, dag_run_id)
            display_name = friendly_run_name(run, index)
            formatted_start = format_datetime(run.get("start_date"))
            formatted_end = format_datetime(run.get("end_date"))
            file_name = find_file_for_run(dag_id, dag_run_id, run_details.get("conf") or run.get("conf"))
            title_left, title_right = st.columns([4, 1.4])
            with title_left:
                st.markdown(
                    f"""
                    <div class="run-card">
                        <div class="run-title">
                            <div>{icon_svg('activity', size=18, stroke='#BFA37A')}</div>
                            <span>{display_name}</span>
                        </div>
                        <div class="run-subtitle">Started: {formatted_start}</div>
                        <div class="run-id">DAG: {dag_id}</div>
                        <div class="run-id">Run ID: {dag_run_id}</div>
                        <div class="run-file">{icon_svg('folder', size=14, stroke='#9A7B5F')}File: {file_name}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
            with title_right:
                st.markdown(
                    f"""
                    <div style="padding-top: 0.6rem; text-align:right;">
                        {render_status_badge(state)}
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

            details_col1, details_col2, details_col3 = st.columns(3, gap="large")
            with details_col1:
                render_detail_card("Start Time", formatted_start)
            with details_col2:
                render_detail_card("End Time", formatted_end)
            with details_col3:
                render_detail_card("Triggered By", dag_id)

            if state == "running":
                if st.button("Stop Run", key=f"stop_{dag_id}_{dag_run_id}", use_container_width=True):
                    if stop_pipeline(dag_id, dag_run_id):
                        st.success(f"{display_name} stopped.")
                        st.rerun()

            st.markdown("<div style='height: 0.8rem;'></div>", unsafe_allow_html=True)

elif page == "Data Lineage":
    section_header("Data Lineage & Monitoring", "Trace your data from upload to dashboard", "database")
    
    metadata_base = METADATA_DIR
    runs = []
    if os.path.exists(metadata_base):
        for item in os.listdir(metadata_base):
            if item.startswith("run_"):
                runs.append(item)
    runs.sort(reverse=True)
    
    if not runs:
        st.info("No pipeline runs found. Trigger a pipeline to generate metadata.")
    else:
        st.markdown(
            """
            <style>
            .flow-box {
                text-align: center; 
                padding: 15px; 
                border-radius: 12px;
                background: #fff;
                box-shadow: 0 4px 6px rgba(0,0,0,0.1);
                border: 3px solid #ccc;
                width: 180px;
            }
            .flow-success { border-color: #28a745; }
            .flow-failed { border-color: #dc3545; }
            .flow-pending { border-color: #ffc107; }
            .flow-arrow {
                text-align: center;
                font-size: 24px;
                color: #888;
                margin: 10px 0;
            }
            </style>
            """, unsafe_allow_html=True
        )
        selected_run = st.selectbox("Select Pipeline Run", runs)
        run_dir = os.path.join(metadata_base, selected_run)
        
        def load_meta(layer):
            p = os.path.join(run_dir, f"{layer}.json")
            if os.path.exists(p):
                with open(p, "r") as f:
                    return json.load(f)
            return None
            
        bronze_meta = load_meta("bronze")
        silver_meta = load_meta("silver")
        gold_meta = load_meta("gold")
        
        def get_status(meta):
            if not meta: return "Pending"
            return meta.get("status", "Unknown")
            
        b_status = get_status(bronze_meta)
        s_status = get_status(silver_meta)
        g_status = get_status(gold_meta)
        
        def get_color_class(status):
            if status == "Success": return "flow-success"
            if status == "FAILED": return "flow-failed"
            return "flow-pending"
            
        st.subheader("Data Lineage Flow")
        
        st.markdown(f'''
        <div style="display:flex; flex-direction:column; align-items:center;">
            <div class="flow-box">
                <b>CSV Upload</b><br/>📁 Source
            </div>
            <div class="flow-arrow">⬇️</div>
            <div class="flow-box {get_color_class(b_status)}">
                <b>Bronze Layer</b><br/>{b_status}
            </div>
            <div class="flow-arrow">⬇️</div>
            <div class="flow-box {get_color_class(s_status)}">
                <b>Silver Layer</b><br/>{s_status}
            </div>
            <div class="flow-arrow">⬇️</div>
            <div class="flow-box {get_color_class(g_status)}">
                <b>Gold Layer</b><br/>{g_status}
            </div>
            <div class="flow-arrow">⬇️</div>
            <div class="flow-box">
                <b>Dashboard</b><br/>📊 Analytics
            </div>
        </div>
        ''', unsafe_allow_html=True)
        
        # Error Handling
        for meta in [bronze_meta, silver_meta, gold_meta]:
            if meta and meta.get("status") == "FAILED":
                st.error(f"❌ {meta.get('layer')} Layer FAILED: {meta.get('exception', 'Unknown Error')}")
                
        st.divider()
        
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Pipeline Timeline")
            timeline = []
            timeline.append(f"CSV Uploaded -> Run: {selected_run}")
            if bronze_meta:
                timeline.append(f"Bronze Started: {bronze_meta.get('start_time')}")
                timeline.append(f"Bronze Completed: {bronze_meta.get('end_time')}")
            if silver_meta:
                timeline.append(f"Silver Started: {silver_meta.get('start_time')}")
                timeline.append(f"Silver Completed: {silver_meta.get('end_time')}")
            if gold_meta:
                timeline.append(f"Gold Started: {gold_meta.get('start_time')}")
                timeline.append(f"Gold Completed: {gold_meta.get('end_time')}")
            
            for t in timeline:
                st.markdown(f"`{t}`")
                
        with col2:
            st.subheader("Data Quality Metrics")
            up_rows = bronze_meta.get("input_rows", 0) if bronze_meta else 0
            proc_rows = bronze_meta.get("output_rows", 0) if bronze_meta else 0
            dup_rem = silver_meta.get("duplicates_removed", 0) if silver_meta else 0
            null_fill = silver_meta.get("nulls_filled", 0) if silver_meta else 0
            final_rows = silver_meta.get("rows_after", 0) if silver_meta else 0
            
            st.markdown(f"- **Rows Uploaded:** {up_rows:,}")
            st.markdown(f"- **Rows Processed:** {proc_rows:,}")
            st.markdown(f"- **Duplicates Removed:** {dup_rem:,}")
            st.markdown(f"- **Null Values Filled:** {null_fill:,}")
            st.markdown(f"- **Final Rows:** {final_rows:,}")
            
        st.divider()
        st.subheader("Pipeline Summary")
        summary_data = []
        for meta in [bronze_meta, silver_meta, gold_meta]:
            if meta:
                summary_data.append({
                    "Layer": meta.get("layer"),
                    "Rows": meta.get("output_rows") or meta.get("rows_after") or meta.get("aggregation_count"),
                    "Time": meta.get("duration"),
                    "Status": meta.get("status")
                })
        if summary_data:
            st.table(pd.DataFrame(summary_data))
            
        st.divider()
        st.subheader("Metadata Explorer")
        tab1, tab2, tab3 = st.tabs(["Bronze Layer", "Silver Layer", "Gold Layer"])
        with tab1:
            if bronze_meta: st.json(bronze_meta)
            else: st.info("No Bronze metadata available.")
        with tab2:
            if silver_meta: st.json(silver_meta)
            else: st.info("No Silver metadata available.")
        with tab3:
            if gold_meta: st.json(gold_meta)
            else: st.info("No Gold metadata available.")

st.markdown("---")
st.markdown(
    f"""
    <div class="footer-note">
        {icon_svg('sparkles', size=15, stroke='#9A7B5F')}
        Built with Streamlit, Spark, Delta Lake, and Airflow
    </div>
    """,
    unsafe_allow_html=True,
)
