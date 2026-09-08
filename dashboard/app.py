"""
F1 Data Engineering Lab — Streamlit Dashboard
Port 8501 | Connects to PostgreSQL (labdb) on the Docker network
Provides visual analytics for the F1 Star Schema.
"""
import os
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import psycopg2
from psycopg2.extras import RealDictCursor
import psycopg2.extensions

# Register a typecaster to convert PostgreSQL DECIMAL to Python float
# This prevents Streamlit's Glide Data Grid from crashing with "Cannot read properties of undefined (reading 'params')"
# when trying to render PyArrow decimal128 columns.
DEC2FLOAT = psycopg2.extensions.new_type(
    psycopg2.extensions.DECIMAL.values,
    'DEC2FLOAT',
    lambda value, curs: float(value) if value is not None else None
)
psycopg2.extensions.register_type(DEC2FLOAT)

# ── Page config ─────────────────────────────────────────
st.set_page_config(
    page_title="F1 Data Engineering Lab",
    page_icon="🏎️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ───────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;800&display=swap');
  html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
  .metric-card {
    background: linear-gradient(135deg, #0f1429, #1a2050);
    border: 1px solid rgba(99,102,241,0.25);
    border-radius: 14px;
    padding: 1.2rem 1.5rem;
    text-align: center;
  }
  .stTabs [data-baseweb="tab"] { font-weight: 600; }
  .stTabs [aria-selected="true"] { color: #e10600 !important; border-bottom-color: #e10600 !important; }
  .f1-header {
    background: linear-gradient(135deg, #e10600 0%, #ff4433 50%, #1a0000 100%);
    padding: 1.5rem 2rem;
    border-radius: 14px;
    margin-bottom: 1.5rem;
    color: white;
  }
  .f1-header h1 { margin: 0; font-size: 2rem; font-weight: 900; }
  .f1-header p  { margin: 0.25rem 0 0; opacity: 0.85; }
</style>
""", unsafe_allow_html=True)

# ── DB connection ────────────────────────────────────────
@st.cache_resource
def get_conn():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        database=os.environ.get("POSTGRES_DB", "labdb"),
        user=os.environ.get("POSTGRES_USER", "labadmin"),
        password=os.environ.get("POSTGRES_PASSWORD", "labpassword"),
    )

import datetime

def query(sql: str, params=None) -> pd.DataFrame:
    try:
        conn = get_conn()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        df = pd.DataFrame(rows)
        # Convert to robust Pandas extension dtypes (e.g. Int64, Float64, String) 
        # to prevent Streamlit PyArrow from crashing on mixed object columns.
        df = df.convert_dtypes()
        
        # Fix Streamlit Glide Data Grid crash on Python datetime.date objects
        for col in df.columns:
            if not df[col].empty:
                first_valid = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
                if isinstance(first_valid, (datetime.date, datetime.datetime)):
                    df[col] = pd.to_datetime(df[col])
        return df
    except Exception as e:
        st.error(f"DB Error: {e}")
        return pd.DataFrame()

# ── Header ───────────────────────────────────────────────
st.markdown("""
<div class="f1-header">
  <h1>🏎️ F1 Data Engineering Lab</h1>
  <p>22MDCEL10 · CIT Coimbatore · F1DB Analytics Dashboard</p>
</div>
""", unsafe_allow_html=True)

# ── Top KPIs ────────────────────────────────────────────
col1, col2, col3, col4, col5 = st.columns(5)
kpi_sql = {
    "Drivers":    "SELECT COUNT(*) FROM dim_driver",
    "Circuits":   "SELECT COUNT(*) FROM dim_circuit",
    "Races":      "SELECT COUNT(*) FROM dim_race",
    "Seasons":    "SELECT COUNT(DISTINCT year) FROM dim_race",
    "Results":    "SELECT COUNT(*) FROM fact_race_results",
}
kpi_icons  = ["🏎️", "🗺️", "🏁", "📅", "📊"]
kpi_colors = ["#6366f1", "#22d3ee", "#10b981", "#f59e0b", "#a855f7"]

for col, (label, sql), icon, color in zip(
    [col1, col2, col3, col4, col5], kpi_sql.items(), kpi_icons, kpi_colors
):
    df = query(sql)
    val = int(df.iloc[0, 0]) if not df.empty else "—"
    col.metric(f"{icon} {label}", f"{val:,}" if isinstance(val, int) else val)

st.divider()

# ── Tabs ────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "🏆 Championship", "🏎️ Drivers", "🔧 Constructors", "🗺️ Circuits & Races", "⚡ Live Lap Events", "📊 Data Profiling & EDA"
])

# ══════════════════════════════════════════════
# TAB 1 — Championship Standings
# ══════════════════════════════════════════════
with tab1:
    st.subheader("🏆 Driver Championship Standings")

    years = query("SELECT DISTINCT year FROM dim_race ORDER BY year DESC")
    year_options = ["All Time"] + (years["year"].tolist() if not years.empty else [])
    selected_year = st.selectbox("Filter by Season", year_options, key="champ_year")

    year_filter = f"AND dr.year = {selected_year}" if selected_year != "All Time" else ""

    df_champ = query(f"""
        SELECT dd.driver_name, dd.code, dd.nationality,
               SUM(fr.points) AS total_points,
               SUM(CASE WHEN fr.finish_position = 1 THEN 1 ELSE 0 END) AS wins,
               SUM(CASE WHEN fr.finish_position <= 3 THEN 1 ELSE 0 END) AS podiums,
               COUNT(*) AS races
        FROM fact_race_results fr
        JOIN dim_driver dd ON fr.driver_id = dd.driver_id
        JOIN dim_race   dr ON fr.race_id   = dr.race_id
        WHERE fr.points IS NOT NULL {year_filter}
        GROUP BY dd.driver_name, dd.code, dd.nationality
        ORDER BY total_points DESC
        LIMIT 25
    """)

    if not df_champ.empty:
        df_champ.insert(0, "Pos", range(1, len(df_champ) + 1))

        col_a, col_b = st.columns([2, 1])
        with col_a:
            st.dataframe(
                df_champ,
                use_container_width=True, height=450,
            )
        with col_b:
            st.bar_chart(
                pd.DataFrame({
                    "driver_name": df_champ["driver_name"].head(15).astype(str).tolist(),
                    "total_points": df_champ["total_points"].head(15).astype(float).tolist()
                }),
                x="driver_name", y="total_points"
            )
    else:
        st.info("Run the ETL pipeline in Airflow first to load data.")

    st.subheader("🔧 Constructor Championship")
    df_con = query(f"""
        SELECT dcon.name AS constructor, dcon.nationality,
               SUM(fr.points) AS total_points,
               SUM(CASE WHEN fr.finish_position = 1 THEN 1 ELSE 0 END) AS wins,
               COUNT(DISTINCT fr.race_id) AS races
        FROM fact_race_results fr
        JOIN dim_constructor dcon ON fr.constructor_id = dcon.constructor_id
        JOIN dim_race        dr   ON fr.race_id        = dr.race_id
        WHERE fr.points IS NOT NULL {year_filter}
        GROUP BY dcon.name, dcon.nationality
        ORDER BY total_points DESC LIMIT 15
    """)
    if not df_con.empty:
        df_con.insert(0, "Pos", range(1, len(df_con) + 1))
        col_c, col_d = st.columns([2, 1])
        with col_c:
            st.dataframe(df_con,
                         use_container_width=True, height=350)
        with col_d:
            st.bar_chart(
                pd.DataFrame({
                    "constructor": df_con["constructor"].astype(str).tolist(),
                    "total_points": df_con["total_points"].astype(float).tolist()
                }),
                x="constructor", y="total_points"
            )

# ══════════════════════════════════════════════
# TAB 2 — Driver Profiles
# ══════════════════════════════════════════════
with tab2:
    st.subheader("🏎️ Driver Profiles")
    df_drivers = query("""
        SELECT driver_name, code, number, nationality, dob
        FROM dim_driver ORDER BY driver_name
    """)
    if not df_drivers.empty:
        col_srch, _ = st.columns([1, 2])
        with col_srch:
            search = st.text_input("Search driver", placeholder="e.g. Hamilton")
        if search:
            df_drivers = df_drivers[df_drivers["driver_name"].str.contains(search, case=False, na=False)]

        st.dataframe(df_drivers, use_container_width=True, height=400)

        # Career stats
        st.subheader("📊 Top 10 — Career Points Leaders")
        df_career = query("""
            SELECT dd.driver_name, dd.nationality,
                   SUM(fr.points) AS points,
                   SUM(CASE WHEN fr.finish_position=1 THEN 1 ELSE 0 END) AS wins,
                   SUM(CASE WHEN fr.finish_position<=3 THEN 1 ELSE 0 END) AS podiums,
                   COUNT(DISTINCT dr.year) AS seasons
            FROM fact_race_results fr
            JOIN dim_driver dd ON fr.driver_id = dd.driver_id
            JOIN dim_race   dr ON fr.race_id   = dr.race_id
            GROUP BY dd.driver_name, dd.nationality
            ORDER BY points DESC LIMIT 10
        """)
        if not df_career.empty:
            st.dataframe(df_career,
                         use_container_width=True, height=300)
    else:
        st.info("No driver data yet. Run Airflow ETL.")

# ══════════════════════════════════════════════
# TAB 3 — Constructors
# ══════════════════════════════════════════════
with tab3:
    st.subheader("🔧 Constructor Statistics")
    df_con_stats = query("""
        SELECT dcon.name AS constructor, dcon.nationality,
               COUNT(DISTINCT dr.year) AS seasons,
               COUNT(DISTINCT fr.race_id) AS race_entries,
               SUM(fr.points) AS total_points,
               SUM(CASE WHEN fr.finish_position=1 THEN 1 ELSE 0 END) AS wins,
               ROUND(AVG(fr.finish_position)::numeric, 1) AS avg_finish
        FROM fact_race_results fr
        JOIN dim_constructor dcon ON fr.constructor_id = dcon.constructor_id
        JOIN dim_race        dr   ON fr.race_id        = dr.race_id
        WHERE fr.finish_position IS NOT NULL
        GROUP BY dcon.name, dcon.nationality
        ORDER BY total_points DESC
    """)
    if not df_con_stats.empty:
        col_x, col_y = st.columns([3, 2])
        with col_x:
            st.dataframe(df_con_stats,
                         use_container_width=True, height=450)
        with col_y:
            st.markdown("**Wins by Constructor (Top 15)**")
            st.bar_chart(
                pd.DataFrame({
                    "constructor": df_con_stats["constructor"].head(15).astype(str).tolist(),
                    "wins": df_con_stats["wins"].head(15).astype(float).tolist()
                }),
                x="constructor", y="wins"
            )
    else:
        st.info("No constructor data yet. Run Airflow ETL.")

# ══════════════════════════════════════════════
# TAB 4 — Circuits & Races
# ══════════════════════════════════════════════
with tab4:
    st.subheader("🗺️ Circuit Directory")
    df_circuits = query("""
        SELECT name, location, country, lat, lng, alt
        FROM dim_circuit ORDER BY country, name
    """)
    if not df_circuits.empty:
        col_s, _ = st.columns([1, 2])
        with col_s:
            ctry = st.selectbox("Filter by Country", ["All"] + sorted(df_circuits["country"].dropna().unique().tolist()))
        if ctry != "All":
            df_circuits = df_circuits[df_circuits["country"] == ctry]
        st.dataframe(df_circuits, use_container_width=True, height=300)

        # Map
        map_df = df_circuits.dropna(subset=["lat","lng"]).rename(columns={"lat":"latitude","lng":"longitude"})
        if not map_df.empty:
            map_df["latitude"] = map_df["latitude"].astype(float)
            map_df["longitude"] = map_df["longitude"].astype(float)
            st.subheader("🌍 Circuit World Map")
            st.map(map_df[["latitude","longitude"]], zoom=1)
    else:
        st.info("No circuit data yet. Run Airflow ETL.")

    st.subheader("🏁 Recent Races")
    df_races = query("""
        SELECT dr.year, dr.round, dr.name AS race_name, dc.name AS circuit, dc.country, dr.race_date
        FROM dim_race dr JOIN dim_circuit dc ON dr.circuit_id = dc.circuit_id
        ORDER BY dr.year DESC, dr.round DESC LIMIT 30
    """)
    if not df_races.empty:
        st.dataframe(df_races, use_container_width=True, height=350)

# ══════════════════════════════════════════════
# TAB 5 — Live Kafka Lap Events
# ══════════════════════════════════════════════
with tab5:
    st.subheader("⚡ Live F1 Lap Events (Kafka Stream)")

    auto_refresh = st.checkbox("Auto-refresh every 5 seconds", value=True)
    if auto_refresh:
        import time
        st.caption("⏱️ Auto-refreshing…")
        time.sleep(5)
        st.rerun()

    col_v, col_e = st.columns(2)

    with col_v:
        st.markdown("### ✅ Valid Lap Events")
        df_laps = query("""
            SELECT sle.race_id, COALESCE(dd.code, sle.driver_id) AS driver,
                   sle.lap, sle.position, sle.lap_time, sle.milliseconds, sle.ingested_at
            FROM streaming_lap_events sle
            LEFT JOIN dim_driver dd ON sle.driver_id = dd.driver_id
            ORDER BY sle.ingested_at DESC LIMIT 25
        """)
        if not df_laps.empty:
            st.dataframe(df_laps, use_container_width=True, height=350)
            st.metric("Total Valid Events", len(df_laps))
        else:
            st.info("Waiting for Kafka producer to start streaming lap events…")

    with col_e:
        st.markdown("### ☠️ Dead Letter Queue")
        df_dlq = query("""
            SELECT source_id, error_type, error_message, failed_at
            FROM pipeline_errors ORDER BY failed_at DESC LIMIT 15
        """)
        if not df_dlq.empty:
            st.dataframe(df_dlq,
                         use_container_width=True, height=350)
            st.metric("Total DLQ Events", len(df_dlq))
        else:
            st.success("No errors. All lap events passed validation.")

    # ETL run log
    st.subheader("📜 ETL Run History (Airflow)")
    df_etl = query("SELECT * FROM etl_run_log ORDER BY started_at DESC LIMIT 10")
    if not df_etl.empty:
        st.dataframe(df_etl, use_container_width=True)

# ══════════════════════════════════════════════
# TAB 6 — Data Profiling & EDA
# ══════════════════════════════════════════════
with tab6:
    st.subheader("📊 Data Profiling & Exploratory Data Analysis")
    st.markdown("Automated EDA mimicking Data Science notebook processes.")
    
    table_options = ["dim_driver", "dim_circuit", "dim_race", "dim_constructor", "fact_race_results", "fact_driver_standings"]
    selected_table = st.selectbox("Select F1DB Table for Profiling", table_options)
    
    df_eda = query(f"SELECT * FROM {selected_table} LIMIT 10000")
    
    if not df_eda.empty:
        st.markdown(f"**Dataset Shape:** `{df_eda.shape[0]} rows` × `{df_eda.shape[1]} columns` (Sampled at 10,000 for performance)")
        
        # Missing values breakdown
        st.markdown("### 🔍 Missing Values")
        null_counts = df_eda.isnull().sum()
        null_df = pd.DataFrame({"Feature": null_counts.index, "Missing Values": null_counts.values, "Percentage (%)": (null_counts.values / len(df_eda) * 100).round(1)})
        col_m1, col_m2 = st.columns([1, 2])
        with col_m1:
            if null_counts.sum() > 0:
                st.table(null_df[null_df["Missing Values"] > 0])
            else:
                st.success("No missing values found!")
        with col_m2:
            if null_counts.sum() > 0:
                st.bar_chart(
                    pd.DataFrame({
                        "Feature": null_df["Feature"].astype(str).tolist(),
                        "Missing Values": null_df["Missing Values"].astype(float).tolist()
                    }),
                    x="Feature", y="Missing Values"
                )

        # Summary statistics
        st.markdown("### 📈 Data Dictionary & Summary Statistics")
        summary = []
        for col in df_eda.columns:
            s = df_eda[col]
            dtype = str(s.dtype)
            uniques = s.nunique()
            entry = {"Column": col, "Data Type": dtype, "Unique Values": uniques}
            
            if pd.api.types.is_numeric_dtype(s):
                entry.update({
                    "Min": float(round(s.min(), 2)) if not pd.isna(s.min()) else None,
                    "Max": float(round(s.max(), 2)) if not pd.isna(s.max()) else None,
                    "Mean": float(round(s.mean(), 2)) if not pd.isna(s.mean()) else None,
                    "Std Dev": float(round(s.std(), 2)) if not pd.isna(s.std()) else None,
                    "Top Value (Categorical)": None
                })
            else:
                top_val = s.value_counts().idxmax() if not s.empty and not s.dropna().empty else None
                entry.update({
                    "Min": None, "Max": None, "Mean": None, "Std Dev": None,
                    "Top Value (Categorical)": str(top_val) if top_val is not None else None
                })
            summary.append(entry)
            
        summary_df = pd.DataFrame(summary).fillna("-")
        st.table(summary_df)

        # Distributions
        st.markdown("### 📊 Distributions")
        num_cols = df_eda.select_dtypes(include=[np.number]).columns.tolist()
        
        if num_cols:
            selected_num = st.selectbox("Select Numerical Column for Histogram", num_cols)
            fig, ax = plt.subplots(figsize=(8, 3))
            ax.hist(df_eda[selected_num].dropna(), bins=20, color="#e10600", alpha=0.7)
            ax.set_title(f"Histogram of {selected_num}")
            ax.set_facecolor('#0f1429')
            fig.patch.set_facecolor('#0f1429')
            ax.tick_params(colors='white')
            ax.title.set_color('white')
            st.pyplot(fig)
            
    else:
        st.warning("Table is empty or not found.")

# ── Sidebar ──────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🏎️ F1 Lab Navigation")
    st.markdown("""
    | Service | URL |
    |---|---|
    | **This Dashboard** | :8501 |
    | **React Frontend** | :5173 |
    | **FastAPI Docs** | :8000/docs |
    | **Airflow UI** | :8080 |
    """)
    st.divider()
    st.markdown("### 🚀 Quick Actions")
    if st.button("🔄 Refresh All Data"):
        st.cache_resource.clear()
        st.rerun()
    st.divider()
    st.markdown("### 📁 F1DB Tables Loaded")
    for tbl in ["dim_driver","dim_circuit","dim_constructor","dim_race",
                 "fact_race_results","fact_driver_standings","fact_constructor_standings"]:
        df_cnt = query(f"SELECT COUNT(*) AS n FROM {tbl}")
        n = int(df_cnt.iloc[0,0]) if not df_cnt.empty else 0
        status = "✅" if n > 0 else "⏳"
        st.caption(f"{status} **{tbl}** — {n:,} rows")
