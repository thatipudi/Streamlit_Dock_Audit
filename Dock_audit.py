# Dock_audit.py — Infinitum Dock Audit: Form + Live Dashboard (Google Sheets ONLY)

import os
import base64
from datetime import datetime, date as date_cls

import streamlit as st
import pandas as pd
import altair as alt

# Sheets libs
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe


# -------------------- PAGE --------------------
st.set_page_config(page_title="Infinitum | Dock Audit", layout="wide", page_icon="📦")

# -------------------- GLOBAL CSS --------------------
st.markdown("""
<style>
  .block-container { max-width: 1280px; padding-top: .6rem; padding-bottom: 1.2rem; }
  .inf-header-wrap { margin: 8px 0 16px 0; }
  .inf-header {
    width: 100%;
    background: linear-gradient(90deg,#0A2540 0%,#183B5C 100%);
    border-radius: 14px; padding: 18px 22px;
    display: flex; align-items: center; gap: 18px;
    box-shadow: 0 6px 16px rgba(0,0,0,.18);
  }
  .inf-logo {
    width: 84px; height: 84px; border-radius: 12px; object-fit: contain;
    background:#fff; padding:10px; box-shadow: 0 4px 8px rgba(0,0,0,.22);
  }
  .inf-tt { display:flex; flex-direction:column; line-height:1.15; }
  .inf-title { color:#fff; font-weight:800; font-size:1.9rem; margin:0; }
  .inf-sub { color:#D7E2EC; font-size:1.05rem; margin:4px 0 0 0; }
  .stTextInput>div>div>input, .stTextArea textarea, .stSelectbox>div>div { border-radius: 10px !important; }
  .sticky-bar{
    position: sticky; bottom: 10px; z-index: 50;
    background: rgba(16,24,40,0.35); backdrop-filter: blur(6px);
    border: 1px solid rgba(255,255,255,0.08); border-radius: 12px; padding: 10px 12px; margin-top: 6px;
  }
</style>
""", unsafe_allow_html=True)

# -------------------- LOGO HEADER --------------------
LOGO_FILE = "Infinitum Logo.png"

def render_header(logo_path: str = LOGO_FILE,
                  title: str = "Dock Audit Dashboard",
                  subtitle: str = "Infinitum Electric"):
    if os.path.exists(logo_path):
        with open(logo_path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode()
        logo_html = f'<img class="inf-logo" src="data:image/png;base64,{b64}" />'
    else:
        logo_html = '<div class="inf-logo" style="display:flex;align-items:center;justify-content:center;font-weight:800;color:#183B5C;">IE</div>'
    st.markdown(
        f"""
        <div class="inf-header-wrap">
          <div class="inf-header">
            {logo_html}
            <div class="inf-tt">
              <div class="inf-title">{title}</div>
              <div class="inf-sub">{subtitle}</div>
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

render_header()

# -------------------- CANONICAL SCHEMA --------------------
CANON = [
    "event dock audit","linea","wo","sku","date","serial number","result",
    "finding","defect code","specific issue","classification","comments"
]
CANON_TITLE = [
    "Event Dock Audit","Linea","WO","SKU","Date","Serial Number","Result",
    "Finding","Defect Code","Specific Issue","Classification","Comments"
]
SYNONYMS = {
    "dock audit": "event dock audit", "event dock audit": "event dock audit",
    "line": "linea", "linea": "linea",
    "work order": "wo", "wo": "wo",
    "sku": "sku", "date": "date",
    "serial": "serial number", "serial number": "serial number",
    "status": "result", "result": "result",
    "finding": "finding",
    "defect": "defect code", "defect code": "defect code",
    "issue": "specific issue", "specific issue": "specific issue",
    "classification": "classification", "comments": "comments",
}
DEFECT_DICT = {
    "D01": "D01 - Dimension error", "C01": "C01 - Appearance defect",
    "M01": "M01 - Supplier fault", "M02": "M02 - Obsolete part",
    "M03": "M03 - Contract manufacturer fault", "A01": "A01 - Workmanship error",
    "A02": "A02 - Damage in production", "F01": "F01 - Function error",
    "F02": "F02 - Test failure", "L01": "L01 - Shipping error",
    "L02": "L02 - Handling damage", "S01": "S01 - Firmware error",
    "S02": "S02 - Embedded software error", "S03": "S03 - Version incorrect",
    "S04": "S04 - Faulty factory settings", "A03": "A03 - Defective crimp",
    "A04": "A04 - Printing defect", "A05": "A05 - Mising component",
    "A06": "A06 - Incorrect routing", "A07": "A07 - Noise",
    "F03": "F03 - Hipot reject",
}

# -------------------- UTIL --------------------
def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or (df.empty and df.columns.size == 0):
        return df
    out = df.copy()
    out.columns = [SYNONYMS.get(str(c).strip().lower().replace("\ufeff",""), str(c)) for c in out.columns]
    return out

def ensure_canon_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        df = pd.DataFrame()
    df = normalize_cols(df)
    for col in CANON:
        if col not in df.columns:
            df[col] = pd.Series(dtype="object")
    return df[CANON]

def result_norm(val: str) -> str:
    return {"accepted": "Pass", "rejected": "Reject"}.get(str(val).strip().lower(), val)

def code_from_label(lbl: str) -> str:
    s = str(lbl)
    return s.split(" - ")[0] if " - " in s else s

def is_duplicate(df: pd.DataFrame, serial: str, dt) -> bool:
    if df is None or df.empty:
        return False
    if "serial number" not in df.columns or "date" not in df.columns:
        return False
    try:
        d = pd.to_datetime(dt).date()
        df2 = df.copy()
        df2["date"] = pd.to_datetime(df2["date"], errors="coerce").dt.date
        return ((df2["serial number"].astype(str).str.strip()==str(serial).strip()) &
                (df2["date"]==d)).any()
    except Exception:
        return False

# -------------------- GOOGLE SHEETS LAYER (required) --------------------
if not (("google_service_account" in st.secrets) and ("SHEETS" in st.secrets)):
    st.error("Missing secrets. Please add [google_service_account] and [SHEETS] to secrets.toml.")
    st.stop()

def _extract_sheet_key(sheet_id_or_url: str) -> str:
    """Accepts either a pure spreadsheet key or a full URL and returns the key."""
    text = str(sheet_id_or_url).strip()
    if "/d/" in text:
        # Full URL -> split out the key between /d/ and the next /
        try:
            return text.split("/d/")[1].split("/")[0]
        except Exception:
            pass
    return text

@st.cache_resource(show_spinner=False)
def _get_gs_client():
    creds_info = dict(st.secrets["google_service_account"])
    # Convert literal '\n' to real newlines if needed
    if "private_key" in creds_info:
        creds_info["private_key"] = creds_info["private_key"].replace("\\n", "\n")
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    return gspread.authorize(creds)

def _open_sheet():
    client = _get_gs_client()
    sheet_id_raw = st.secrets["SHEETS"]["SHEET_ID"]
    sheet_key = _extract_sheet_key(sheet_id_raw)
    tab = st.secrets["SHEETS"]["TAB_NAME"]
    sh = client.open_by_key(sheet_key)
    ws = sh.worksheet(tab)
    return ws

@st.cache_data(show_spinner=False)
def load_data() -> pd.DataFrame:
    ws = _open_sheet()
    df = get_as_dataframe(ws, header=0, evaluate_formulas=True).dropna(how="all")
    if df.empty:
        return ensure_canon_columns(pd.DataFrame(columns=CANON))
    df = ensure_canon_columns(df)
    df["result"] = df["result"].apply(result_norm)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df

def append_and_save(entry_df: pd.DataFrame):
    ws = _open_sheet()
    current = get_as_dataframe(ws, header=0, evaluate_formulas=True).dropna(how="all")
    if current.empty:
        out = entry_df.copy()
    else:
        current = ensure_canon_columns(current)
        out = pd.concat([current, entry_df], ignore_index=True)
    # Write back with pretty headers:
    out2 = out.copy()
    out2.columns = CANON_TITLE
    ws.clear()
    set_with_dataframe(ws, out2, include_index=False, include_column_header=True, resize=True)

# -------------------- FORM --------------------
defect_options = list(DEFECT_DICT.values())

st.markdown("### Submit Dock Audit Entry")
fc1, fc2, fc3 = st.columns([1,1,1], gap="large")

with fc1:
    dock_audit = st.text_input("Dock Audit", value="Audit")
    linea      = st.selectbox("Linea", ["Line A","Line B","Line C"])
    wo         = st.text_input("WO", placeholder="WO-12345")
    sku        = st.text_input("SKU", placeholder="SKU-ABC-123")

with fc2:
    serial         = st.text_input("Serial Number", placeholder="SN-000001")
    result_ui      = st.selectbox("Result", ["Accepted","Rejected"])
    defect_label   = st.selectbox("Defect Code", defect_options)
    finding        = st.text_input("Finding", placeholder="Short description")

with fc3:
    dt             = st.date_input("Date", datetime.today())
    specific_issue = st.text_input("Specific Issue", placeholder="Cosmetic / Functional / etc.")
    classification = st.selectbox("Classification", ["Minor","Major","Critical"])
    comments       = st.text_area("Comments", height=68)

defect_code  = code_from_label(defect_label)
result_store = result_norm(result_ui)

new_entry = pd.DataFrame([{
    "event dock audit": dock_audit,
    "linea": linea,
    "wo": wo,
    "sku": sku,
    "date": dt,
    "serial number": serial,
    "result": result_store,
    "finding": finding,
    "defect code": defect_code,
    "specific issue": specific_issue,
    "classification": classification,
    "comments": comments
}], columns=CANON)

# Sticky actions
st.markdown("<div class='sticky-bar'>", unsafe_allow_html=True)
cbtn, cprev = st.columns([1,5])
with cbtn:
    submitted = st.button("📤 Submit Entry", use_container_width=True, key="submit_btn")
with cprev:
    with st.expander("🔍 Preview Entry"):
        st.json(new_entry.to_dict(orient="records")[0])
st.markdown("</div>", unsafe_allow_html=True)

if submitted:
    required = [dock_audit, linea, wo, sku, serial, finding, specific_issue]
    if any(str(x).strip()=="" for x in required):
        st.warning("⚠️ Please fill all required fields (Linea, WO, SKU, Serial, Finding, Specific Issue).")
    elif is_duplicate(load_data(), serial, dt):
        st.error("🚫 Duplicate entry detected for this Serial Number & Date.")
    else:
        try:
            append_and_save(new_entry)
            load_data.clear()
            try:
                st.toast("✅ Entry saved! Dashboard updating…", icon="✅")
            except Exception:
                st.success("✅ Entry saved! Dashboard updating…")
            st.rerun()
        except Exception as e:
            st.error(f"❌ Save failed: {e}")

st.markdown("---")

# -------------------- DASHBOARD --------------------
df = load_data()

# Sidebar Filters
with st.sidebar:
    st.header("Filters")
    if not df.empty and "date" in df.columns:
        min_d = pd.to_datetime(df["date"].min()).date() if pd.notna(df["date"].min()) else date_cls.today()
        max_d = pd.to_datetime(df["date"].max()).date() if pd.notna(df["date"].max()) else date_cls.today()
    else:
        min_d = max_d = date_cls.today()
    date_range = st.date_input("Date Range", (min_d, max_d))
    linea_vals = sorted(df["linea"].dropna().unique()) if "linea" in df.columns else []
    sku_vals   = sorted(df["sku"].dropna().unique()) if "sku" in df.columns else []
    f_linea = st.multiselect("Linea", linea_vals)
    f_sku   = st.multiselect("SKU", sku_vals)
    f_res   = st.multiselect("Result", ["Pass","Reject"])
    st.divider()
    if st.button("↺ Reset Filters", use_container_width=True):
        st.session_state.clear()
        st.rerun()

# Apply filters
if not df.empty:
    if date_range and len(date_range)==2 and "date" in df.columns:
        df = df[(pd.to_datetime(df["date"]).dt.date >= date_range[0]) &
                (pd.to_datetime(df["date"]).dt.date <= date_range[1])]
    if f_linea and "linea" in df.columns:
        df = df[df["linea"].isin(f_linea)]
    if f_sku and "sku" in df.columns:
        df = df[df["sku"].isin(f_sku)]   # <-- fixed here
    if f_res and "result" in df.columns:
        df = df[df["result"].isin(f_res)]

# Today metrics
st.markdown("#### Today")
t1, t2, t3 = st.columns(3)
if not df.empty and "date" in df.columns:
    tmp = df.copy()
    tmp["date"] = pd.to_datetime(tmp["date"], errors="coerce").dt.date
    today = datetime.today().date()
    td = tmp[tmp["date"] == today]
    total_t  = len(td)
    pass_t   = (td["result"]=="Pass").sum() if "result" in td.columns else 0
    rej_t    = (td["result"]=="Reject").sum() if "result" in td.columns else 0
    rate_t   = round((rej_t/total_t)*100,2) if total_t else 0.0
    t1.metric("Audits Today", total_t)
    t2.metric("Pass Today",  pass_t)
    t3.metric("Rejection % Today", f"{rate_t}%")
else:
    t1.metric("Audits Today", 0); t2.metric("Pass Today", 0); t3.metric("Rejection % Today", "0%")

# Key Metrics
st.markdown("#### Key Metrics")
k1, k2, k3, k4, k5 = st.columns(5, gap="small")

if df.empty:
    for c in (k1, k2, k3, k4, k5):
        c.metric("-", "-")
    st.info("No entries yet. Submit above to populate the dashboard.")
else:
    total   = len(df)
    passed  = (df["result"]=="Pass").sum()   if "result" in df.columns else 0
    failed  = (df["result"]=="Reject").sum() if "result" in df.columns else 0
    rate    = round((failed/total)*100, 2) if total else 0.0
    uniqSKU = df["sku"].nunique() if "sku" in df.columns else 0

    k1.metric("Total Inspected", total)
    k2.metric("Total Passed",   passed)
    k3.metric("Total Failed",   failed)
    k4.metric("Rejection Rate %", f"{rate}%")
    k5.metric("Unique SKUs", uniqSKU)

    work = df.copy()
    if "date" in work.columns:
        work["date"] = pd.to_datetime(work["date"], errors="coerce")
        work["YearMonth"] = work["date"].dt.to_period("M").astype(str)
    else:
        work["YearMonth"] = "Unknown"

    if "result" in work.columns:
        monthly = (work.groupby("YearMonth")
                        .agg(total=("result","count"),
                             rejects=("result", lambda s: (s=="Reject").sum()))
                        .reset_index())
        monthly["RejectionRate%"] = (monthly["rejects"]/monthly["total"]*100).round(2)
    else:
        monthly = pd.DataFrame({"YearMonth":[], "total":[], "rejects":[], "RejectionRate%":[]})

    g1, g2 = st.columns([3,2], gap="large")

    if not monthly.empty:
        bar_rr = alt.Chart(monthly).mark_bar().encode(
            x=alt.X("YearMonth:O", title="Year–Month"),
            y=alt.Y("RejectionRate%:Q", title="Monthly Rejection Rate %"),
            tooltip=["YearMonth","total","rejects","RejectionRate%"]
        ).properties(height=300)
        g1.altair_chart(bar_rr, use_container_width=True)
    else:
        g1.info("No monthly data yet.")

    g2.metric("Rejection Rate % (overall)", f"{rate}%")

    g3, g4 = st.columns([2,2], gap="large")

    if "result" in work.columns and "classification" in work.columns:
        cls_counts = (work.loc[work["result"]=="Reject","classification"]
                         .value_counts().reset_index())
        cls_counts.columns = ["Classification","Count"]
        if not cls_counts.empty:
            donut = alt.Chart(cls_counts).mark_arc(innerRadius=60).encode(
                theta="Count:Q", color="Classification:N",
                tooltip=["Classification","Count"]
            ).properties(height=300)
            g3.altair_chart(donut, use_container_width=True)
        else:
            g3.info("No rejected classifications yet.")
    else:
        g3.info("Classification data not found.")

    if "result" in work.columns and "defect code" in work.columns:
        top_def = (work.loc[work["result"]=="Reject","defect code"]
                      .value_counts().reset_index().head(10))
        top_def.columns = ["Defect Code","Count"]
        top_def["Defect Code"] = top_def["Defect Code"].map(lambda x: DEFECT_DICT.get(str(x), str(x)))
        if not top_def.empty:
            bar_def = alt.Chart(top_def).mark_bar().encode(
                x=alt.X("Count:Q"),
                y=alt.Y("Defect Code:N", sort="-x"),
                tooltip=["Defect Code","Count"]
            ).properties(height=300)
            g4.altair_chart(bar_def, use_container_width=True)
        else:
            g4.info("No defect code data yet.")

    st.markdown("#### Monthly Audit Trend by Status")
    if "result" in work.columns:
        trend = (work.groupby(["YearMonth","result"])
                      .size().reset_index(name="Count"))
        if not trend.empty:
            line = alt.Chart(trend).mark_line(point=True).encode(
                x=alt.X("YearMonth:O", title="Year–Month"),
                y=alt.Y("Count:Q", title="Count of D.A. STATUS"),
                color=alt.Color("result:N", title="Result",
                                scale=alt.Scale(domain=["Pass","Reject"])),
                tooltip=["YearMonth","result","Count"]
            ).properties(height=320)
            st.altair_chart(line, use_container_width=True)
        else:
            st.info("No trend data yet.")
    else:
        st.info("Result column not found.")

    st.markdown("#### Recent 25 Entries")
    show_cols = [c for c in ["date","linea","wo","sku","serial number","result",
                             "defect code","classification","finding","comments"]
                 if c in work.columns]
    st.dataframe(
        work.sort_values("date" if "date" in work.columns else work.index)[show_cols].tail(25),
        use_container_width=True
    )

    # CSV download (Sheets-only)
    csv_bytes = work.to_csv(index=False).encode("utf-8")
    st.download_button("⬇️ Download CSV", data=csv_bytes,
                       file_name="dock_audit_entries.csv", mime="text/csv")

