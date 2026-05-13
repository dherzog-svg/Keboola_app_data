import asyncio
import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

try:
    from kai_client import KaiClient
    KAI_AVAILABLE = True
except ImportError:
    KAI_AVAILABLE = False

# ### INJECTED_CODE ####
# ### QUERY DATA FUNCTION ####
import os
import httpx
import pandas as pd
def query_data(query: str) -> pd.DataFrame:
    branch_id = os.environ.get('BRANCH_ID')
    workspace_id = os.environ.get('WORKSPACE_ID')
    kbc_url = os.environ.get('KBC_URL')
    token = os.environ.get('KBC_TOKEN')
    if not branch_id or not workspace_id or not kbc_url or not token:
        raise RuntimeError('Missing required environment variables: BRANCH_ID, WORKSPACE_ID, KBC_URL, KBC_TOKEN.')
    timeout = httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=None)
    limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
    if token.startswith('Bearer '):
        headers = {'Authorization': token, 'Accept': 'application/json'}
    else:
        headers = {'X-StorageAPI-Token': token, 'Accept': 'application/json'}
    with httpx.Client(timeout=timeout, limits=limits) as client:
        response = client.post(
            f'{kbc_url}/v2/storage/branch/{branch_id}/workspaces/{workspace_id}/query',
            json={'query': query},
            headers=headers,
        )
        response.raise_for_status()
        response_json = response.json()
        if response_json.get('status') == 'error':
            raise ValueError(f'Error when executing query "{query}": {response_json.get("message")}.')
        return pd.DataFrame(response_json['data']['rows'])
# ### END_OF_INJECTED_CODE ####


# =============================================================================
# KAI AI CONFIGURATION
# =============================================================================
DATA_APP_CONFIG_ID = "01kmj2b099g9msvkv5jy1qen64"

CONTEXT_STRING = f"""You are an AI assistant embedded in a Keboola Data App for Groupon's INTL markets analytics.

## YOUR FIRST PRIORITY: UNDERSTAND THE APP'S METHODOLOGY
Before answering ANY analytical or calculation question, you MUST:
1. **Look up this Data App's source code** using the `get_data_apps` tool with configuration_id: "{DATA_APP_CONFIG_ID}"
2. **Analyze the code** to understand the tables, metrics, formulas, filters, and business logic
3. **Use the SAME methodology** as the app when answering questions

## DATA SOURCES YOU HAVE ACCESS TO
- `user_behaviour_INTL` — rolling snapshot of UV, active UV, CVR, bookings, M1 VFM per country (data refreshed daily)
- `financial_INTL_app` — daily orders, gross bookings, M1 VFM, M2 estimate, activations, reactivations, refunds, OD/ILS discounts by country × platform (incremental daily updates)
- `cohort_INTL_app` — weekly cohort CVR and M1 VFM per UV across D1-7, D7-14, D14-28 windows (incremental, growing history)

## KEY BUSINESS RULES
- `od_applied` and `ils_applied` are NEGATIVE numbers — always use abs() for rates/display
- CVR in cohort table is already a percentage (e.g. 5.74 means 5.74%)
- Markets: AE, AU, BE, CA, DE, ES, FR, GB, IE, IT, NL, PL
- Platforms: iphone, android, ipad

## BEFORE ANSWERING CALCULATION QUESTIONS
Ask clarifying questions first: time period, markets, filters? Only proceed after user confirms.

User question: """

STORAGE_API_TOKEN = os.environ.get("STORAGE_API_TOKEN") or os.environ.get("KBC_TOKEN", "")
STORAGE_API_URL = os.environ.get("STORAGE_API_URL") or os.environ.get("KBC_URL", "")

if "kai_messages" not in st.session_state:
    st.session_state.kai_messages = []
if "kai_chat_id" not in st.session_state:
    st.session_state.kai_chat_id = KaiClient.new_chat_id() if KAI_AVAILABLE else None
if "pending_approval" not in st.session_state:
    st.session_state.pending_approval = None
if "pending_prompt" not in st.session_state:
    st.session_state.pending_prompt = None

def run_async(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()

async def get_client():
    return await KaiClient.from_storage_api(
        storage_api_token=STORAGE_API_TOKEN,
        storage_api_url=STORAGE_API_URL,
    )

async def collect_chat_response(chat_id, text, container):
    accumulated = ""
    pending = None
    tool_names = {}
    text_placeholder = container.empty()
    try:
        client = await get_client()
        async with client:
            async for event in client.send_message(chat_id, text):
                if event.type == "text":
                    accumulated += event.text
                    text_placeholder.markdown(accumulated + "▌")
                elif event.type == "tool-call":
                    call_id = getattr(event, "tool_call_id", "")
                    name = getattr(event, "tool_name", None)
                    state = getattr(event, "state", None)
                    if name:
                        tool_names[call_id] = name
                    display_name = name or tool_names.get(call_id, "tool")
                    if state == "input-available":
                        text_placeholder.markdown(accumulated)
                        container.info(f"🔧 Calling **{display_name}**...")
                        text_placeholder = container.empty()
                    elif state == "output-available":
                        text_placeholder.markdown(accumulated)
                        container.success(f"✅ **{display_name}** completed.")
                        text_placeholder = container.empty()
                elif event.type == "tool-approval-request":
                    pending = {"approval_id": event.approval_id, "tool_call_id": event.tool_call_id}
                elif event.type == "error":
                    container.error(getattr(event, "message", "Unknown error"))
        text_placeholder.markdown(accumulated)
        return accumulated, pending
    except Exception as e:
        error_msg = str(e)
        if "401" in error_msg:
            container.error(f"🔐 **Authentication Error (401)** — token may be invalid.\n\n{error_msg}")
        else:
            container.error(f"Error: {error_msg}")
        return f"Error: {error_msg}", None

async def collect_approval_response(chat_id, approval_id, approved, container):
    accumulated = ""
    text_placeholder = container.empty()
    client = await get_client()
    async with client:
        if approved:
            stream = client.approve_tool(chat_id=chat_id, approval_id=approval_id)
        else:
            stream = client.reject_tool(chat_id=chat_id, approval_id=approval_id, reason="User denied")
        async for event in stream:
            if event.type == "text":
                accumulated += event.text
                text_placeholder.markdown(accumulated + "▌")
            elif event.type == "error":
                container.error(getattr(event, "message", "Unknown error"))
    text_placeholder.markdown(accumulated)
    return accumulated


# =============================================================================
# PAGE CONFIG & STYLING
# =============================================================================
st.set_page_config(
    page_title="INTL Markets Analytics",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    /* Clean sidebar */
    [data-testid="stSidebar"] {
        background-color: #f8f9fa;
    }
    [data-testid="stSidebar"] h2 {
        color: #1a1a2e;
        font-size: 1.1rem;
        font-weight: 700;
        margin-top: 0.5rem;
    }
    /* Metric cards */
    [data-testid="metric-container"] {
        background-color: #ffffff;
        border: 1px solid #e8ecf0;
        border-radius: 10px;
        padding: 14px 18px;
        box-shadow: 0 1px 4px rgba(0,0,0,0.06);
    }
    /* Tab styling */
    [data-testid="stTabs"] button {
        font-weight: 600;
        font-size: 0.9rem;
    }
    /* Section divider labels */
    .section-label {
        font-size: 0.75rem;
        font-weight: 700;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        color: #6b7280;
        margin-bottom: 0.25rem;
    }
    /* Insight callout */
    .insight-box {
        background: #eef6ff;
        border-left: 4px solid #2196F3;
        border-radius: 6px;
        padding: 10px 16px;
        font-size: 0.88rem;
        color: #1a3a5c;
        margin-bottom: 1rem;
    }
</style>
""", unsafe_allow_html=True)


# =============================================================================
# DATA LOADING
# =============================================================================
COHORT_WINDOWS = ['day_01', 'day_01_07', 'day_01_14', 'day_01_21', 'day_01_28']
COHORT_BASE_METRICS = ['orders', 'purchasers', 'nob', 'gross_bookings', 'm1_vfm', 'm2_estimate',
                       'ils_applied', 'od_applied', 'deals_all', 'deals_with_od_applied',
                       'deals_with_ils_applied', 'gb_deals_with_od_applied', 'gb_deals_with_ils_applied']

@st.cache_data(ttl=300)
def load_cohort_data():
    df = query_data('SELECT * FROM `kbc-grpn-40-0cd2`.`out_c_testing_data_apps`.`cohort_INTL_app`')
    numeric_cols = ['UV'] + [f'{w}_{m}' for w in COHORT_WINDOWS for m in COHORT_BASE_METRICS]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    df['cohort_week'] = pd.to_datetime(df['cohort_week'])
    df = (df.sort_values('UV', ascending=False)
            .drop_duplicates(subset=['country', 'cohort_week'], keep='first')
            .sort_values(['country', 'cohort_week'])
            .reset_index(drop=True))
    return df

@st.cache_data(ttl=300)
def load_yoy_data():
    df = query_data('''
    SELECT `iso_year`, `iso_week`, `iso_year_week`, `iso_week_start`,
           `country_code`, `groupon_version`, `operating_system`,
           SUM(`m1_vfm`) AS m1_vfm,
           SUM(`gross_bookings`) AS gross_bookings,
           SUM(`orders`) AS orders,
           SUM(`nob`) AS nob,
           SUM(`activations`) AS activations,
           SUM(`purchasers`) AS purchasers
    FROM `kbc-grpn-40-0cd2`.`out_c_intl_app_yoy_trends`.`weekly_yoy_INTL_app`
    GROUP BY 1, 2, 3, 4, 5, 6, 7
    ''')
    for col in ['iso_year','iso_week','m1_vfm','gross_bookings','orders','nob','activations','purchasers']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    df['iso_week_start'] = pd.to_datetime(df['iso_week_start'])
    return df

@st.cache_data(ttl=300)
def load_yoy_uv_data():
    df = query_data('''
    SELECT `iso_year`, `iso_week`, `iso_year_week`, `iso_week_start`,
           `country_code`, `operating_system`,
           MAX(`weekly_distinct_bcookies`) AS weekly_distinct_bcookies
    FROM `kbc-grpn-40-0cd2`.`out_c_intl_app_yoy_trends`.`weekly_uv_INTL_app`
    GROUP BY 1, 2, 3, 4, 5, 6
    ''')
    for col in ['iso_year','iso_week','weekly_distinct_bcookies']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    df['iso_week_start'] = pd.to_datetime(df['iso_week_start'])
    return df


# =============================================================================
# LOAD DATA
# =============================================================================
try:
    coh_df_raw = load_cohort_data()
except Exception:
    coh_df_raw = pd.DataFrame()

try:
    yoy_df_raw = load_yoy_data()
except Exception:
    yoy_df_raw = pd.DataFrame()

try:
    yoy_uv_df_raw = load_yoy_uv_data()
except Exception:
    yoy_uv_df_raw = pd.DataFrame()


# =============================================================================
# SIDEBAR — GLOBAL FILTERS
# =============================================================================
with st.sidebar:
    st.markdown("## 🌍 INTL Markets Analytics")
    st.markdown("---")

    # Country filter — shared across Cohort + YoY tabs
    all_countries = sorted(set(
        list(coh_df_raw['country'].dropna().unique() if not coh_df_raw.empty else []) +
        list(yoy_df_raw['country_code'].dropna().unique() if not yoy_df_raw.empty else [])
    ))
    core_countries = all_countries
    st.markdown('<p class="section-label">Markets</p>', unsafe_allow_html=True)
    select_all_mkts = st.checkbox("Select all", value=True, key="select_all_mkts")
    if select_all_mkts:
        selected_countries = core_countries
    else:
        selected_countries = st.multiselect(
            "Select markets", options=all_countries, default=core_countries,
            label_visibility="collapsed", key="global_countries"
        )
        if not selected_countries:
            selected_countries = core_countries

    if KAI_AVAILABLE and STORAGE_API_TOKEN:
        st.markdown("---")
        st.markdown('<p class="section-label">Kai AI</p>', unsafe_allow_html=True)
        if st.button("🗑️ New Chat", key="new_chat", use_container_width=True):
            st.session_state.kai_messages = []
            st.session_state.kai_chat_id = KaiClient.new_chat_id()
            st.session_state.pending_approval = None
            st.rerun()


# =============================================================================
# PAGE HEADER
# =============================================================================
st.markdown("# 🌍 INTL APP Markets by countries")
st.markdown("Sidebar holds global Markets filter. Tab-specific filters live inline next to the relevant table/chart.")

tab_yoy, tab_cohort, tab_kai, tab_docs = st.tabs([
    "📈 YoY Trends",
    "🔄 Cohort Analysis",
    "🤖 Ask Kai",
    "📚 Docs"
])


# =============================================================================
# TAB 0 — YoY TRENDS
# =============================================================================
with tab_yoy:
    st.caption("Weekly Year-over-Year and Week-over-Week trends for the INTL App, mirroring Radomir's NA YoY sheet. **2024 data starts at ISO week 19** (backfill window: 2024-05-06 → 2024-12-23) — earlier weeks show blank, and 2025 YoY% vs 2024 is blank wherever 2024 has no data.")

    if yoy_df_raw.empty or yoy_uv_df_raw.empty:
        st.warning("YoY tables not yet available. The data load may still be in progress. Check `weekly_yoy_INTL_app` and `weekly_uv_INTL_app`.")
    else:
        # --- Inline filters ---
        f1, f2, f3 = st.columns([1, 1, 2])
        with f1:
            countries_yoy = sorted(yoy_df_raw['country_code'].dropna().unique())
            default_country = 'GB' if 'GB' in countries_yoy else (countries_yoy[0] if countries_yoy else None)
            sel_country = st.selectbox(
                "Country", countries_yoy,
                index=countries_yoy.index(default_country) if default_country else 0,
                key="yoy_country"
            )
        with f2:
            version_opts = ['All', 'legacy', 'mbnxt']
            sel_version = st.selectbox(
                "Groupon Version", version_opts, index=0, key="yoy_version",
                help="Side-by-side MBNXT-vs-Legacy isn't measurable on INTL (no local bucketing). Pick a single version for a within-cohort YoY read."
            )
        with f3:
            os_opts = sorted(yoy_df_raw['operating_system'].dropna().unique())
            default_os = ['iOS'] if 'iOS' in os_opts else os_opts[:1]
            sel_os = st.multiselect("OS", os_opts, default=default_os, key="yoy_os")

        if not sel_os:
            st.warning("Pick at least one OS.")
            st.stop()

        # --- Filter + aggregate to weekly grain ---
        yoy_f = yoy_df_raw[
            (yoy_df_raw['country_code'] == sel_country) &
            (yoy_df_raw['operating_system'].isin(sel_os))
        ]
        uv_f = yoy_uv_df_raw[
            (yoy_uv_df_raw['country_code'] == sel_country) &
            (yoy_uv_df_raw['operating_system'].isin(sel_os))
        ]
        if sel_version != 'All':
            yoy_f = yoy_f[yoy_f['groupon_version'] == sel_version]

        if yoy_f.empty:
            st.warning(f"No data for {sel_country} / {sel_version} / {'+'.join(sel_os)}.")
        else:
            # Aggregate daily-grain → weekly (sum over groupon_version if "All", sum over days within a week)
            weekly_yoy = yoy_f.groupby(['iso_year', 'iso_week', 'iso_year_week', 'iso_week_start'], as_index=False).agg(
                m1_vfm=('m1_vfm', 'sum'),
                orders=('orders', 'sum'),
                gross_bookings=('gross_bookings', 'sum'),
                nob=('nob', 'sum'),
            )
            # UV table is already weekly grain
            weekly_uv = uv_f.groupby(['iso_year', 'iso_week', 'iso_year_week', 'iso_week_start'], as_index=False).agg(
                uvs=('weekly_distinct_bcookies', 'sum')
            )
            weekly = weekly_yoy.merge(weekly_uv, on=['iso_year', 'iso_week', 'iso_year_week', 'iso_week_start'], how='outer')
            weekly['m1_vfm_per_uv'] = weekly.apply(
                lambda r: (r['m1_vfm'] / r['uvs']) if r.get('uvs', 0) and r['uvs'] > 0 else None,
                axis=1
            )
            weekly = weekly.sort_values(['iso_year', 'iso_week']).reset_index(drop=True)

            # --- Render a YoY table per metric ---
            def render_yoy_table(metric_col, metric_label, fmt_fn):
                pivot = weekly.pivot_table(index='iso_week', columns='iso_year', values=metric_col, aggfunc='sum')
                pivot = pivot.sort_index()  # ISO weeks 1..53
                pivot = pivot.reindex(sorted(pivot.columns), axis=1)  # years left→right oldest→newest
                years = list(pivot.columns)
                # Year-over-year columns — mask zero/missing prior-year to avoid +inf% on partial backfills
                for i in range(1, len(years)):
                    prior = pivot[years[i-1]].replace(0, np.nan)
                    pivot[f"{years[i]} YoY %"] = (pivot[years[i]] / prior - 1) * 100
                # WoW % for the latest year
                if years:
                    latest = years[-1]
                    pivot['WoW %'] = pivot[latest].pct_change() * 100
                # Clean any residual inf values (e.g. float roundoff) → blank
                pivot = pivot.replace([np.inf, -np.inf], np.nan)
                # Oldest week first
                pivot = pivot.sort_index(ascending=True)

                st.markdown(f"### {metric_label}")
                show = pivot.reset_index().rename(columns={'iso_week': 'ISO Week'})

                pct_cols = [c for c in show.columns if isinstance(c, str) and "%" in c]
                year_cols = [c for c in show.columns if c in years]

                def _pct_bg(v, cap=50.0):
                    if pd.isna(v):
                        return ''
                    intensity = min(abs(v), cap) / cap
                    if v >= 0:
                        r = int(255 - intensity * 127)
                        g = int(255 - intensity * 56)
                        b = int(255 - intensity * 127)
                    else:
                        r = int(255 - intensity * 23)
                        g = int(255 - intensity * 127)
                        b = int(255 - intensity * 127)
                    return f'background-color: rgb({r},{g},{b})'

                fmt_map = {"ISO Week": "{:d}"}
                for c in year_cols:
                    fmt_map[c] = lambda v, ff=fmt_fn: '' if pd.isna(v) else (ff % v)
                for c in pct_cols:
                    fmt_map[c] = lambda v: '' if pd.isna(v) else f"{v:+.1f}%"

                styled = show.style.format(fmt_map).map(_pct_bg, subset=pct_cols)
                st.dataframe(styled, use_container_width=True, hide_index=True)

            render_yoy_table('m1_vfm', '💰 M1 VFM', "$%.0f")
            render_yoy_table('m1_vfm_per_uv', '💵 M1 VFM per UV', "$%.2f")
            render_yoy_table('uvs', '👥 Distinct UVs', "%.0f")

            # --- Optional line chart per metric ---
            st.markdown("---")
            st.markdown("### Trend chart")
            metric_choice = st.radio(
                "Metric", ["M1 VFM", "M1 VFM per UV", "Distinct UVs"], horizontal=True, key="yoy_metric_chart"
            )
            metric_map = {"M1 VFM": "m1_vfm", "M1 VFM per UV": "m1_vfm_per_uv", "Distinct UVs": "uvs"}
            mc = metric_map[metric_choice]
            chart_df = weekly[['iso_year', 'iso_week', mc]].dropna(subset=[mc]).copy()
            chart_df['iso_year_str'] = chart_df['iso_year'].astype(int).astype(str)
            fig = px.line(chart_df, x='iso_week', y=mc, color='iso_year_str',
                          markers=True,
                          labels={'iso_week': 'ISO Week', mc: metric_choice, 'iso_year_str': 'Year'},
                          title=f"{metric_choice} — {sel_country} / {sel_version} / {'+'.join(sel_os)}")
            fig.update_layout(height=380, margin=dict(l=0, r=0, t=40, b=10))
            st.plotly_chart(fig, use_container_width=True)

            # --- Download ---
            st.markdown("---")
            with st.expander("📋 Raw weekly data"):
                st.dataframe(weekly, use_container_width=True, hide_index=True)
                st.download_button("📥 Download CSV", weekly.to_csv(index=False),
                                   f"yoy_{sel_country}_{sel_version}_{'-'.join(sel_os)}.csv", "text/csv", key="yoy_dl")



# =============================================================================
# TAB 1 — COHORT ANALYSIS
# =============================================================================
with tab_cohort:
    st.caption("New-user cohorts by first-visit week. Each metric shown across cumulative windows (Day 1 / 1–7 / 1–14 / 1–21 / 1–28). Mirrors the NA Android Local-Bucketing report layout, without the groupon_version split (no local bucketing on INTL).")

    if coh_df_raw.empty:
        st.warning("No cohort data available.")
    else:
        coh_df = coh_df_raw[coh_df_raw['country'].isin(selected_countries)].copy()

        countries_coh = sorted(coh_df['country'].unique())
        if not countries_coh:
            st.info("No cohort data for the selected markets.")
        else:
            default_c = 'GB' if 'GB' in countries_coh else countries_coh[0]
            metric_options = {
                'Orders': 'orders',
                'Purchasers': 'purchasers',
                'CVR (%)': 'cvr',
                'NOB (USD)': 'nob',
                'Gross Bookings (USD)': 'gross_bookings',
                'M1 VFM (USD)': 'm1_vfm',
                'M1 VFM / UV (USD)': 'm1_vfm_per_uv',
                'AOV (NOB / order, USD)': 'aov',
                'Avg M1 VFM / order (USD)': 'avg_m1_vfm',
                'Deals (Quantity)': 'deals_all',
                'New UVs (day_01 only)': 'UV',
            }
            f1, f2 = st.columns([1, 2])
            with f1:
                sel_country_coh = st.selectbox(
                    "Country", countries_coh,
                    index=countries_coh.index(default_c), key="coh_country"
                )
            with f2:
                sel_metric_label = st.selectbox(
                    "Metric", list(metric_options.keys()),
                    index=list(metric_options.keys()).index('Orders'),
                    key="coh_metric"
                )

            cdf = coh_df[coh_df['country'] == sel_country_coh].copy().sort_values('cohort_week')

            if cdf.empty:
                st.info("No cohort data for the selected country.")
            else:
                window_labels = {
                    'day_01': 'Day 1', 'day_01_07': 'Day 1–7', 'day_01_14': 'Day 1–14',
                    'day_01_21': 'Day 1–21', 'day_01_28': 'Day 1–28',
                }
                fmt_by_metric = {
                    'orders': '{:,.0f}', 'purchasers': '{:,.0f}', 'deals_all': '{:,.0f}',
                    'UV': '{:,.0f}',
                    'cvr': '{:.2f}%',
                    'nob': '${:,.0f}', 'gross_bookings': '${:,.0f}', 'm1_vfm': '${:,.0f}',
                    'm1_vfm_per_uv': '${:,.4f}', 'aov': '${:,.2f}', 'avg_m1_vfm': '${:,.2f}',
                }

                def compute_metric(row, mkey, win):
                    if mkey == 'UV':
                        return row['UV']
                    uv = row['UV']
                    if mkey == 'cvr':
                        p = row[f'{win}_purchasers']
                        return (p / uv * 100) if uv > 0 else None
                    if mkey == 'aov':
                        o = row[f'{win}_orders']
                        return (row[f'{win}_nob'] / o) if o > 0 else None
                    if mkey == 'm1_vfm_per_uv':
                        return (row[f'{win}_m1_vfm'] / uv) if uv > 0 else None
                    if mkey == 'avg_m1_vfm':
                        o = row[f'{win}_orders']
                        return (row[f'{win}_m1_vfm'] / o) if o > 0 else None
                    return row[f'{win}_{mkey}']

                # --- Single formatted table for the selected metric (rows = cohort week, cols = windows) ---
                mkey = metric_options[sel_metric_label]
                rows = []
                for _, row in cdf.iterrows():
                    cw = row['cohort_week']
                    _, iso_w, _ = cw.isocalendar()
                    r = {'Cohort Week': f"W{iso_w} — {cw.strftime('%Y-%m-%d')}"}
                    if mkey == 'UV':
                        r['New UVs'] = row['UV']
                    else:
                        for w in COHORT_WINDOWS:
                            r[window_labels[w]] = compute_metric(row, mkey, w)
                    rows.append(r)
                tbl = pd.DataFrame(rows)

                st.markdown(f"#### {sel_country_coh} — {sel_metric_label}")
                value_cols = [c for c in tbl.columns if c != 'Cohort Week']
                fmt_str = fmt_by_metric.get(mkey, '{:,.2f}')
                fmt_map = {c: (lambda v, f=fmt_str: '' if pd.isna(v) else f.format(v)) for c in value_cols}
                try:
                    styled = (tbl.style
                              .format(fmt_map)
                              .background_gradient(cmap='RdYlGn', subset=value_cols, axis=None))
                    st.dataframe(styled, use_container_width=True, hide_index=True)
                except Exception:
                    st.dataframe(tbl, use_container_width=True, hide_index=True)

                # --- Line chart of the same metric across cohort weeks, one line per window ---
                pkey = mkey
                primary_label = sel_metric_label
                st.markdown("---")
                st.markdown(f"### Trend across cohort weeks")
                chart_rows = []
                for _, row in cdf.iterrows():
                    for w in COHORT_WINDOWS:
                        if pkey == 'UV' and w != 'day_01':
                            continue
                        v = compute_metric(row, pkey, w)
                        if v is None:
                            continue
                        chart_rows.append({
                            'Cohort Week': row['cohort_week'],
                            'Window': window_labels[w],
                            'Value': v,
                        })
                if chart_rows:
                    chart_df = pd.DataFrame(chart_rows)
                    fig_coh = px.line(
                        chart_df, x='Cohort Week', y='Value', color='Window', markers=True,
                        labels={'Value': primary_label},
                        color_discrete_map={
                            'Day 1': '#1f77b4', 'Day 1–7': '#2196F3', 'Day 1–14': '#FF9800',
                            'Day 1–21': '#9C27B0', 'Day 1–28': '#2ecc71',
                        },
                        title=f"{sel_country_coh} — {primary_label}",
                    )
                    fig_coh.update_layout(height=380, margin=dict(l=0, r=0, t=40, b=10))
                    st.plotly_chart(fig_coh, use_container_width=True)

                # --- Raw data + CSV ---
                with st.expander("📋 Raw cohort data"):
                    raw = cdf.copy()
                    raw['cohort_week'] = raw['cohort_week'].dt.strftime('%Y-%m-%d')
                    st.dataframe(raw, use_container_width=True, hide_index=True)
                    st.download_button(
                        "📥 Download CSV", raw.to_csv(index=False),
                        f"cohort_{sel_country_coh}.csv", "text/csv", key="coh_dl"
                    )


# =============================================================================
# TAB 2 — ASK KAI
# =============================================================================
with tab_kai:
    st.markdown("### 🤖 Ask Kai — Your INTL Markets Analyst")
    st.markdown("Kai has access to all three data sources. Ask strategic questions and get data-backed answers.")

    if not KAI_AVAILABLE:
        st.warning("Kai AI is not available in this environment (kai_client module not found).")
        st.stop()

    with st.expander("🔧 Debug Info", expanded=False):
        st.write(f"**Token available:** {'Yes ✓' if STORAGE_API_TOKEN else 'No ✗'}")
        st.write(f"**API URL:** {STORAGE_API_URL or 'Not set'}")
        if st.session_state.kai_chat_id:
            st.write(f"**Chat ID:** `{st.session_state.kai_chat_id[:8]}...`")

    if not STORAGE_API_TOKEN:
        st.error("""
        **Storage API Token not configured.**

        1. Go to Keboola → Settings → API Tokens → Create New Token (Full Access)
        2. Go to this Data App Configuration → Secrets → set `STORAGE_API_TOKEN`
        3. Redeploy the app
        """)
    else:
        # Suggested PO-level questions — kept narrow to the YoY + Cohort scope of the current dashboard
        st.markdown("**Strategic questions for the Product Owner:**")
        suggestions = [
            "Which markets show improving YoY weekly M1 VFM per UV in 2026 vs 2025, and which are declining?",
            "How does the latest cohort week's Day 1–28 M1 VFM per UV compare to the average of the previous 8 cohort weeks, per market?",
            "Rank markets by their most recent cohort week's CVR in the Day 1–7 window — which markets convert new users fastest?",
            "For each market, how much extra M1 VFM comes from Day 1–28 vs Day 1–7 — which markets monetize new users beyond the first week?",
        ]

        s_cols = st.columns(2)
        for i, s in enumerate(suggestions):
            with s_cols[i % 2]:
                if st.button(s, key=f"sug_{i}", use_container_width=True):
                    st.session_state.pending_prompt = s
                    st.rerun()

        st.markdown("---")

        for msg in st.session_state.kai_messages:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"])

        if st.session_state.pending_approval:
            approval = st.session_state.pending_approval
            st.warning("**A tool requires your approval to proceed.**")
            col1, col2 = st.columns(2)
            with col1:
                if st.button("✅ Approve", type="primary", use_container_width=True):
                    st.session_state.pending_approval = None
                    with st.chat_message("assistant"):
                        container = st.container()
                        result = run_async(collect_approval_response(
                            st.session_state.kai_chat_id, approval["approval_id"], True, container
                        ))
                    st.session_state.kai_messages.append({"role": "assistant", "content": result})
                    st.rerun()
            with col2:
                if st.button("❌ Deny", use_container_width=True):
                    st.session_state.pending_approval = None
                    with st.chat_message("assistant"):
                        container = st.container()
                        result = run_async(collect_approval_response(
                            st.session_state.kai_chat_id, approval["approval_id"], False, container
                        ))
                    if result:
                        st.session_state.kai_messages.append({"role": "assistant", "content": result})
                    st.rerun()

        prompt = st.chat_input("Ask Kai about INTL market performance...")

        if st.session_state.pending_prompt:
            prompt = st.session_state.pending_prompt
            st.session_state.pending_prompt = None

        if prompt:
            st.session_state.kai_messages.append({"role": "user", "content": prompt})
            with st.chat_message("user"):
                st.markdown(prompt)
            with st.chat_message("assistant"):
                container = st.container()
                result_text, pending = run_async(collect_chat_response(
                    st.session_state.kai_chat_id, CONTEXT_STRING + prompt, container
                ))
            st.session_state.kai_messages.append({"role": "assistant", "content": result_text})
            if pending:
                st.session_state.pending_approval = pending
            st.rerun()


# =============================================================================
# TAB 3 — DOCS
# =============================================================================
with tab_docs:
    st.markdown("## 📚 INTL Markets Analytics — Documentation")
    st.caption("How the dashboard works, what it measures, and what it deliberately does not.")

    st.markdown("---")

    st.markdown("### ⚠️ Important: no MBNXT vs Legacy comparison on INTL")
    st.markdown(
        """
The INTL ramp-up does **not** use local bucketing, so there is **no clean MBNXT-vs-Legacy comparison
available** on INTL traffic.

- A single-version filter ("show me MBNXT users' trend over time") is fine — it's a directional read of one cohort.
- A **side-by-side comparison** ("MBNXT users had X% higher CVR than Legacy users") is **not defensible** — it suffers
  from *second-launch bias*: a user bucketed as MBNXT can still purchase on the Legacy app on their first session,
  so purchases get attributed to the wrong bucket.

This was a deliberate decision for the INTL ramp, not a tracking gap. Per-batch measurement is **before-vs-after
trend monitoring**, not MBNXT-vs-Legacy.
"""
    )

    st.markdown("---")

    st.markdown("### 📈 YoY Trends tab")
    st.markdown(
        """
**Purpose.** Weekly Year-over-Year and Week-over-Week trends per country, mirroring Radomír's NA YoY sheet.

**Inline filters.**

- **Country** — single select (default GB).
- **Groupon Version** — All / legacy / mbnxt (single-select). Side-by-side comparison is invalid (see above);
  use this to scope a trend to one version at a time.
- **OS** — multiselect (default iOS). iOS + Android can be combined.

**Tables.** Three stacked tables (M1 VFM, M1 VFM per UV, Distinct UVs). Columns:

| Column | Meaning |
|---|---|
| ISO Week | ISO week number (1–53) |
| 2024 / 2025 / 2026 | Metric value for that ISO week × year |
| {year} YoY % | Year-over-year change vs the prior year |
| WoW % | Week-over-week change within the latest year |

Color scale: red → yellow → green from −50% to +50% (clipped at ±50% to keep outliers from washing out the rest).

**Data sources.**

- `kbc-grpn-40-0cd2.out_c_intl_app_yoy_trends.weekly_yoy_INTL_app` — weekly aggregates from `unit_economics`,
  filtered to INTL countries, `event_platform LIKE 'app%'`, `user_brand_affiliation = 'groupon'`,
  `event_type IN ('authorize', 'capture')`, `last_status <> 'cancel'`.
- `kbc-grpn-40-0cd2.out_c_intl_app_yoy_trends.weekly_uv_INTL_app` — weekly distinct bcookies from
  `junoHourly_analytics`, filtered to mobile + INTL + groupon brand + bot-excluded.
"""
    )

    st.markdown("---")

    st.markdown("### 🔄 Cohort Analysis tab")
    st.markdown(
        """
**Purpose.** New-user cohorts by first-visit week. Mirrors the NA Android Local-Bucketing report layout,
without the groupon_version split (see "no MBNXT vs Legacy" note above).

**Cohort definition.** A "cohort" is the set of bcookies whose `first_event_date` falls in a given ISO week.
Source: `out_c_00_new_bcookie_identifier.bcookie_first_event_date`.

**Inline filters.**

- **Country** — single select (default GB).
- **Metric** — single select (drives the entire table view).

**Time windows.** The table has five **cumulative** windows per row:

| Window | Days included | What it captures |
|---|---|---|
| Day 1 | day 0 only | Performance on first day of acquisition |
| Day 1–7 | days 0–7 | First week activity (cumulative) |
| Day 1–14 | days 0–14 | First two weeks (cumulative) |
| Day 1–21 | days 0–21 | First three weeks (cumulative) |
| Day 1–28 | days 0–28 | First four weeks (cumulative) — the full 28-day cohort window |

**Available metrics.** Direct from the cohort table (per window):

| Metric | Definition |
|---|---|
| Orders | Distinct `parent_order_uuid` count, event_type ∈ {authorize, capture}, last_status ≠ cancel |
| Purchasers | Distinct bcookies with at least one order in the window |
| NOB | Net Order Bookings (USD) — sum of `nob_loc × fx_rate_loc_to_usd_fxn` |
| Gross Bookings | `gross_bookings_operational × fx_rate_loc_to_usd_fxn` |
| M1 VFM | `(margin_1_operational + vfm_operational) × fx_rate_loc_to_usd_fxn` |
| Deals (Quantity) | Sum of `per_unit_quantity` across all orders in the window |
| ILS / OD Applied | Item-Level / Order-level discount totals (negative values; display as absolute) |

**Derived metrics** (computed in the dashboard, not in SQL):

| Metric | Formula |
|---|---|
| CVR (%) | `purchasers / UV × 100` |
| AOV (USD) | `NOB / orders` (volume-weighted average order value) |
| M1 VFM / UV | `M1 VFM / UV` |
| Avg M1 VFM / order | `M1 VFM / orders` |

**Plot below the table.** Line chart of the selected metric × cohort_week, one line per window.

**Data source.** `kbc-grpn-40-0cd2.out_c_testing_data_apps.cohort_INTL_app`. Schema mirrors the Android
local-bucketing pattern (5 windows × 13 base metrics + UV at day_01).
"""
    )

    st.markdown("---")

    st.markdown("### 🤖 Ask Kai tab")
    st.markdown(
        """
AI assistant powered by Keboola Kai. Has access to this data app's source code and underlying tables;
ask analytical questions in natural language. Approves tool calls before executing them.
"""
    )

    st.markdown("---")

    st.markdown("### Data refresh + retention")
    st.markdown(
        """
- **Cohort transformation** (`Data apps - INTL app dashboard`) — daily run (once scheduled). Each run
  recomputes the trailing 26 weeks of cohorts and upserts on `(country, cohort_week)`. Older history is
  preserved indefinitely from the backfill (currently 2025-04-14 → 2026-04-13).
- **YoY transformation** (`INTL app YoY trends`) — backfilled through 2026-05-10; daily incremental refresh
  rebuilds the trailing week and rolls forward.
- **Dashboard cache** — Streamlit caches every loader for **5 minutes** (`@st.cache_data(ttl=300)`). To force
  a fresh pull, use the "Clear cache" menu or wait 5 min.
"""
    )

    st.markdown("---")

    st.markdown("### Known gotchas")
    st.markdown(
        """
- **Most recent cohort week is partial.** The SQL bounds `cohort_end = today - 29 days` to ensure 28+ days
  of post-cohort observation. The newest week shown may still be under-counted relative to older weeks if
  fewer than 7 full days have passed.
- **US and Quebec excluded.** All INTL queries filter out `country IN ('US', 'QC')`. Rest of Canada is INTL.
- **Mobile-app only.** Cohort + YoY both filter to `event_platform LIKE 'app%'` (UE) and `platform = 'mobile'` (juno).
  Web traffic is not in scope.
- **Groupon brand only.** `LOWER(user_brand_affiliation) = 'groupon'` — excludes other brands.
- **Bot filter.** `isBot = 'false' OR isBot IS NULL` on juno side; bots otherwise inflate UV counts.
- **`country = 'UK'` in juno → mapped to `'GB'`** for consistency with `feature_country` in unit_economics.
"""
    )

    st.markdown("---")

    st.markdown("### Contact")
    st.markdown(
        """
- BI analyst: **Daniel Herzog** (dherzog@groupon.com)
- BI owner: **Pavel Cernik**
- BI approver: **Michal Zachar**
- Program manager: **Bogdana Boncheva**

Repo: `github.com:dherzog-svg/Keboola_app_data`. Push to `main` → manual redeploy in Keboola UI.
"""
    )
