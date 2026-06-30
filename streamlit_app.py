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
    if 'first_groupon_version' not in df.columns:
        df['first_groupon_version'] = 'legacy'
    df['first_groupon_version'] = df['first_groupon_version'].fillna('legacy')
    df['cohort_week'] = pd.to_datetime(df['cohort_week'])
    df = (df.sort_values('UV', ascending=False)
            .drop_duplicates(subset=['country', 'cohort_week', 'first_groupon_version', 'operating_system'], keep='first')
            .sort_values(['country', 'cohort_week', 'first_groupon_version'])
            .reset_index(drop=True))
    return df

@st.cache_data(ttl=300)
def load_yoy_data():
    df = query_data('''
    SELECT
      `order_created_date`, `iso_year`, `iso_week`, `country_code`, `operating_system`,
      SUM(`m1_vfm`) AS m1_vfm, SUM(`orders`) AS orders
    FROM `kbc-grpn-40-0cd2`.`out_c_intl_app_yoy_trends`.`daily_yoy_INTL_app`
    GROUP BY 1, 2, 3, 4, 5
    ''')
    for col in ['iso_year', 'iso_week', 'm1_vfm', 'orders']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    df['order_created_date'] = pd.to_datetime(df['order_created_date'])
    return df

@st.cache_data(ttl=300)
def load_yoy_uv_data():
    df = query_data('''
    SELECT
      `event_date`, `iso_year`, `iso_week`, `country_code`, `operating_system`,
      SUM(`daily_distinct_bcookies`) AS daily_uvs
    FROM `kbc-grpn-40-0cd2`.`out_c_intl_app_yoy_trends`.`daily_uv_INTL_app`
    GROUP BY 1, 2, 3, 4, 5
    ''')
    df['daily_uvs'] = pd.to_numeric(df.get('daily_uvs', 0), errors='coerce').fillna(0)
    for col in ['iso_year', 'iso_week']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    df['event_date'] = pd.to_datetime(df['event_date'])
    return df


@st.cache_data(ttl=300)
def load_uv_split_data():
    df = query_data('''
    SELECT
      `event_date`, `country_code`, `operating_system`, `groupon_version`,
      SUM(`uv`) AS uv
    FROM `kbc-grpn-40-0cd2`.`out_c_testing_data_apps`.`daily_uv_split`
    GROUP BY 1, 2, 3, 4
    ''')
    df['uv'] = pd.to_numeric(df.get('uv', 0), errors='coerce').fillna(0)
    df['event_date'] = pd.to_datetime(df['event_date'])
    return df


@st.cache_data(ttl=300)
def load_appversion_data():
    df = query_data('''
    SELECT
      `event_date`, `country_code`, `operating_system`, `app_version`,
      SUM(`uv`) AS uv
    FROM `kbc-grpn-40-0cd2`.`out_c_testing_data_apps`.`daily_appversion_split`
    WHERE `groupon_version` = 'mbnxt'
    GROUP BY 1, 2, 3, 4
    ''')
    df['uv'] = pd.to_numeric(df.get('uv', 0), errors='coerce').fillna(0)
    df['event_date'] = pd.to_datetime(df['event_date'])
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

try:
    uv_split_raw = load_uv_split_data()
except Exception:
    uv_split_raw = pd.DataFrame()

try:
    appver_raw = load_appversion_data()
except Exception:
    appver_raw = pd.DataFrame()


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
    default_country = 'GB' if 'GB' in all_countries else (all_countries[0] if all_countries else None)
    st.markdown('<p class="section-label">Country</p>', unsafe_allow_html=True)
    selected_country = st.selectbox(
        "Country", all_countries,
        index=all_countries.index(default_country) if default_country else 0,
        label_visibility="collapsed", key="global_country",
    )

    # Client Platform filter (iOS / Android) — shared across YoY + Cohort tabs
    os_pool = set()
    if not yoy_df_raw.empty and 'operating_system' in yoy_df_raw.columns:
        os_pool |= set(yoy_df_raw['operating_system'].dropna().unique())
    if not coh_df_raw.empty and 'operating_system' in coh_df_raw.columns:
        os_pool |= set(coh_df_raw['operating_system'].dropna().unique())
    os_opts_all = [o for o in ['iOS', 'Android'] if o in os_pool] or sorted(os_pool) or ['iOS']
    default_os_sel = ['iOS'] if 'iOS' in os_opts_all else os_opts_all
    st.markdown('<p class="section-label">Client Platform</p>', unsafe_allow_html=True)
    selected_os = st.multiselect(
        "Client Platform", os_opts_all, default=default_os_sel,
        label_visibility="collapsed", key="global_os",
    )
    if not selected_os:
        selected_os = os_opts_all

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
st.markdown("Sidebar holds the global Country + Client Platform filters — applied to both tabs. The Metric selector lives inline next to each table.")

def render_split_section(scope_df, label):
    """MBNXT vs Legacy share of app UV (from daily_uv_split)."""
    if scope_df.empty:
        st.info(f"No traffic-split data for {label}.")
        return
    daily = scope_df.groupby(['event_date', 'groupon_version'], as_index=False)['uv'].sum()
    pivot = daily.pivot(index='event_date', columns='groupon_version', values='uv').fillna(0.0)
    for v in ['legacy', 'mbnxt']:
        if v not in pivot.columns:
            pivot[v] = 0.0
    pivot = pivot.sort_index()
    total = (pivot['legacy'] + pivot['mbnxt']).replace(0, np.nan)
    pivot['legacy %'] = pivot['legacy'] / total * 100
    pivot['mbnxt %'] = pivot['mbnxt'] / total * 100

    cdf = pivot.reset_index().melt(
        id_vars='event_date', value_vars=['legacy %', 'mbnxt %'],
        var_name='Treatment', value_name='pct'
    )
    cdf['Treatment'] = cdf['Treatment'].str.replace(' %', '', regex=False)
    fig = px.line(
        cdf.dropna(subset=['pct']), x='event_date', y='pct', color='Treatment', markers=True,
        color_discrete_map={'legacy': '#1f77b4', 'mbnxt': '#ff7f0e'},
        labels={'pct': '% of Total UV', 'event_date': 'Day of Event Date'},
    )
    fig.update_layout(
        height=340, margin=dict(l=0, r=0, t=10, b=0),
        yaxis=dict(range=[0, 100], ticksuffix='%'), legend_title_text='Treatment',
    )
    st.plotly_chart(fig, use_container_width=True)

    tbl = pivot.reset_index()[['event_date', 'legacy', 'mbnxt', 'mbnxt %']].sort_values(
        'event_date', ascending=False
    ).copy()
    tbl['event_date'] = tbl['event_date'].dt.strftime('%Y-%m-%d')
    tbl = tbl.rename(columns={'event_date': 'Day of Event Date'})
    styled = tbl.style.format({'legacy': '{:,.0f}', 'mbnxt': '{:,.0f}', 'mbnxt %': '{:.2f}%'})
    st.dataframe(styled, use_container_width=True, hide_index=True, height=320)


def render_appversion_section(scope_df, label, selected_versions=None, min_day_uv=30):
    """100%-stacked area of MBNXT UV share by app_version over time (from daily_appversion_split).
    Every build is shown individually (no 'Other'); the date×version grid is densified (missing → 0)
    so the stack fills cleanly to 100% with no gaps, and versions stack in first-appearance order.
    Days with < min_day_uv total MBNXT UV are hidden (pre-ramp INTL = a handful of test/QA accounts)."""
    if scope_df is None or scope_df.empty:
        st.info(f"No app-version data for {label} yet.")
        return
    daily = scope_df.groupby(['event_date', 'app_version'], as_index=False)['uv'].sum()
    day_tot = daily.groupby('event_date')['uv'].transform('sum')
    # Drop low-volume days — pre-ramp INTL markets are only test/employee accounts (median ~1 UV/day),
    # which the % normalisation would otherwise blow up to 100%.
    daily = daily[day_tot >= min_day_uv].copy()
    if daily.empty:
        st.info(f"No day in this range reaches ≥{min_day_uv} MBNXT UV for {label} — "
                f"pre-ramp INTL markets carry only test/QA accounts.")
        return
    # Normalise against each day's FULL MBNXT UV (all versions) so a subset keeps its true share.
    day_tot = daily.groupby('event_date')['uv'].transform('sum')
    daily['pct'] = daily['uv'] / day_tot.replace(0, np.nan) * 100
    daily = daily.rename(columns={'app_version': 'version'})

    if selected_versions:
        plot = daily[daily['version'].isin(selected_versions)].copy()
        if plot.empty:
            st.info("No data for the selected app version(s) in this range.")
            return
        note = f"{len(selected_versions)} build(s) selected · each as % of the day's MBNXT UV · days <{min_day_uv} UV hidden."
    else:
        plot = daily.copy()
        note = f"All builds · days <{min_day_uv} MBNXT UV hidden."

    # Stack versions in first-appearance order (newer builds ride on top → migration-wave look).
    appear = daily.groupby('version')['event_date'].min().sort_values()
    order = [v for v in appear.index.tolist() if v in set(plot['version'])]
    # Densify: every date × version present, missing filled with 0, so px.area stacks continuously to 100%.
    dates = sorted(plot['event_date'].unique())
    full_idx = pd.MultiIndex.from_product([dates, order], names=['event_date', 'version'])
    plot = (plot.set_index(['event_date', 'version'])[['uv', 'pct']]
                .reindex(full_idx, fill_value=0).reset_index())

    fig = px.area(
        plot, x='event_date', y='pct', color='version',
        category_orders={'version': order},
        labels={'pct': '% of Total UV', 'event_date': 'Day of Event Date', 'version': 'App Version'},
    )
    fig.update_layout(
        height=460, margin=dict(l=0, r=0, t=10, b=0),
        yaxis=dict(range=[0, 100], ticksuffix='%'), legend_title_text='App Version',
    )
    st.plotly_chart(fig, use_container_width=True)

    latest = plot['event_date'].max()
    lt = plot[(plot['event_date'] == latest) & (plot['uv'] > 0)].sort_values('uv', ascending=False)[['version', 'uv', 'pct']].copy()
    lt = lt.rename(columns={'version': 'App Version', 'uv': 'UV', 'pct': '% of day'})
    st.caption(f"Latest day: {latest.strftime('%Y-%m-%d')} · {note}")
    st.dataframe(
        lt.style.format({'UV': '{:,.0f}', '% of day': '{:.1f}%'}),
        use_container_width=True, hide_index=True, height=300,
    )


def render_date_range(scope_dfs, key):
    """Inline date-range picker spanning the given dataframes. Returns (start_ts, end_ts) Timestamps,
    or (None, None) when no data is available."""
    series = [d['event_date'] for d in scope_dfs if d is not None and not d.empty]
    if not series:
        return None, None
    alld = pd.concat(series)
    dmin, dmax = alld.min().date(), alld.max().date()
    if dmin == dmax:
        st.caption(f"Date: {dmin:%Y-%m-%d} (single day available)")
        return pd.Timestamp(dmin), pd.Timestamp(dmax)
    sel = st.date_input(
        "Date range", value=(dmin, dmax), min_value=dmin, max_value=dmax, key=key,
    )
    if isinstance(sel, (tuple, list)) and len(sel) == 2:
        return pd.Timestamp(sel[0]), pd.Timestamp(sel[1])
    return pd.Timestamp(dmin), pd.Timestamp(dmax)


tab_yoy, tab_cohort, tab_split, tab_split_us, tab_kai, tab_docs = st.tabs([
    "📈 YoY Trends",
    "🔄 Cohort Analysis",
    "🔀 Traffic Split",
    "🇺🇸 Traffic Split — US",
    "🤖 Ask Kai",
    "📚 Docs"
])


# =============================================================================
# TAB 0 — YoY TRENDS
# =============================================================================
with tab_yoy:
    st.caption("Daily data by ISO week + weekday. Rows = (Week, Day-of-week), columns = per-year values with YoY%, DoD%, and WoW%.")

    if yoy_df_raw.empty or yoy_uv_df_raw.empty:
        st.warning("Daily YoY tables not yet available — run the **INTL app YoY trends** transformation first (outputs: `daily_yoy_INTL_app`, `daily_uv_INTL_app`).")
    else:
        sel_country = selected_country
        sel_os = selected_os

        if not sel_os:
            st.warning("Pick at least one Client Platform in the sidebar.")
            st.stop()

        fin = yoy_df_raw[
            (yoy_df_raw['country_code'] == sel_country) &
            (yoy_df_raw['operating_system'].isin(sel_os))
        ].copy()
        uv = yoy_uv_df_raw[
            (yoy_uv_df_raw['country_code'] == sel_country) &
            (yoy_uv_df_raw['operating_system'].isin(sel_os))
        ].copy()

        if fin.empty:
            st.warning(f"No data for {sel_country} / {'+'.join(sel_os)}.")
        else:
            fin_d = fin.groupby(['order_created_date', 'iso_year', 'iso_week'], as_index=False).agg(
                m1_vfm=('m1_vfm', 'sum'),
                orders=('orders', 'sum'),
            )
            uv_d = uv.groupby(['event_date', 'iso_year', 'iso_week'], as_index=False).agg(
                daily_uvs=('daily_uvs', 'sum'),
            ).rename(columns={'event_date': 'order_created_date'})

            # M1/UV only for days where both tables have data (inner join)
            muv = fin_d.merge(
                uv_d[['order_created_date', 'iso_year', 'iso_week', 'daily_uvs']],
                on=['order_created_date', 'iso_year', 'iso_week'],
                how='inner'
            )
            muv['m1_vfm_per_uv'] = muv['m1_vfm'] / muv['daily_uvs'].replace(0, np.nan)

            # --- Quick summary: latest available day, all 3 metrics in one place ---
            # Same ISO-week × weekday logic as the tables below, so deltas match exactly.
            def _latest_summary(df, metric_col):
                import datetime as _dt
                d = df[['iso_year', 'iso_week', 'order_created_date', metric_col]].dropna(subset=[metric_col]).copy()
                d = d[d[metric_col] > 0]
                if d.empty:
                    return None
                d['sort_val'] = d['iso_week'].astype(int) * 10 + d['order_created_date'].dt.dayofweek
                years = sorted(d['iso_year'].astype(int).unique())
                latest_year = years[-1]
                cur = d[d['iso_year'] == latest_year].groupby('sort_val')[metric_col].sum().sort_index()
                if cur.empty:
                    return None
                sv = int(cur.index[-1])
                val = float(cur.iloc[-1])
                try:
                    when = _dt.date.fromisocalendar(latest_year, sv // 10, sv % 10 + 1)
                except Exception:
                    when = d[d['sort_val'] == sv]['order_created_date'].max().date()
                dod = (val / cur.iloc[-2] - 1) * 100 if len(cur) >= 2 and cur.iloc[-2] else None
                wow_prev = cur.get(sv - 10)
                wow = (val / wow_prev - 1) * 100 if wow_prev else None
                yoy = None
                if (latest_year - 1) in years:
                    prior = d[d['iso_year'] == latest_year - 1].groupby('sort_val')[metric_col].sum()
                    pv = prior.get(sv)
                    yoy = (val / pv - 1) * 100 if pv else None
                return {'date': when, 'value': val, 'yoy': yoy, 'dod': dod, 'wow': wow}

            _summ = [
                ('💰 M1 VFM',        _latest_summary(fin_d, 'm1_vfm'),        "${:,.0f}"),
                ('💵 M1 VFM per UV', _latest_summary(muv,   'm1_vfm_per_uv'), "${:,.2f}"),
                ('👥 Distinct UVs',  _latest_summary(uv_d,  'daily_uvs'),     "{:,.0f}"),
            ]
            # Delta pill styled to match Streamlit's native metric delta (red down / green up).
            def _pill(v, suffix):
                if v is None:
                    return (f'<span style="display:inline-block;background:#f1f3f5;color:#868e96;'
                            f'border-radius:8px;padding:1px 9px;font-size:0.82rem;font-weight:600;">'
                            f'n/a {suffix}</span>')
                up = v >= 0
                arrow = '↑' if up else '↓'
                color = '#1a9e4b' if up else '#d6453f'
                bg = 'rgba(33,195,84,0.12)' if up else 'rgba(255,75,75,0.12)'
                return (f'<span style="display:inline-block;background:{bg};color:{color};'
                        f'border-radius:8px;padding:1px 9px;font-size:0.82rem;font-weight:600;">'
                        f'{arrow} {v:+.1f}% {suffix}</span>')

            st.markdown(f"#### 📌 Latest day at a glance — {sel_country} / {'+'.join(sel_os)}")
            _scols = st.columns(3)
            for _box, (_lbl, _s, _fmt) in zip(_scols, _summ):
                with _box:
                    if not _s:
                        st.metric(_lbl, "—")
                        continue
                    st.metric(_lbl, _fmt.format(_s['value']))
                    _pills = ''.join(
                        f'<div style="margin-bottom:5px;">{_pill(_v, _sfx)}</div>'
                        for _v, _sfx in [(_s['yoy'], 'YoY'), (_s['wow'], 'WoW'), (_s['dod'], 'DoD')]
                    )
                    st.markdown(_pills, unsafe_allow_html=True)
                    st.caption(_s['date'].strftime('%a %b %d'))
            st.caption("Deltas: YoY (same ISO week + weekday, prior year) · WoW (prior week, same weekday) · DoD (prior day). Full history in the tables below.")
            st.markdown("---")

            def render_yoy_table_daily(df, metric_col, metric_label, fmt_fn):
                import datetime as _dt
                df = df[['order_created_date', 'iso_year', 'iso_week', metric_col]].copy()
                df['weekday_num'] = df['order_created_date'].dt.dayofweek
                df['weekday_name'] = df['order_created_date'].dt.strftime('%A')
                df['sort_val'] = df['iso_week'] * 10 + df['weekday_num']
                agg = df.groupby(['sort_val', 'iso_year', 'iso_week', 'weekday_name'], as_index=False)[metric_col].sum()
                pivot = agg.pivot_table(
                    index='sort_val', columns='iso_year', values=metric_col, aggfunc='sum'
                ).sort_index()
                meta = agg.drop_duplicates('sort_val').set_index('sort_val')[['iso_week', 'weekday_name']]
                years = sorted([c for c in pivot.columns if isinstance(c, (int, float, np.integer))])
                # Compute date mathematically for every row so future weeks aren't blank
                latest_year = int(years[-1]) if years else pd.Timestamp.now().year
                def _iso_date(sv):
                    try:
                        return _dt.date.fromisocalendar(latest_year, int(sv) // 10, int(sv) % 10 + 1).strftime('%b %d')
                    except Exception:
                        return ''
                for i in range(1, len(years)):
                    prior = pivot[years[i - 1]].replace(0, np.nan)
                    pivot[f"{int(years[i])} YoY %"] = (pivot[years[i]] / prior - 1) * 100
                if years:
                    pivot['DoD %'] = pivot[years[-1]].pct_change(fill_method=None) * 100
                    prev_sv = pivot.index - 10
                    wow_prior = pivot[years[-1]].reindex(prev_sv).values
                    pivot['WoW %'] = (
                        pivot[years[-1]].values
                        / np.where(wow_prior == 0, np.nan, wow_prior)
                        - 1
                    ) * 100
                pivot = pivot.replace([np.inf, -np.inf], np.nan)
                pivot = pivot.sort_index(ascending=True)
                show = pivot.reset_index()
                show.insert(1, 'Date',        show['sort_val'].apply(_iso_date))
                show.insert(2, 'Day of Week', show['sort_val'].map(meta['weekday_name']))
                show.insert(3, 'ISO Week',    show['sort_val'].map(meta['iso_week']))
                show = show.drop(columns=['sort_val'])
                pct_cols = [c for c in show.columns if isinstance(c, str) and '%' in c]
                year_cols = [c for c in show.columns if c in years]

                def _pct_bg(v, cap=50.0):
                    if pd.isna(v):
                        return ''
                    intensity = min(abs(v), cap) / cap
                    if v >= 0:
                        r, g, b = int(255 - intensity * 127), int(255 - intensity * 56), int(255 - intensity * 127)
                    else:
                        r, g, b = int(255 - intensity * 23), int(255 - intensity * 127), int(255 - intensity * 127)
                    return f'background-color: rgb({r},{g},{b})'

                fmt_map = {'Week-Day': '{}'}
                for c in year_cols:
                    fmt_map[c] = lambda v, ff=fmt_fn: '' if pd.isna(v) else (ff % v)
                for c in pct_cols:
                    fmt_map[c] = lambda v: '' if pd.isna(v) else f'{v:+.1f}%'
                styled = show.style.format(fmt_map).map(_pct_bg, subset=pct_cols)
                st.markdown(f'### {metric_label}')
                st.dataframe(styled, use_container_width=True, hide_index=True)

            render_yoy_table_daily(fin_d, 'm1_vfm',        '💰 M1 VFM',        "$%.0f")
            render_yoy_table_daily(muv,   'm1_vfm_per_uv', '💵 M1 VFM per UV', "$%.2f")
            render_yoy_table_daily(uv_d,  'daily_uvs',     '👥 Distinct UVs',  "%.0f")

            st.markdown("---")
            st.markdown("### Trend chart")
            metric_choice = st.radio(
                "Metric", ["M1 VFM", "M1 VFM per UV", "Distinct UVs"], horizontal=True, key="yoy_metric_chart"
            )
            metric_map = {"M1 VFM": ("m1_vfm", fin_d), "M1 VFM per UV": ("m1_vfm_per_uv", muv), "Distinct UVs": ("daily_uvs", uv_d)}
            mc, chart_src = metric_map[metric_choice]
            chart_df = chart_src[['iso_year', 'iso_week', 'order_created_date', mc]].dropna(subset=[mc]).copy()
            chart_df = chart_df[chart_df[mc] > 0].copy()
            chart_df['iso_year_str'] = chart_df['iso_year'].astype(int).astype(str)
            # ISO-aligned day-of-year: ISO-week-1 dates that fall in late December belong to the next
            # iso_year — using calendar dayofyear put them at ~day 365 and drew a straight line across.
            chart_df['day_of_year'] = (chart_df['iso_week'].astype(int) - 1) * 7 + (chart_df['order_created_date'].dt.dayofweek + 1)
            fig = px.line(
                chart_df, x='day_of_year', y=mc, color='iso_year_str',
                markers=False,
                labels={'day_of_year': 'Day of Year', mc: metric_choice, 'iso_year_str': 'Year'},
                title=f"{metric_choice} — {sel_country} / {'+'.join(sel_os)}"
            )
            fig.update_layout(height=380, margin=dict(l=0, r=0, t=40, b=10))
            st.plotly_chart(fig, use_container_width=True)

            st.markdown("---")
            with st.expander("📋 Raw daily data"):
                st.dataframe(fin_d, use_container_width=True, hide_index=True)
                st.download_button(
                    "📥 Download CSV", fin_d.to_csv(index=False),
                    f"daily_yoy_{sel_country}_{'-'.join(sel_os)}.csv", "text/csv", key="yoy_dl"
                )



# =============================================================================
# TAB 1 — COHORT ANALYSIS
# =============================================================================
with tab_cohort:
    st.caption(
        "MBNXT vs Legacy cohorts — ALL active users (new + existing) whose first event on the new "
        "build (appVersion 26.10+) fell in each Start-Day week, split Legacy vs MBNXT with uplift "
        "across cumulative windows (Day 1 / 1–7 / 1–14 / 1–21 / 1–28). Local bucketing assigns ~50:50 within 26.10."
    )

    if coh_df_raw.empty:
        st.warning("No cohort data available.")
    else:
        sel_country_coh = selected_country
        coh_df = coh_df_raw[coh_df_raw['country'] == sel_country_coh].copy()

        # Apply the sidebar Client Platform filter, then collapse OS into one row per (week, version).
        if 'operating_system' in coh_df.columns:
            coh_df = coh_df[coh_df['operating_system'].isin(selected_os)].copy()
            _sum_cols = [c for c in (['UV'] + [f'{w}_{m}' for w in COHORT_WINDOWS for m in COHORT_BASE_METRICS]) if c in coh_df.columns]
            if not coh_df.empty:
                coh_df = coh_df.groupby(
                    ['country', 'cohort_week', 'first_groupon_version'], as_index=False
                )[_sum_cols].sum()

        if coh_df.empty:
            st.info(f"No cohort data for {sel_country_coh}.")
        else:
            window_labels = {
                'day_01': 'Day 1', 'day_01_07': 'Day 1–7', 'day_01_14': 'Day 1–14',
                'day_01_21': 'Day 1–21', 'day_01_28': 'Day 1–28',
            }
            # Window is only fully observable once today - cohort_week >= window length (+ UE T+1 lag).
            window_min_days = {
                'day_01': 1, 'day_01_07': 7, 'day_01_14': 14, 'day_01_21': 21, 'day_01_28': 28,
            }
            today_ts = pd.Timestamp.today().normalize()

            # Per-window metric shown as Legacy / MBNXT / Uplift. M1 VFM / UV mirrors the NA sheet.
            coh_metric_options = {
                'M1 VFM / UV': ('m1_vfm_per_uv', '${:,.2f}'),
                'M1 VFM':      ('m1_vfm',        '${:,.0f}'),
                'Orders':      ('orders',        '{:,.0f}'),
                'CVR (%)':     ('cvr',           '{:.2f}%'),
                'AOV (USD)':   ('aov',           '${:,.2f}'),
            }
            sel_metric_label = st.selectbox(
                "Per-window metric", list(coh_metric_options.keys()), index=0, key="coh_metric"
            )
            mkey, fmt_str = coh_metric_options[sel_metric_label]

            def cohort_metric(row, mkey, win):
                # Value of `mkey` for one version-row in cumulative window `win`; None if missing / immature.
                if row is None:
                    return None
                if (today_ts - row['cohort_week']).days < window_min_days[win]:
                    return None
                uv = row['UV']
                if mkey == 'm1_vfm_per_uv':
                    return (row[f'{win}_m1_vfm'] / uv) if uv > 0 else None
                if mkey == 'cvr':
                    return (row[f'{win}_purchasers'] / uv * 100) if uv > 0 else None
                if mkey == 'aov':
                    o = row[f'{win}_orders']
                    return (row[f'{win}_nob'] / o) if o > 0 else None
                return row.get(f'{win}_{mkey}')

            # Pivot the two version rows into a two-level header (group row + Legacy/MBNXT/Uplift),
            # matching the NA local-bucketing sheet so sub-labels stay short.
            weeks = sorted(coh_df['cohort_week'].unique(), reverse=True)
            table_rows = []
            for cw in weeks:
                wk = coh_df[coh_df['cohort_week'] == cw]
                leg = wk[wk['first_groupon_version'] == 'legacy']
                mbx = wk[wk['first_groupon_version'] == 'mbnxt']
                leg_row = leg.iloc[0] if not leg.empty else None
                mbx_row = mbx.iloc[0] if not mbx.empty else None
                uv_leg = int(leg_row['UV']) if leg_row is not None else 0
                uv_mbx = int(mbx_row['UV']) if mbx_row is not None else 0
                size = uv_leg + uv_mbx
                _, iso_w, _ = pd.Timestamp(cw).isocalendar()
                r = {
                    ('', 'Start Day'): f"W{iso_w} · {pd.Timestamp(cw).strftime('%Y-%m-%d')}",
                    ('Users in Cohort', 'Legacy'): uv_leg,
                    ('Users in Cohort', 'MBNXT'): uv_mbx,
                    ('Users in Cohort', 'MBNXT %'): (uv_mbx / size * 100) if size > 0 else None,
                }
                for w in COHORT_WINDOWS:
                    grp = window_labels[w]
                    lv = cohort_metric(leg_row, mkey, w)
                    mv = cohort_metric(mbx_row, mkey, w)
                    up = ((mv / lv - 1) * 100) if (lv not in (None, 0) and mv is not None) else None
                    r[(grp, 'Legacy')] = lv
                    r[(grp, 'MBNXT')] = mv
                    r[(grp, 'Uplift')] = up
                table_rows.append(r)

            tbl = pd.DataFrame(table_rows)
            tbl.columns = pd.MultiIndex.from_tuples(tbl.columns)

            st.markdown(f"#### {sel_country_coh} — {sel_metric_label} · Legacy vs MBNXT by cohort")

            pct_col = ('Users in Cohort', 'MBNXT %')
            uv_cols = [('Users in Cohort', 'Legacy'), ('Users in Cohort', 'MBNXT')]
            metric_cols = [c for c in tbl.columns if c[1] in ('Legacy', 'MBNXT') and c[0] != 'Users in Cohort']
            uplift_cols = [c for c in tbl.columns if c[1] == 'Uplift']

            fmt_map = {c: fmt_str for c in metric_cols}
            for c in uplift_cols:
                fmt_map[c] = '{:,.1f}%'
            for c in uv_cols:
                fmt_map[c] = '{:,.0f}'
            fmt_map[pct_col] = '{:,.1f}%'

            # Only gradient columns that actually have values, else all-NaN renders as black cells.
            grad_up = [c for c in uplift_cols if tbl[c].notna().any()]
            grad_pct = [pct_col] if tbl[pct_col].notna().any() else []
            try:
                styler = tbl.style.format(fmt_map, na_rep='')
                if grad_up:
                    styler = styler.background_gradient(cmap='Greens', subset=grad_up, axis=None)
                if grad_pct:
                    styler = styler.background_gradient(cmap='Blues', subset=grad_pct)
                try:
                    styler = styler.highlight_null(color='white')
                except Exception:
                    pass
                st.dataframe(styler, use_container_width=True, hide_index=True)
            except Exception:
                st.dataframe(tbl, use_container_width=True, hide_index=True)

            st.caption(
                "Uplift = MBNXT ÷ Legacy − 1 for the selected metric. Blank = window not yet matured "
                "(today − Start Day < window length) or that version has no users in the cohort. "
                "Cohort entry = first event on the new build (appVersion 26.10+); arm = grouponversion."
            )

            # --- Raw data + CSV ---
            with st.expander("📋 Raw cohort data"):
                raw = coh_df.copy()
                raw['cohort_week'] = raw['cohort_week'].dt.strftime('%Y-%m-%d')
                st.dataframe(raw, use_container_width=True, hide_index=True)
                st.download_button(
                    "📥 Download CSV", raw.to_csv(index=False),
                    f"cohort_{sel_country_coh}.csv", "text/csv", key="coh_dl"
                )


# =============================================================================
# TAB — TRAFFIC SPLIT (MBNXT vs Legacy share of app UV)
# =============================================================================
with tab_split:
    st.caption("INTL app traffic. **Top:** MBNXT vs Legacy share of app UV (from MSA S2). "
               "**Bottom:** MBNXT app-version mix. Sidebar Country + Client Platform filters apply.")
    sel_os_split = selected_os or os_opts_all

    intl_split_all = uv_split_raw[
        (uv_split_raw['country_code'] == selected_country) &
        (uv_split_raw['operating_system'].isin(sel_os_split))
    ] if not uv_split_raw.empty else uv_split_raw
    intl_av_all = appver_raw[
        (appver_raw['country_code'] == selected_country) &
        (appver_raw['operating_system'].isin(sel_os_split))
    ] if not appver_raw.empty else appver_raw

    d_start, d_end = render_date_range([intl_split_all, intl_av_all], key="dr_intl")
    if d_start is not None:
        intl_split = intl_split_all[(intl_split_all['event_date'] >= d_start) & (intl_split_all['event_date'] <= d_end)] if not intl_split_all.empty else intl_split_all
        intl_av = intl_av_all[(intl_av_all['event_date'] >= d_start) & (intl_av_all['event_date'] <= d_end)] if not intl_av_all.empty else intl_av_all
    else:
        intl_split, intl_av = intl_split_all, intl_av_all

    # ---- MBNXT vs Legacy share (INTL) ----
    st.markdown(f"### 🔀 MBNXT vs Legacy — {selected_country} · {'+'.join(sel_os_split)}")
    if uv_split_raw.empty:
        st.warning("Traffic split table not yet available — run the **INTL+US app traffic split (S2 UV)** "
                   "transformation (output: `daily_uv_split`).")
    else:
        st.caption("% of total app UV over the selected date range.")
        render_split_section(intl_split, f"INTL / {selected_country}")

    st.markdown("---")

    # ---- App-version mix (INTL) ----
    st.markdown(f"### 📲 App Version Mix — {selected_country} · {'+'.join(sel_os_split)}")
    if appver_raw.empty:
        st.warning("App-version table not yet available — run the **INTL+US app traffic split by app version** "
                   "transformation (output: `daily_appversion_split`).")
    else:
        st.caption("MBNXT app builds as % of daily MBNXT UV. INTL markets get MBNXT from the June launch (build 26.10).")
        _av_opts = sorted(intl_av['app_version'].dropna().unique().tolist()) if not intl_av.empty else []
        _picked = st.multiselect("App version", options=['All'] + _av_opts, default=['All'], key="av_intl")
        _sel_v = None if ('All' in _picked or not _picked) else _picked
        render_appversion_section(intl_av, f"INTL / {selected_country}", selected_versions=_sel_v)


with tab_split_us:
    st.caption("US app traffic. **Top:** MBNXT vs Legacy share of app UV (from MSA S2). "
               "**Bottom:** MBNXT app-version mix. Fixed to US — the Country filter does not apply; Client Platform does.")
    sel_os_split = selected_os or os_opts_all

    us_split_all = uv_split_raw[
        (uv_split_raw['country_code'] == 'US') &
        (uv_split_raw['operating_system'].isin(sel_os_split))
    ] if not uv_split_raw.empty else uv_split_raw
    us_av_all = appver_raw[
        (appver_raw['country_code'] == 'US') &
        (appver_raw['operating_system'].isin(sel_os_split))
    ] if not appver_raw.empty else appver_raw

    d_start, d_end = render_date_range([us_split_all, us_av_all], key="dr_us")
    if d_start is not None:
        us_split = us_split_all[(us_split_all['event_date'] >= d_start) & (us_split_all['event_date'] <= d_end)] if not us_split_all.empty else us_split_all
        us_av = us_av_all[(us_av_all['event_date'] >= d_start) & (us_av_all['event_date'] <= d_end)] if not us_av_all.empty else us_av_all
    else:
        us_split, us_av = us_split_all, us_av_all

    # ---- MBNXT vs Legacy share (US) ----
    st.markdown(f"### 🔀 MBNXT vs Legacy — US · {'+'.join(sel_os_split)}")
    if uv_split_raw.empty:
        st.warning("Traffic split table not yet available — run the **INTL+US app traffic split (S2 UV)** "
                   "transformation (output: `daily_uv_split`).")
    else:
        st.caption("% of total app UV over the selected date range.")
        render_split_section(us_split, "US")

    st.markdown("---")

    # ---- App-version mix (US) ----
    st.markdown(f"### 📲 App Version Mix — US · {'+'.join(sel_os_split)}")
    if appver_raw.empty:
        st.warning("App-version table not yet available — run the **INTL+US app traffic split by app version** "
                   "transformation (output: `daily_appversion_split`).")
    else:
        st.caption("US MBNXT app builds as % of daily MBNXT UV — the version migration waves.")
        _av_opts_us = sorted(us_av['app_version'].dropna().unique().tolist()) if not us_av.empty else []
        _picked_us = st.multiselect("App version", options=['All'] + _av_opts_us, default=['All'], key="av_us")
        _sel_v_us = None if ('All' in _picked_us or not _picked_us) else _picked_us
        render_appversion_section(us_av, "US", selected_versions=_sel_v_us)


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

    st.markdown("### MBNXT vs Legacy via local bucketing (26.10+)")
    st.markdown(
        """
The INTL ramp **does** use local bucketing (launched 25 Jun 2026), so a clean **MBNXT-vs-Legacy comparison is
available** on the new app build (appVersion 26.10+).

- Bucketing assigns users ~**50:50** to MBNXT vs Legacy at hash level — a randomised A/B, so a side-by-side
  comparison (CVR, M1 VFM/UV, etc.) is defensible.
- Cohort entry = a user's **first event on 26.10+**, and metrics are measured **forward** from there, so there is no
  *second-launch bias* (no pre-bucketing purchases are attributed).
- The Cohort tab uses **all active users** on 26.10 (new + existing), not new-users-only: Android new-install volume
  is too low to read, and on iOS the app update mints fresh bcookies so "new users" are unreliable. All-users gives
  bigger, artifact-free samples.

⚠️ Read small per-platform cells with care — Android cohorts are still low-volume (tens of orders).
"""
    )

    st.markdown("---")

    st.markdown("### 📈 YoY Trends tab")
    st.markdown(
        """
**Purpose.** Daily DoD / WoW / YoY trends per country. Single table, most recent day first.

**Inline filters.**

- **Country** — single select (default GB).
- **OS** — multiselect (default iOS). Multiple OS values are summed.

**Table columns.**

| Column | Meaning |
|---|---|
| ISO Week | ISO week number |
| Week day | Day name (Monday … Sunday) |
| Date | Calendar date |
| M1_VFM | Daily M1 VFM (USD) |
| DoD | Day-over-Day change (vs previous calendar day) |
| WoW | Week-over-Week change (vs same day 7 days ago) |
| YoY | Year-over-Year change (vs same day 364 days ago) |
| M1_VFM/UV | M1 VFM / Daily UVs |
| M1/UV WoW / YoY | WoW and YoY for M1_VFM/UV |
| Daily UVs | Distinct bcookies from juno |
| UVs WoW / YoY | WoW and YoY for Daily UVs |

Color scale: green positive, red negative (scale ±30%).

**Data sources.**

- `kbc-grpn-40-0cd2.out_c_intl_app_yoy_trends.daily_yoy_INTL_app` — daily financial metrics from `unit_economics`,
  filtered to INTL countries, `event_platform LIKE 'app%'`, `user_brand_affiliation = 'groupon'`,
  `event_type IN ('authorize', 'capture')`, `last_status <> 'cancel'`.
- `kbc-grpn-40-0cd2.out_c_intl_app_yoy_trends.daily_uv_INTL_app` — daily distinct bcookies from
  `junoHourly_analytics`, filtered to mobile + INTL + groupon brand + bot-excluded.
"""
    )

    st.markdown("---")

    st.markdown("### 🔄 Cohort Analysis tab")
    st.markdown(
        """
**Purpose.** MBNXT vs Legacy cohorts on the new build (26.10+), by Start-Day week, split by groupon_version
(see the local-bucketing note above).

**Cohort definition.** A "cohort" is the set of **all active users** (new + existing) whose **first event on
appVersion 26.10+** falls in a given ISO week, bucketed by the Monday of that first event. Arm = `grouponversion`
(mbnxt / legacy) at that first event. Source: `ext_bucket_janus.junoHourly_analytics` (cohort entry) + `unit_economics`.

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
- **YoY transformation** (`INTL app YoY trends`) — outputs `daily_yoy_INTL_app` (full-replace, daily financial)
  and `daily_uv_INTL_app` (incremental, last-3-days UV window). Initial UV backfill: run once with
  `BACKFILL_FROM = DATE_SUB(CURRENT_DATE, INTERVAL 400 DAY)` in the `days_to_delete` block to seed 13+ months.
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
