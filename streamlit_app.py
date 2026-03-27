import asyncio
import os
import streamlit as st
import pandas as pd
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
- `user_behaviour_INTL` — snapshot of UV, active UV, CVR, bookings, M1 VFM per country
- `financial_INTL_app` — daily orders, gross bookings, M1 VFM, M2 estimate, activations, reactivations, refunds, OD/ILS discounts by country × platform (Jan–Mar 2026)
- `cohort_INTL_app` — weekly cohort CVR and M1 VFM per UV across D1-7, D7-14, D14-28 windows (Feb 2026)

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
@st.cache_data(ttl=300)
def load_behaviour_data():
    df = query_data('''
    SELECT `country`, `uv`, `active_uv`, `active_uv_rate_pct`, `nob`,
           `m1vfm_usd`, `gross_bookings_usd`, `cvr`,
           `m1vfm_per_active_uv`, `gross_bookings_per_active_uv`,
           `m1vfm_per_uv`, `gross_bookings_per_uv`
    FROM `kbc-grpn-40-0cd2`.`out_c_testing_data_apps`.`user_behaviour_INTL`
    ''')
    for col in ['uv','active_uv','active_uv_rate_pct','nob','m1vfm_usd',
                'gross_bookings_usd','cvr','m1vfm_per_active_uv',
                'gross_bookings_per_active_uv','m1vfm_per_uv','gross_bookings_per_uv']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    return df

@st.cache_data(ttl=300)
def load_financial_data():
    df = query_data('''
    SELECT `country_upper`, `order_created_date`, `client_platform`,
           `orders`, `activations`, `reactivations`, `refunded_orders`, `cancelled_orders`,
           `deals_all`, `deals_with_od_applied`, `deals_with_ils_applied`,
           `gross_bookings`, `nob`, `gb_deals_with_od_applied`, `gb_deals_with_ils_applied`,
           `ils_applied`, `od_applied`, `m1_vfm`, `m1_vfm_deals_with_od_applied`, `m2_estimate`
    FROM `kbc-grpn-40-0cd2`.`out_c_testing_data_apps`.`financial_INTL_app`
    ''')
    for col in ['orders','activations','reactivations','refunded_orders','cancelled_orders',
                'deals_all','deals_with_od_applied','deals_with_ils_applied',
                'gross_bookings','nob','gb_deals_with_od_applied','gb_deals_with_ils_applied',
                'ils_applied','od_applied','m1_vfm','m1_vfm_deals_with_od_applied','m2_estimate']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    df['order_created_date'] = pd.to_datetime(df['order_created_date'])
    return df

@st.cache_data(ttl=300)
def load_cohort_data():
    df = query_data('''
    SELECT `country`, `cohort_week`, `new_uvs`,
           `buyers_d1_7`, `cvr_d1_7_pct`, `m1vfm_per_uv_d1_7_usd`,
           `buyers_d7_14`, `cvr_d7_14_pct`, `m1vfm_per_uv_d7_14_usd`,
           `buyers_d14_28`, `cvr_d14_28_pct`, `m1vfm_per_uv_d14_28_usd`
    FROM `kbc-grpn-40-0cd2`.`out_c_testing_data_apps`.`cohort_INTL_app`
    ''')
    for col in ['new_uvs','buyers_d1_7','cvr_d1_7_pct','m1vfm_per_uv_d1_7_usd',
                'buyers_d7_14','cvr_d7_14_pct','m1vfm_per_uv_d7_14_usd',
                'buyers_d14_28','cvr_d14_28_pct','m1vfm_per_uv_d14_28_usd']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    df['cohort_week'] = pd.to_datetime(df['cohort_week'])
    return df


# =============================================================================
# LOAD DATA
# =============================================================================
try:
    beh_df_raw = load_behaviour_data()
except Exception:
    beh_df_raw = pd.DataFrame()

try:
    fin_df_raw = load_financial_data()
except Exception:
    fin_df_raw = pd.DataFrame()

try:
    coh_df_raw = load_cohort_data()
except Exception:
    coh_df_raw = pd.DataFrame()


# =============================================================================
# SIDEBAR — GLOBAL FILTERS
# =============================================================================
with st.sidebar:
    st.markdown("## 🌍 INTL Markets Analytics")
    st.markdown("---")

    # Country filter — shared across all tabs
    all_countries = sorted(set(
        list(beh_df_raw['country'].dropna().unique() if not beh_df_raw.empty else []) +
        list(fin_df_raw['country_upper'].dropna().unique() if not fin_df_raw.empty else []) +
        list(coh_df_raw['country'].dropna().unique() if not coh_df_raw.empty else [])
    ))
    st.markdown('<p class="section-label">Markets</p>', unsafe_allow_html=True)
    selected_countries = st.multiselect(
        "Select markets", options=all_countries, default=all_countries,
        label_visibility="collapsed", key="global_countries"
    )
    if not selected_countries:
        selected_countries = all_countries

    st.markdown("---")
    st.markdown('<p class="section-label">Financial Tab</p>', unsafe_allow_html=True)

    if not fin_df_raw.empty:
        min_d = fin_df_raw['order_created_date'].min().date()
        max_d = fin_df_raw['order_created_date'].max().date()
        date_range = st.date_input(
            "Date range", value=(min_d, max_d), min_value=min_d, max_value=max_d, key="fin_dates"
        )
        start_d, end_d = (date_range[0], date_range[1]) if len(date_range) == 2 else (min_d, max_d)

        all_platforms = sorted(fin_df_raw['client_platform'].dropna().unique())
        sel_platforms = st.multiselect(
            "Platform", options=all_platforms, default=all_platforms, key="fin_platforms"
        )
        if not sel_platforms:
            sel_platforms = all_platforms
    else:
        start_d, end_d, sel_platforms = None, None, []

    st.markdown("---")
    st.markdown('<p class="section-label">Cohort Tab</p>', unsafe_allow_html=True)

    if not coh_df_raw.empty:
        available_weeks = sorted(coh_df_raw['cohort_week'].dt.date.unique())
        sel_weeks = st.multiselect(
            "Cohort weeks", options=available_weeks, default=available_weeks, key="coh_weeks"
        )
        if not sel_weeks:
            sel_weeks = available_weeks
    else:
        sel_weeks = []

    if KAI_AVAILABLE and STORAGE_API_TOKEN:
        st.markdown("---")
        st.markdown('<p class="section-label">Kai AI</p>', unsafe_allow_html=True)
        if st.button("🗑️ New Chat", key="new_chat", use_container_width=True):
            st.session_state.kai_messages = []
            st.session_state.kai_chat_id = KaiClient.new_chat_id()
            st.session_state.pending_approval = None
            st.rerun()


# =============================================================================
# APPLY FILTERS
# =============================================================================
beh_df = beh_df_raw[beh_df_raw['country'].isin(selected_countries)].copy() if not beh_df_raw.empty else beh_df_raw

if not fin_df_raw.empty and start_d and end_d:
    fin_df = fin_df_raw[
        fin_df_raw['country_upper'].isin(selected_countries) &
        (fin_df_raw['order_created_date'].dt.date >= start_d) &
        (fin_df_raw['order_created_date'].dt.date <= end_d) &
        fin_df_raw['client_platform'].isin(sel_platforms)
    ].copy()
else:
    fin_df = fin_df_raw.copy()

if not coh_df_raw.empty:
    coh_df = coh_df_raw[
        coh_df_raw['country'].isin(selected_countries) &
        coh_df_raw['cohort_week'].dt.date.isin(sel_weeks)
    ].copy()
else:
    coh_df = coh_df_raw.copy()


# =============================================================================
# PAGE HEADER
# =============================================================================
st.markdown("# 🌍 INTL APP Markets by countries")
st.markdown("Use the sidebar to filter by market, date range, or platform.")

tab_behaviour, tab_financial, tab_cohort, tab_kai = st.tabs([
    "🧠 User Behaviour",
    "💰 Financial Performance",
    "🔄 Cohort Analysis",
    "🤖 Ask Kai"
])


# =============================================================================
# TAB 1 — USER BEHAVIOUR
# =============================================================================
with tab_behaviour:
    if beh_df.empty:
        st.warning("No user behaviour data available for the selected markets.")
    else:
        # --- KPIs ---
        total_uv      = beh_df['uv'].sum()
        total_active  = beh_df['active_uv'].sum()
        total_nob     = beh_df['nob'].sum()
        total_m1vfm   = beh_df['m1vfm_usd'].sum()
        total_gb      = beh_df['gross_bookings_usd'].sum()
        avg_cvr       = beh_df['cvr'].mean()
        avg_active_rt = beh_df['active_uv_rate_pct'].mean()
        avg_m1_per_uv = beh_df['m1vfm_per_uv'].mean()

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Total Unique Visitors", f"{total_uv:,.0f}")
        k2.metric("Active Visitors", f"{total_active:,.0f}", delta=f"{avg_active_rt:.1f}% active rate")
        k3.metric("Total Bookings", f"{total_nob:,.0f}")
        k4.metric("M1 VFM", f"${total_m1vfm:,.0f}")

        k5, k6, k7, k8 = st.columns(4)
        k5.metric("Gross Bookings", f"${total_gb:,.0f}")
        k6.metric("Avg Conversion Rate", f"{avg_cvr:.3f}")
        k7.metric("Avg M1 VFM per UV", f"${avg_m1_per_uv:.4f}")
        k8.metric("Markets in View", beh_df['country'].nunique())

        st.markdown("---")

        # --- Market Leaderboard ---
        hdr_col, sel_col = st.columns([2, 1])
        with hdr_col:
            st.markdown("### Market Leaderboard")
            st.markdown('<p class="section-label">Ranked by M1 VFM — the primary profitability signal</p>', unsafe_allow_html=True)
        with sel_col:
            lb_metric_options = {
                'M1 VFM (USD)': 'm1vfm_usd',
                'Gross Bookings (USD)': 'gross_bookings_usd',
                'Unique Visitors': 'uv',
                'Active Visitors': 'active_uv',
                'Bookings': 'nob',
                'Conversion Rate': 'cvr',
                'M1 VFM per Active UV': 'm1vfm_per_active_uv',
                'M1 VFM per UV': 'm1vfm_per_uv',
                'Active UV Rate (%)': 'active_uv_rate_pct',
            }
            lb_metric_name = st.selectbox("Rank by", list(lb_metric_options.keys()), index=0, key="beh_lb_metric")
            lb_metric = lb_metric_options[lb_metric_name]

        chart_df = beh_df[['country', lb_metric]].sort_values(lb_metric, ascending=False)
        fig_lb = px.bar(
            chart_df, x='country', y=lb_metric,
            color=lb_metric, color_continuous_scale='Blues',
            text=lb_metric,
            labels={'country': '', lb_metric: lb_metric_name}
        )
        fmt = '$%{text:,.0f}' if 'USD' in lb_metric_name or 'VFM' in lb_metric_name or 'Bookings' in lb_metric_name else '%{text:,.2f}'
        fig_lb.update_traces(texttemplate=fmt, textposition='outside')
        fig_lb.update_layout(height=420, coloraxis_showscale=False, margin=dict(l=0, r=80, t=20, b=20))
        st.plotly_chart(fig_lb, use_container_width=True)

        st.markdown("---")

        # --- Efficiency Quadrant ---
        st.markdown("### Efficiency Quadrant — CVR vs. M1 VFM per Active UV")
        st.markdown(
            '<div class="insight-box">Markets in the top-right quadrant are the most efficient — '
            'high conversion <em>and</em> high revenue per engaged user. These deserve priority investment.</div>',
            unsafe_allow_html=True
        )

        median_cvr = beh_df['cvr'].median()
        median_vfm = beh_df['m1vfm_per_active_uv'].median()

        fig_quad = px.scatter(
            beh_df, x='cvr', y='m1vfm_per_active_uv',
            size='uv', color='country',
            hover_name='country',
            hover_data={'uv': ':,.0f', 'active_uv': ':,.0f', 'nob': ':,.0f'},
            labels={'cvr': 'Conversion Rate', 'm1vfm_per_active_uv': 'M1 VFM per Active UV (USD)'},
            size_max=55
        )
        fig_quad.add_hline(y=median_vfm, line_dash="dot", line_color="gray", opacity=0.5,
                           annotation_text="Median VFM/UV", annotation_position="right")
        fig_quad.add_vline(x=median_cvr, line_dash="dot", line_color="gray", opacity=0.5,
                           annotation_text="Median CVR", annotation_position="top")
        fig_quad.update_layout(height=440, margin=dict(l=0, r=0, t=20, b=20))
        st.plotly_chart(fig_quad, use_container_width=True)

        st.markdown("---")

        # --- Active UV Rate vs. Gross Bookings per UV ---
        st.markdown("### Audience Quality — Active Rate vs. Revenue per Visitor")
        st.markdown(
            '<div class="insight-box">Active rate shows how many visitors engage. '
            'Gross bookings per UV shows monetisation. A high active rate with low GB/UV signals a conversion bottleneck.</div>',
            unsafe_allow_html=True
        )

        fig_aq = px.scatter(
            beh_df, x='active_uv_rate_pct', y='gross_bookings_per_uv',
            size='uv', color='country',
            hover_name='country',
            labels={
                'active_uv_rate_pct': 'Active UV Rate (%)',
                'gross_bookings_per_uv': 'Gross Bookings per UV (USD)'
            },
            size_max=55
        )
        fig_aq.update_layout(height=400, margin=dict(l=0, r=0, t=20, b=20))
        st.plotly_chart(fig_aq, use_container_width=True)

        st.markdown("---")

        # --- Data Table ---
        with st.expander("📋 Full Data Table"):
            display_df = beh_df.copy().sort_values('m1vfm_usd', ascending=False)
            st.dataframe(
                display_df, use_container_width=True, hide_index=True,
                column_config={
                    "country": st.column_config.TextColumn("Market"),
                    "uv": st.column_config.NumberColumn("UV", format="%d"),
                    "active_uv": st.column_config.NumberColumn("Active UV", format="%d"),
                    "active_uv_rate_pct": st.column_config.NumberColumn("Active Rate %", format="%.2f"),
                    "nob": st.column_config.NumberColumn("Bookings", format="%d"),
                    "m1vfm_usd": st.column_config.NumberColumn("M1 VFM (USD)", format="$%.0f"),
                    "gross_bookings_usd": st.column_config.NumberColumn("Gross Bookings (USD)", format="$%.0f"),
                    "cvr": st.column_config.NumberColumn("CVR", format="%.4f"),
                    "m1vfm_per_active_uv": st.column_config.NumberColumn("M1 VFM / Active UV", format="$%.2f"),
                    "gross_bookings_per_active_uv": st.column_config.NumberColumn("GB / Active UV", format="$%.2f"),
                    "m1vfm_per_uv": st.column_config.NumberColumn("M1 VFM / UV", format="$%.4f"),
                    "gross_bookings_per_uv": st.column_config.NumberColumn("GB / UV", format="$%.4f"),
                }
            )
            st.download_button("📥 Download CSV", beh_df.to_csv(index=False),
                               "user_behaviour_intl.csv", "text/csv", key="beh_dl")


# =============================================================================
# TAB 2 — FINANCIAL PERFORMANCE
# =============================================================================
with tab_financial:
    if fin_df.empty:
        st.warning("No financial data available for the selected filters.")
    else:
        total_orders  = fin_df['orders'].sum()
        total_gb_fin  = fin_df['gross_bookings'].sum()
        total_m1_fin  = fin_df['m1_vfm'].sum()
        total_m2_fin  = fin_df['m2_estimate'].sum()
        total_act     = fin_df['activations'].sum()
        total_react   = fin_df['reactivations'].sum()
        total_refund  = fin_df['refunded_orders'].sum()
        total_od      = abs(fin_df['od_applied'].sum())
        total_ils     = abs(fin_df['ils_applied'].sum())
        total_disc    = total_od + total_ils

        act_rate    = total_act / total_orders * 100 if total_orders > 0 else 0
        react_rate  = total_react / total_orders * 100 if total_orders > 0 else 0
        refund_rate = total_refund / total_orders * 100 if total_orders > 0 else 0
        disc_rate   = total_disc / total_gb_fin * 100 if total_gb_fin > 0 else 0
        m1_margin   = total_m1_fin / total_gb_fin * 100 if total_gb_fin > 0 else 0

        # --- KPIs ---
        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Gross Bookings", f"${total_gb_fin:,.0f}")
        k2.metric("M1 VFM", f"${total_m1_fin:,.0f}", delta=f"{m1_margin:.1f}% margin")
        k3.metric("M2 Estimate", f"${total_m2_fin:,.0f}")
        k4.metric("Total Orders", f"{total_orders:,.0f}")
        k5.metric("Total Discounts", f"${total_disc:,.0f}", delta=f"{disc_rate:.1f}% of GB", delta_color="inverse")

        k6, k7, k8, k9, k10 = st.columns(5)
        k6.metric("New Activations", f"{total_act:,.0f}", delta=f"{act_rate:.1f}%")
        k7.metric("Reactivations", f"{total_react:,.0f}", delta=f"{react_rate:.1f}%")
        k8.metric("Refunded Orders", f"{total_refund:,.0f}", delta=f"{refund_rate:.1f}% rate", delta_color="inverse")
        k9.metric("OD Discounts", f"${total_od:,.0f}")
        k10.metric("ILS Discounts", f"${total_ils:,.0f}")

        st.markdown("---")

        # --- Revenue Trend ---
        st.markdown("### Revenue Trend")
        tc1, tc2, tc3 = st.columns([2, 1, 1])
        with tc1:
            trend_options = {
                'Gross Bookings (USD)': 'gross_bookings',
                'M1 VFM (USD)': 'm1_vfm',
                'M2 Estimate (USD)': 'm2_estimate',
                'Orders': 'orders',
                'Activations': 'activations',
                'Reactivations': 'reactivations',
            }
            trend_name = st.selectbox("Metric", list(trend_options.keys()), key="fin_trend_metric")
            trend_col = trend_options[trend_name]
        with tc2:
            by_country = st.checkbox("By market", value=False, key="fin_by_country")
        with tc3:
            by_platform = st.checkbox("By platform", value=False, key="fin_by_platform")

        if by_country:
            tdf = fin_df.groupby(['order_created_date', 'country_upper'])[trend_col].sum().reset_index()
            fig_trend = px.line(tdf, x='order_created_date', y=trend_col, color='country_upper',
                                markers=False,
                                labels={'order_created_date': '', trend_col: trend_name, 'country_upper': 'Market'})
        elif by_platform:
            tdf = fin_df.groupby(['order_created_date', 'client_platform'])[trend_col].sum().reset_index()
            fig_trend = px.line(tdf, x='order_created_date', y=trend_col, color='client_platform',
                                markers=False,
                                color_discrete_map={'iphone': '#007AFF', 'android': '#34C759', 'ipad': '#FF9500'},
                                labels={'order_created_date': '', trend_col: trend_name, 'client_platform': 'Platform'})
        else:
            tdf = fin_df.groupby('order_created_date')[trend_col].sum().reset_index()
            fig_trend = px.area(tdf, x='order_created_date', y=trend_col,
                                color_discrete_sequence=['#1f77b4'],
                                labels={'order_created_date': '', trend_col: trend_name})

        fig_trend.update_layout(height=380, margin=dict(l=0, r=0, t=10, b=10))
        st.plotly_chart(fig_trend, use_container_width=True)

        st.markdown("---")

        # --- Market Performance + Platform Mix ---
        st.markdown("### Market & Platform Breakdown")
        col_a, col_b = st.columns(2)

        with col_a:
            mkt_sum = fin_df.groupby('country_upper').agg(
                gross_bookings=('gross_bookings', 'sum'),
                m1_vfm=('m1_vfm', 'sum'),
                orders=('orders', 'sum')
            ).reset_index().sort_values('gross_bookings', ascending=True)
            mkt_sum['m1_margin_pct'] = mkt_sum['m1_vfm'] / mkt_sum['gross_bookings'].replace(0, 1) * 100

            fig_mkt = go.Figure()
            fig_mkt.add_trace(go.Bar(
                y=mkt_sum['country_upper'], x=mkt_sum['gross_bookings'],
                name='Gross Bookings', orientation='h', marker_color='#93c5fd',
                text=mkt_sum['gross_bookings'].apply(lambda v: f'${v:,.0f}'),
                textposition='outside'
            ))
            fig_mkt.add_trace(go.Bar(
                y=mkt_sum['country_upper'], x=mkt_sum['m1_vfm'],
                name='M1 VFM', orientation='h', marker_color='#1d4ed8',
                text=mkt_sum['m1_vfm'].apply(lambda v: f'${v:,.0f}'),
                textposition='outside'
            ))
            fig_mkt.update_layout(
                barmode='overlay', height=420, title='Gross Bookings vs. M1 VFM by Market',
                legend=dict(orientation='h', y=1.05), margin=dict(l=0, r=90, t=40, b=10)
            )
            st.plotly_chart(fig_mkt, use_container_width=True)

        with col_b:
            plat_total = fin_df.groupby('country_upper')['orders'].sum().reset_index(name='total')
            plat_split = fin_df.groupby(['country_upper', 'client_platform'])['orders'].sum().reset_index()
            plat_split = plat_split.merge(plat_total, on='country_upper')
            plat_split['pct'] = plat_split['orders'] / plat_split['total'] * 100
            country_order_plat = plat_total.sort_values('total', ascending=False)['country_upper'].tolist()

            fig_plat = px.bar(
                plat_split, x='country_upper', y='pct', color='client_platform',
                barmode='stack', title='Platform Mix by Market (%)',
                labels={'country_upper': '', 'pct': 'Share (%)', 'client_platform': 'Platform'},
                category_orders={'country_upper': country_order_plat},
                color_discrete_map={'iphone': '#007AFF', 'android': '#34C759', 'ipad': '#FF9500'}
            )
            fig_plat.update_layout(height=420, margin=dict(l=0, r=0, t=40, b=10))
            st.plotly_chart(fig_plat, use_container_width=True)

        st.markdown("---")

        # --- Order Quality + Discount Impact ---
        st.markdown("### Order Health & Discount Impact")
        st.markdown(
            '<div class="insight-box">High activation rates signal healthy top-of-funnel growth. '
            'High refund or cancellation rates erode revenue quality. Discount rates above ~15% of GB warrant review.</div>',
            unsafe_allow_html=True
        )

        col_c, col_d = st.columns(2)

        with col_c:
            q_df = fin_df.groupby('country_upper').agg(
                orders=('orders', 'sum'),
                activations=('activations', 'sum'),
                reactivations=('reactivations', 'sum'),
                refunded_orders=('refunded_orders', 'sum'),
                cancelled_orders=('cancelled_orders', 'sum')
            ).reset_index()
            denom = q_df['orders'].replace(0, 1)
            q_df['New Activation'] = q_df['activations'] / denom * 100
            q_df['Reactivation'] = q_df['reactivations'] / denom * 100
            q_df['Refund'] = q_df['refunded_orders'] / denom * 100
            q_df['Cancellation'] = q_df['cancelled_orders'] / denom * 100
            q_melt = q_df.melt(
                id_vars='country_upper',
                value_vars=['New Activation', 'Reactivation', 'Refund', 'Cancellation'],
                var_name='Metric', value_name='Rate (%)'
            )
            fig_q = px.bar(
                q_melt, x='country_upper', y='Rate (%)', color='Metric', barmode='group',
                title='Order Quality Rates by Market (%)',
                labels={'country_upper': ''},
                color_discrete_map={
                    'New Activation': '#2ecc71', 'Reactivation': '#3498db',
                    'Refund': '#e74c3c', 'Cancellation': '#e67e22'
                }
            )
            fig_q.update_layout(height=400, margin=dict(l=0, r=0, t=40, b=10))
            st.plotly_chart(fig_q, use_container_width=True)

        with col_d:
            disc_df = fin_df.groupby('country_upper').agg(
                gross_bookings=('gross_bookings', 'sum'),
                od_applied=('od_applied', 'sum'),
                ils_applied=('ils_applied', 'sum'),
            ).reset_index()
            denom2 = disc_df['gross_bookings'].replace(0, 1)
            disc_df['OD Discount'] = abs(disc_df['od_applied']) / denom2 * 100
            disc_df['ILS Discount'] = abs(disc_df['ils_applied']) / denom2 * 100
            disc_melt = disc_df.melt(
                id_vars='country_upper',
                value_vars=['OD Discount', 'ILS Discount'],
                var_name='Type', value_name='% of Gross Bookings'
            )
            fig_disc = px.bar(
                disc_melt, x='country_upper', y='% of Gross Bookings',
                color='Type', barmode='stack',
                title='Discount Spend as % of Gross Bookings',
                labels={'country_upper': ''},
                color_discrete_map={'OD Discount': '#9b59b6', 'ILS Discount': '#1abc9c'}
            )
            fig_disc.update_layout(height=400, margin=dict(l=0, r=0, t=40, b=10))
            st.plotly_chart(fig_disc, use_container_width=True)

        st.markdown("---")

        with st.expander("📋 Daily Summary Table"):
            daily_sum = fin_df.groupby(['order_created_date', 'country_upper']).agg(
                orders=('orders', 'sum'),
                gross_bookings=('gross_bookings', 'sum'),
                m1_vfm=('m1_vfm', 'sum'),
                m2_estimate=('m2_estimate', 'sum'),
                activations=('activations', 'sum'),
                reactivations=('reactivations', 'sum'),
                refunded_orders=('refunded_orders', 'sum'),
            ).reset_index().sort_values(['order_created_date', 'country_upper'], ascending=[False, True])

            st.dataframe(daily_sum, use_container_width=True, hide_index=True,
                column_config={
                    "order_created_date": st.column_config.DateColumn("Date", format="YYYY-MM-DD"),
                    "country_upper": st.column_config.TextColumn("Market"),
                    "orders": st.column_config.NumberColumn("Orders", format="%d"),
                    "gross_bookings": st.column_config.NumberColumn("Gross Bookings", format="$%.0f"),
                    "m1_vfm": st.column_config.NumberColumn("M1 VFM", format="$%.0f"),
                    "m2_estimate": st.column_config.NumberColumn("M2 Estimate", format="$%.0f"),
                    "activations": st.column_config.NumberColumn("Activations", format="%d"),
                    "reactivations": st.column_config.NumberColumn("Reactivations", format="%d"),
                    "refunded_orders": st.column_config.NumberColumn("Refunds", format="%d"),
                })
            st.download_button("📥 Download CSV", daily_sum.to_csv(index=False),
                               "financial_intl.csv", "text/csv", key="fin_dl")


# =============================================================================
# TAB 3 — COHORT ANALYSIS
# =============================================================================
with tab_cohort:
    if coh_df.empty:
        st.warning("No cohort data available for the selected filters.")
    else:
        total_new_uvs  = coh_df['new_uvs'].sum()
        avg_cvr_d1_7   = coh_df['cvr_d1_7_pct'].mean()
        avg_cvr_d7_14  = coh_df['cvr_d7_14_pct'].mean()
        avg_cvr_d14_28 = coh_df['cvr_d14_28_pct'].mean()
        total_buyers   = coh_df['buyers_d1_7'].sum() + coh_df['buyers_d7_14'].sum() + coh_df['buyers_d14_28'].sum()

        # --- KPIs ---
        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Total New UVs", f"{total_new_uvs:,.0f}")
        k2.metric("Total Buyers (28d)", f"{total_buyers:,.0f}")
        k3.metric("Avg CVR Day 1–7", f"{avg_cvr_d1_7:.2f}%")
        k4.metric("Avg CVR Day 7–14", f"{avg_cvr_d7_14:.2f}%")
        k5.metric("Avg CVR Day 14–28", f"{avg_cvr_d14_28:.2f}%")

        st.markdown("---")

        # --- CVR by market and window (primary view) ---
        st.markdown("### Conversion Rates by Market & Time Window")
        st.markdown(
            '<div class="insight-box">Day 1–7 CVR is the strongest signal — users who convert early generate '
            'the most M1 VFM. Markets with weak D1-7 but strong D7-28 may benefit from re-engagement nudges.</div>',
            unsafe_allow_html=True
        )

        cvr_agg = coh_df.groupby('country').agg(
            cvr_d1_7=('cvr_d1_7_pct', 'mean'),
            cvr_d7_14=('cvr_d7_14_pct', 'mean'),
            cvr_d14_28=('cvr_d14_28_pct', 'mean'),
            new_uvs=('new_uvs', 'sum'),
        ).reset_index().sort_values('cvr_d1_7', ascending=False)

        cvr_melt = cvr_agg.melt(
            id_vars='country',
            value_vars=['cvr_d1_7', 'cvr_d7_14', 'cvr_d14_28'],
            var_name='Window', value_name='CVR (%)'
        )
        cvr_melt['Window'] = cvr_melt['Window'].map({
            'cvr_d1_7': 'Day 1–7', 'cvr_d7_14': 'Day 7–14', 'cvr_d14_28': 'Day 14–28'
        })
        country_order_cvr = cvr_agg['country'].tolist()

        fig_cvr = px.bar(
            cvr_melt, x='country', y='CVR (%)', color='Window', barmode='group',
            title='Average CVR by Market & Conversion Window',
            labels={'country': ''},
            category_orders={'country': country_order_cvr},
            color_discrete_map={'Day 1–7': '#2196F3', 'Day 7–14': '#FF9800', 'Day 14–28': '#9C27B0'}
        )
        fig_cvr.update_layout(height=400, margin=dict(l=0, r=0, t=40, b=10))
        st.plotly_chart(fig_cvr, use_container_width=True)

        st.markdown("---")

        # --- M1 VFM per UV + New UVs side by side ---
        st.markdown("### Revenue Quality & Audience Size")
        col_a, col_b = st.columns(2)

        with col_a:
            vfm_agg = coh_df.groupby('country').agg(
                vfm_d1_7=('m1vfm_per_uv_d1_7_usd', 'mean'),
                vfm_d7_14=('m1vfm_per_uv_d7_14_usd', 'mean'),
                vfm_d14_28=('m1vfm_per_uv_d14_28_usd', 'mean'),
            ).reset_index()
            vfm_melt = vfm_agg.melt(
                id_vars='country',
                value_vars=['vfm_d1_7', 'vfm_d7_14', 'vfm_d14_28'],
                var_name='Window', value_name='M1 VFM per UV (USD)'
            )
            vfm_melt['Window'] = vfm_melt['Window'].map({
                'vfm_d1_7': 'Day 1–7', 'vfm_d7_14': 'Day 7–14', 'vfm_d14_28': 'Day 14–28'
            })
            fig_vfm = px.bar(
                vfm_melt, x='country', y='M1 VFM per UV (USD)', color='Window', barmode='group',
                title='M1 VFM per UV by Market & Window',
                labels={'country': ''},
                color_discrete_map={'Day 1–7': '#2196F3', 'Day 7–14': '#FF9800', 'Day 14–28': '#9C27B0'}
            )
            fig_vfm.update_layout(height=380, margin=dict(l=0, r=0, t=40, b=10))
            st.plotly_chart(fig_vfm, use_container_width=True)

        with col_b:
            uvs_agg = coh_df.copy()
            uvs_agg['cohort_week_str'] = uvs_agg['cohort_week'].dt.strftime('%b %d')
            uvs_sum = uvs_agg.groupby(['country', 'cohort_week_str'])['new_uvs'].sum().reset_index()
            fig_uvs = px.bar(
                uvs_sum, x='country', y='new_uvs', color='cohort_week_str', barmode='group',
                title='New UVs by Market & Cohort Week',
                labels={'country': '', 'new_uvs': 'New UVs', 'cohort_week_str': 'Cohort Week'}
            )
            fig_uvs.update_layout(height=380, margin=dict(l=0, r=0, t=40, b=10))
            st.plotly_chart(fig_uvs, use_container_width=True)

        st.markdown("---")

        # --- CVR Heatmap ---
        st.markdown("### CVR Heatmap — Spot Trends at a Glance")
        hm_col1, hm_col2 = st.columns([1, 3])
        with hm_col1:
            hm_window = st.radio(
                "Time window",
                ['Day 1–7', 'Day 7–14', 'Day 14–28'],
                key="coh_hm_window"
            )
        hm_col_map = {
            'Day 1–7': 'cvr_d1_7_pct',
            'Day 7–14': 'cvr_d7_14_pct',
            'Day 14–28': 'cvr_d14_28_pct'
        }
        hm_col = hm_col_map[hm_window]
        heat_df = coh_df.copy()
        heat_df['week_label'] = heat_df['cohort_week'].dt.strftime('%b %d')
        heat_pivot = heat_df.pivot_table(values=hm_col, index='country', columns='week_label', aggfunc='mean')

        fig_heat = px.imshow(
            heat_pivot, text_auto='.2f',
            title=f'{hm_window} CVR (%) — Market × Cohort Week',
            labels={'color': 'CVR (%)'},
            color_continuous_scale='Blues', aspect='auto'
        )
        fig_heat.update_layout(height=400, margin=dict(l=0, r=0, t=40, b=10))
        st.plotly_chart(fig_heat, use_container_width=True)

        st.markdown("---")

        # --- Country Deep Dive ---
        st.markdown("### Country Deep Dive")
        deep_country = st.selectbox("Select market", sorted(coh_df['country'].unique()), key="coh_deep")
        deep_df = coh_df[coh_df['country'] == deep_country].sort_values('cohort_week').copy()
        deep_df['week_label'] = deep_df['cohort_week'].dt.strftime('Wk %b %d')

        if not deep_df.empty:
            dd1, dd2 = st.columns(2)

            with dd1:
                dc = deep_df.melt(
                    id_vars='week_label',
                    value_vars=['cvr_d1_7_pct', 'cvr_d7_14_pct', 'cvr_d14_28_pct'],
                    var_name='Window', value_name='CVR (%)'
                )
                dc['Window'] = dc['Window'].map({
                    'cvr_d1_7_pct': 'Day 1–7', 'cvr_d7_14_pct': 'Day 7–14', 'cvr_d14_28_pct': 'Day 14–28'
                })
                fig_dc = px.line(
                    dc, x='week_label', y='CVR (%)', color='Window', markers=True,
                    title=f'{deep_country} — CVR Trend by Cohort Week',
                    color_discrete_map={'Day 1–7': '#2196F3', 'Day 7–14': '#FF9800', 'Day 14–28': '#9C27B0'}
                )
                fig_dc.update_layout(height=320, margin=dict(l=0, r=0, t=40, b=10))
                st.plotly_chart(fig_dc, use_container_width=True)

            with dd2:
                fig_dv = px.bar(
                    deep_df, x='week_label', y='new_uvs',
                    title=f'{deep_country} — New UVs per Cohort Week',
                    labels={'week_label': '', 'new_uvs': 'New UVs'},
                    color_discrete_sequence=['#3498db'], text='new_uvs'
                )
                fig_dv.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
                fig_dv.update_layout(height=320, margin=dict(l=0, r=0, t=40, b=10))
                st.plotly_chart(fig_dv, use_container_width=True)

            vfm_deep = deep_df.melt(
                id_vars='week_label',
                value_vars=['m1vfm_per_uv_d1_7_usd', 'm1vfm_per_uv_d7_14_usd', 'm1vfm_per_uv_d14_28_usd'],
                var_name='Window', value_name='M1 VFM per UV (USD)'
            )
            vfm_deep['Window'] = vfm_deep['Window'].map({
                'm1vfm_per_uv_d1_7_usd': 'Day 1–7',
                'm1vfm_per_uv_d7_14_usd': 'Day 7–14',
                'm1vfm_per_uv_d14_28_usd': 'Day 14–28'
            })
            fig_dv2 = px.bar(
                vfm_deep, x='week_label', y='M1 VFM per UV (USD)', color='Window', barmode='group',
                title=f'{deep_country} — M1 VFM per UV by Window',
                labels={'week_label': ''},
                color_discrete_map={'Day 1–7': '#2196F3', 'Day 7–14': '#FF9800', 'Day 14–28': '#9C27B0'}
            )
            fig_dv2.update_layout(height=320, margin=dict(l=0, r=0, t=40, b=10))
            st.plotly_chart(fig_dv2, use_container_width=True)

        with st.expander("📋 Full Cohort Table"):
            coh_show = coh_df.copy().sort_values(['cohort_week', 'country'], ascending=[False, True])
            coh_show['cohort_week'] = coh_show['cohort_week'].dt.strftime('%Y-%m-%d')
            st.dataframe(coh_show, use_container_width=True, hide_index=True,
                column_config={
                    "country": st.column_config.TextColumn("Market"),
                    "cohort_week": st.column_config.TextColumn("Cohort Week"),
                    "new_uvs": st.column_config.NumberColumn("New UVs", format="%d"),
                    "buyers_d1_7": st.column_config.NumberColumn("Buyers D1-7", format="%d"),
                    "cvr_d1_7_pct": st.column_config.NumberColumn("CVR D1-7 (%)", format="%.2f"),
                    "m1vfm_per_uv_d1_7_usd": st.column_config.NumberColumn("VFM/UV D1-7", format="$%.4f"),
                    "buyers_d7_14": st.column_config.NumberColumn("Buyers D7-14", format="%d"),
                    "cvr_d7_14_pct": st.column_config.NumberColumn("CVR D7-14 (%)", format="%.2f"),
                    "m1vfm_per_uv_d7_14_usd": st.column_config.NumberColumn("VFM/UV D7-14", format="$%.4f"),
                    "buyers_d14_28": st.column_config.NumberColumn("Buyers D14-28", format="%d"),
                    "cvr_d14_28_pct": st.column_config.NumberColumn("CVR D14-28 (%)", format="%.2f"),
                    "m1vfm_per_uv_d14_28_usd": st.column_config.NumberColumn("VFM/UV D14-28", format="$%.4f"),
                })
            st.download_button("📥 Download CSV", coh_show.to_csv(index=False),
                               "cohort_intl.csv", "text/csv", key="coh_dl")


# =============================================================================
# TAB 4 — ASK KAI
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
        # Suggested PO-level questions
        st.markdown("**Strategic questions for the Product Owner:**")
        suggestions = [
            "Which 3 markets have the highest ROI potential based on CVR and M1 VFM per UV?",
            "Where should we prioritize acquisition spend — which markets have the lowest active UV rate?",
            "What is the revenue at risk from refund rates above average across all markets?",
            "Which cohort week produced the best quality users by M1 VFM per UV in D1-7?",
            "Compare iOS vs Android performance — which platform drives better M1 VFM per order?",
            "Which markets show declining CVR trends across cohort weeks — where are we losing momentum?",
            "What would total M1 VFM look like if the bottom 3 markets matched the top 3 CVR?",
            "Summarise the discount strategy: which markets are most dependent on OD or ILS discounts?",
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
