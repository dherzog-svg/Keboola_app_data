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
CONTEXT_STRING = f"""You are an AI assistant embedded in a Keboola Data App.
## YOUR FIRST PRIORITY: UNDERSTAND THE APP'S METHODOLOGY
Before answering ANY analytical or calculation question, you MUST:
1. **Look up this Data App's source code** using the `get_data_apps` tool with configuration_id: "{DATA_APP_CONFIG_ID}"
2. **Analyze the code** to understand:
   - What tables and columns are used
   - How metrics are calculated (formulas, aggregations)
   - How baselines, tiers, or benchmarks are defined
   - What filters and dimensions exist
   - The exact business logic implemented
3. **Use the SAME methodology** as the app when answering questions
## BEFORE ANSWERING CALCULATION QUESTIONS
If the user asks about scenarios, what-ifs, or calculations:
1. First ask clarifying questions to understand exactly what they need:
   - Time period, regions, filters?
   - How to interpret their request (e.g., "15% improvement" can mean different things)?
2. Only proceed after user confirms the parameters
3. Use the methodology from the app's source code
## HOW TO ANSWER
1. Query the data using the same logic as shown in the app's code
2. Show your calculation methodology
3. Ensure your results are consistent with what the dashboard would show
## WRONG APPROACHES (DO NOT DO THIS)
❌ Making up your own methodology without checking the app's code
❌ Using different formulas than what's in the app
❌ Answering calculation questions without clarifying parameters first
❌ Assuming how metrics are calculated instead of checking the code
User question: """

STORAGE_API_TOKEN = os.environ.get("STORAGE_API_TOKEN") or os.environ.get("KBC_TOKEN", "")
STORAGE_API_URL = os.environ.get("STORAGE_API_URL") or os.environ.get("KBC_URL", "")

# Session state initialization
if "kai_messages" not in st.session_state:
    st.session_state.kai_messages = []
if "kai_chat_id" not in st.session_state:
    st.session_state.kai_chat_id = KaiClient.new_chat_id() if KAI_AVAILABLE else None
if "pending_approval" not in st.session_state:
    st.session_state.pending_approval = None
if "pending_prompt" not in st.session_state:
    st.session_state.pending_prompt = None

# Async helper
def run_async(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()

# Client factory
async def get_client():
    return await KaiClient.from_storage_api(
        storage_api_token=STORAGE_API_TOKEN,
        storage_api_url=STORAGE_API_URL,
    )

# Streaming chat response collector
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
            container.error(f"🔐 **Authentication Error (401)**\n\nYour Storage API token may be invalid or expired.\n\nError: {error_msg}")
        else:
            container.error(f"Error: {error_msg}")
        return f"Error: {error_msg}", None

# Tool approval handler
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
# PAGE CONFIG
# =============================================================================
st.set_page_config(
    page_title="INTL Markets Analytics",
    page_icon="📊",
    layout="wide"
)

# =============================================================================
# DATA LOADING
# =============================================================================

@st.cache_data(ttl=300)
def load_behaviour_data():
    query = '''
    SELECT
        `country`, `uv`, `active_uv`, `active_uv_rate_pct`, `nob`,
        `m1vfm_usd`, `gross_bookings_usd`, `cvr`, `m1vfm_per_active_uv`,
        `gross_bookings_per_active_uv`, `m1vfm_per_uv`, `gross_bookings_per_uv`
    FROM `kbc-grpn-40-0cd2`.`out_c_testing_data_apps`.`user_behaviour_INTL`
    '''
    df = query_data(query)
    numeric_cols = [
        'uv', 'active_uv', 'active_uv_rate_pct', 'nob', 'm1vfm_usd',
        'gross_bookings_usd', 'cvr', 'm1vfm_per_active_uv',
        'gross_bookings_per_active_uv', 'm1vfm_per_uv', 'gross_bookings_per_uv'
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    return df


@st.cache_data(ttl=300)
def load_financial_data():
    query = '''
    SELECT
        `country_upper`, `order_created_date`, `client_platform`,
        `orders`, `activations`, `reactivations`, `refunded_orders`, `cancelled_orders`,
        `deals_all`, `deals_with_od_applied`, `deals_with_ils_applied`,
        `gross_bookings`, `nob`, `gb_deals_with_od_applied`, `gb_deals_with_ils_applied`,
        `ils_applied`, `od_applied`, `m1_vfm`, `m1_vfm_deals_with_od_applied`, `m2_estimate`
    FROM `kbc-grpn-40-0cd2`.`out_c_testing_data_apps`.`financial_INTL_app`
    '''
    df = query_data(query)
    numeric_cols = [
        'orders', 'activations', 'reactivations', 'refunded_orders', 'cancelled_orders',
        'deals_all', 'deals_with_od_applied', 'deals_with_ils_applied',
        'gross_bookings', 'nob', 'gb_deals_with_od_applied', 'gb_deals_with_ils_applied',
        'ils_applied', 'od_applied', 'm1_vfm', 'm1_vfm_deals_with_od_applied', 'm2_estimate'
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    df['order_created_date'] = pd.to_datetime(df['order_created_date'])
    return df


@st.cache_data(ttl=300)
def load_cohort_data():
    query = '''
    SELECT
        `country`, `cohort_week`, `new_uvs`,
        `buyers_d1_7`, `cvr_d1_7_pct`, `m1vfm_per_uv_d1_7_usd`,
        `buyers_d7_14`, `cvr_d7_14_pct`, `m1vfm_per_uv_d7_14_usd`,
        `buyers_d14_28`, `cvr_d14_28_pct`, `m1vfm_per_uv_d14_28_usd`
    FROM `kbc-grpn-40-0cd2`.`out_c_testing_data_apps`.`cohort_INTL_app`
    '''
    df = query_data(query)
    numeric_cols = [
        'new_uvs', 'buyers_d1_7', 'cvr_d1_7_pct', 'm1vfm_per_uv_d1_7_usd',
        'buyers_d7_14', 'cvr_d7_14_pct', 'm1vfm_per_uv_d7_14_usd',
        'buyers_d14_28', 'cvr_d14_28_pct', 'm1vfm_per_uv_d14_28_usd'
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    df['cohort_week'] = pd.to_datetime(df['cohort_week'])
    return df


# =============================================================================
# MAIN APP
# =============================================================================
st.title("📊 INTL Markets Analytics")

tab_behaviour, tab_financial, tab_cohort, tab_kai = st.tabs([
    "🧠 User Behaviour",
    "💰 Financial Performance",
    "🔄 Cohort Analysis",
    "🤖 Ask Kai"
])

# =============================================================================
# TAB 1: USER BEHAVIOUR
# =============================================================================
with tab_behaviour:
    st.markdown("Snapshot of user behaviour metrics across international markets.")

    try:
        df = load_behaviour_data()

        if df.empty:
            st.warning("No data available.")
            st.stop()

        # --- Inline filters ---
        f1, f2, f3 = st.columns([3, 2, 1])
        with f1:
            countries = sorted(df['country'].dropna().unique().tolist())
            selected_countries = st.multiselect(
                "Select Countries", options=countries, default=countries, key="beh_countries"
            )
        with f2:
            metric_options = {
                'Unique Visitors (UV)': 'uv',
                'Active UV': 'active_uv',
                'Active UV Rate (%)': 'active_uv_rate_pct',
                'Number of Bookings': 'nob',
                'M1 VFM (USD)': 'm1vfm_usd',
                'Gross Bookings (USD)': 'gross_bookings_usd',
                'Conversion Rate': 'cvr',
                'M1 VFM per Active UV': 'm1vfm_per_active_uv',
                'Gross Bookings per Active UV': 'gross_bookings_per_active_uv',
                'M1 VFM per UV': 'm1vfm_per_uv',
                'Gross Bookings per UV': 'gross_bookings_per_uv'
            }
            selected_metric_name = st.selectbox(
                "Metric for Charts", options=list(metric_options.keys()), index=4, key="beh_metric"
            )
            selected_metric = metric_options[selected_metric_name]
        with f3:
            chart_type = st.radio(
                "Chart Type", options=["Bar", "Pie", "H. Bar"], index=0, key="beh_chart"
            )

        filtered_df = df[df['country'].isin(selected_countries)] if selected_countries else df.copy()

        # KPIs
        st.header("📈 Key Metrics Summary")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total UV", f"{filtered_df['uv'].sum():,.0f}")
        with col2:
            st.metric("Total Active UV", f"{filtered_df['active_uv'].sum():,.0f}")
        with col3:
            st.metric("Total Bookings", f"{filtered_df['nob'].sum():,.0f}")
        with col4:
            st.metric("Total M1 VFM (USD)", f"${filtered_df['m1vfm_usd'].sum():,.2f}")

        col5, col6, col7, col8 = st.columns(4)
        with col5:
            st.metric("Total Gross Bookings (USD)", f"${filtered_df['gross_bookings_usd'].sum():,.2f}")
        with col6:
            avg_cvr = filtered_df['cvr'].mean() if len(filtered_df) > 0 else 0
            st.metric("Avg Conversion Rate", f"{avg_cvr:.4f}")
        with col7:
            avg_active_rate = filtered_df['active_uv_rate_pct'].mean() if len(filtered_df) > 0 else 0
            st.metric("Avg Active UV Rate (%)", f"{avg_active_rate:.2f}%")
        with col8:
            st.metric("Countries", filtered_df['country'].nunique())

        # Main chart
        st.header(f"📊 {selected_metric_name} by Country")
        chart_df = filtered_df[['country', selected_metric]].sort_values(by=selected_metric, ascending=False)

        if chart_type == "Bar":
            fig = px.bar(chart_df, x='country', y=selected_metric, color=selected_metric,
                         color_continuous_scale='Blues', title=f'{selected_metric_name} by Country')
            fig.update_layout(xaxis_tickangle=-45)
        elif chart_type == "Pie":
            fig = px.pie(chart_df, values=selected_metric, names='country',
                         title=f'{selected_metric_name} Distribution by Country')
            fig.update_traces(textposition='inside', textinfo='percent+label')
        else:
            fig = px.bar(chart_df, y='country', x=selected_metric, color=selected_metric,
                         color_continuous_scale='Blues', orientation='h',
                         title=f'{selected_metric_name} by Country')

        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)

        # Scatter comparison
        st.header("🔄 Metric Comparison")
        sc1, sc2 = st.columns(2)
        with sc1:
            compare_metric_1 = st.selectbox("First Metric", list(metric_options.keys()), index=4, key="beh_m1")
        with sc2:
            compare_metric_2 = st.selectbox("Second Metric", list(metric_options.keys()), index=5, key="beh_m2")

        fig_scatter = px.scatter(
            filtered_df, x=metric_options[compare_metric_1], y=metric_options[compare_metric_2],
            color='country', size='uv', hover_name='country',
            title=f'{compare_metric_1} vs {compare_metric_2}',
            labels={
                metric_options[compare_metric_1]: compare_metric_1,
                metric_options[compare_metric_2]: compare_metric_2
            }
        )
        fig_scatter.update_layout(height=450)
        st.plotly_chart(fig_scatter, use_container_width=True)

        # Data table
        st.header("📋 Data Table")
        display_df = filtered_df.copy().sort_values(by='m1vfm_usd', ascending=False)
        st.dataframe(
            display_df, use_container_width=True, hide_index=True,
            column_config={
                "country": st.column_config.TextColumn("Country", width="medium"),
                "uv": st.column_config.NumberColumn("UV", format="%d"),
                "active_uv": st.column_config.NumberColumn("Active UV", format="%d"),
                "active_uv_rate_pct": st.column_config.NumberColumn("Active UV Rate %", format="%.2f%%"),
                "nob": st.column_config.NumberColumn("Bookings", format="%d"),
                "m1vfm_usd": st.column_config.NumberColumn("M1 VFM (USD)", format="$%.2f"),
                "gross_bookings_usd": st.column_config.NumberColumn("Gross Bookings (USD)", format="$%.2f"),
                "cvr": st.column_config.NumberColumn("CVR", format="%.4f"),
                "m1vfm_per_active_uv": st.column_config.NumberColumn("M1 VFM/Active UV", format="$%.2f"),
                "gross_bookings_per_active_uv": st.column_config.NumberColumn("GB/Active UV", format="$%.2f"),
                "m1vfm_per_uv": st.column_config.NumberColumn("M1 VFM/UV", format="$%.4f"),
                "gross_bookings_per_uv": st.column_config.NumberColumn("GB/UV", format="$%.4f"),
            }
        )
        st.download_button(
            label="📥 Download CSV",
            data=filtered_df.to_csv(index=False),
            file_name="user_behaviour_intl.csv",
            mime="text/csv",
            key="beh_download"
        )

    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        st.info("Please check that the data table exists and contains data.")


# =============================================================================
# TAB 2: FINANCIAL PERFORMANCE
# =============================================================================
with tab_financial:
    st.markdown("Daily financial performance — orders, revenue, platform mix, and discount impact across INTL markets.")

    try:
        fin_df = load_financial_data()

        if fin_df.empty:
            st.warning("No financial data available.")
            st.stop()

        # --- Filters ---
        ff1, ff2, ff3 = st.columns([2, 2, 2])
        with ff1:
            fin_countries = sorted(fin_df['country_upper'].unique())
            sel_fin_countries = st.multiselect(
                "Countries", fin_countries, default=fin_countries, key="fin_countries"
            )
        with ff2:
            min_d = fin_df['order_created_date'].min().date()
            max_d = fin_df['order_created_date'].max().date()
            date_range = st.date_input(
                "Date Range", value=(min_d, max_d), min_value=min_d, max_value=max_d, key="fin_dates"
            )
        with ff3:
            all_platforms = sorted(fin_df['client_platform'].unique())
            sel_platforms = st.multiselect(
                "Platform", all_platforms, default=all_platforms, key="fin_platforms"
            )

        # Apply filters
        start_d, end_d = (date_range[0], date_range[1]) if len(date_range) == 2 else (min_d, max_d)
        fdf = fin_df[
            fin_df['country_upper'].isin(sel_fin_countries) &
            (fin_df['order_created_date'].dt.date >= start_d) &
            (fin_df['order_created_date'].dt.date <= end_d) &
            fin_df['client_platform'].isin(sel_platforms)
        ]

        # --- KPIs ---
        st.header("📈 Key Metrics")
        k1, k2, k3, k4 = st.columns(4)
        total_orders = fdf['orders'].sum()
        total_gb = fdf['gross_bookings'].sum()
        total_m1 = fdf['m1_vfm'].sum()
        total_m2 = fdf['m2_estimate'].sum()
        with k1:
            st.metric("Total Orders", f"{total_orders:,.0f}")
        with k2:
            st.metric("Gross Bookings", f"${total_gb:,.0f}")
        with k3:
            st.metric("M1 VFM", f"${total_m1:,.0f}")
        with k4:
            st.metric("M2 Estimate", f"${total_m2:,.0f}")

        k5, k6, k7, k8 = st.columns(4)
        total_act = fdf['activations'].sum()
        total_react = fdf['reactivations'].sum()
        total_refund = fdf['refunded_orders'].sum()
        total_od = abs(fdf['od_applied'].sum())
        total_ils = abs(fdf['ils_applied'].sum())
        total_disc = total_od + total_ils
        with k5:
            act_rate = (total_act / total_orders * 100) if total_orders > 0 else 0
            st.metric("New Activations", f"{total_act:,.0f}", delta=f"{act_rate:.1f}% of orders")
        with k6:
            react_rate = (total_react / total_orders * 100) if total_orders > 0 else 0
            st.metric("Reactivations", f"{total_react:,.0f}", delta=f"{react_rate:.1f}% of orders")
        with k7:
            refund_rate = (total_refund / total_orders * 100) if total_orders > 0 else 0
            st.metric("Refunded Orders", f"{total_refund:,.0f}", delta=f"{refund_rate:.1f}% rate", delta_color="inverse")
        with k8:
            disc_rate = (total_disc / total_gb * 100) if total_gb > 0 else 0
            st.metric("Total Discounts", f"${total_disc:,.0f}", delta=f"{disc_rate:.1f}% of GB", delta_color="inverse")

        st.markdown("---")

        # --- Daily Trend ---
        st.header("📅 Daily Revenue Trend")
        tc1, tc2 = st.columns([2, 1])
        with tc1:
            trend_metric_options = {
                'Gross Bookings (USD)': 'gross_bookings',
                'M1 VFM (USD)': 'm1_vfm',
                'NOB (USD)': 'nob',
                'M2 Estimate (USD)': 'm2_estimate',
                'Orders': 'orders',
                'Activations': 'activations',
            }
            trend_metric_name = st.selectbox(
                "Metric", list(trend_metric_options.keys()), key="fin_trend_metric"
            )
            trend_metric = trend_metric_options[trend_metric_name]
        with tc2:
            group_by_country = st.checkbox("Break down by country", value=False, key="fin_group_country")

        if group_by_country:
            daily_df = fdf.groupby(['order_created_date', 'country_upper'])[trend_metric].sum().reset_index()
            fig_trend = px.line(
                daily_df, x='order_created_date', y=trend_metric, color='country_upper',
                title=f'Daily {trend_metric_name} by Country',
                labels={'order_created_date': 'Date', trend_metric: trend_metric_name, 'country_upper': 'Country'}
            )
        else:
            daily_df = fdf.groupby('order_created_date')[trend_metric].sum().reset_index()
            fig_trend = px.area(
                daily_df, x='order_created_date', y=trend_metric,
                title=f'Daily {trend_metric_name} — All Selected Markets',
                labels={'order_created_date': 'Date', trend_metric: trend_metric_name},
                color_discrete_sequence=['#1f77b4']
            )
        fig_trend.update_layout(height=400)
        st.plotly_chart(fig_trend, use_container_width=True)

        st.markdown("---")

        # --- Country Comparison + Platform Mix ---
        st.header("🌍 Country & Platform Breakdown")
        col_a, col_b = st.columns(2)

        with col_a:
            country_summary = fdf.groupby('country_upper').agg(
                gross_bookings=('gross_bookings', 'sum'),
                m1_vfm=('m1_vfm', 'sum'),
                orders=('orders', 'sum')
            ).reset_index().sort_values('gross_bookings', ascending=True)

            fig_countries = px.bar(
                country_summary, y='country_upper', x='gross_bookings', orientation='h',
                color='gross_bookings', color_continuous_scale='Blues',
                title='Gross Bookings by Country',
                labels={'country_upper': 'Country', 'gross_bookings': 'Gross Bookings (USD)'},
                text='gross_bookings'
            )
            fig_countries.update_traces(texttemplate='$%{text:,.0f}', textposition='outside')
            fig_countries.update_layout(height=420, coloraxis_showscale=False)
            st.plotly_chart(fig_countries, use_container_width=True)

        with col_b:
            plat_total = fdf.groupby('country_upper')['orders'].sum().reset_index(name='orders_total')
            plat_split = fdf.groupby(['country_upper', 'client_platform'])['orders'].sum().reset_index()
            plat_split = plat_split.merge(plat_total, on='country_upper')
            plat_split['share_pct'] = plat_split['orders'] / plat_split['orders_total'] * 100
            country_order = plat_total.sort_values('orders_total', ascending=False)['country_upper'].tolist()

            fig_platform = px.bar(
                plat_split, x='country_upper', y='share_pct', color='client_platform',
                barmode='stack', title='Platform Mix by Country (%)',
                labels={'country_upper': 'Country', 'share_pct': 'Share (%)', 'client_platform': 'Platform'},
                category_orders={'country_upper': country_order},
                color_discrete_map={'iphone': '#007AFF', 'android': '#34C759', 'ipad': '#FF9500'}
            )
            fig_platform.update_layout(height=420)
            st.plotly_chart(fig_platform, use_container_width=True)

        st.markdown("---")

        # --- Order Quality + Discount Analysis ---
        st.header("🔍 Order Quality & Discount Impact")
        col_c, col_d = st.columns(2)

        with col_c:
            quality_df = fdf.groupby('country_upper').agg(
                orders=('orders', 'sum'),
                activations=('activations', 'sum'),
                reactivations=('reactivations', 'sum'),
                refunded_orders=('refunded_orders', 'sum'),
                cancelled_orders=('cancelled_orders', 'sum')
            ).reset_index()
            quality_df['New Activation'] = quality_df['activations'] / quality_df['orders'].replace(0, 1) * 100
            quality_df['Reactivation'] = quality_df['reactivations'] / quality_df['orders'].replace(0, 1) * 100
            quality_df['Refund'] = quality_df['refunded_orders'] / quality_df['orders'].replace(0, 1) * 100
            quality_df['Cancellation'] = quality_df['cancelled_orders'] / quality_df['orders'].replace(0, 1) * 100

            quality_melt = quality_df.melt(
                id_vars='country_upper',
                value_vars=['New Activation', 'Reactivation', 'Refund', 'Cancellation'],
                var_name='Metric', value_name='Rate (%)'
            )
            fig_quality = px.bar(
                quality_melt, x='country_upper', y='Rate (%)', color='Metric', barmode='group',
                title='Order Quality Rates by Country (%)',
                labels={'country_upper': 'Country'},
                color_discrete_map={
                    'New Activation': '#2ecc71',
                    'Reactivation': '#3498db',
                    'Refund': '#e74c3c',
                    'Cancellation': '#e67e22'
                }
            )
            fig_quality.update_layout(height=420)
            st.plotly_chart(fig_quality, use_container_width=True)

        with col_d:
            discount_df = fdf.groupby('country_upper').agg(
                gross_bookings=('gross_bookings', 'sum'),
                od_applied=('od_applied', 'sum'),
                ils_applied=('ils_applied', 'sum'),
            ).reset_index()
            discount_df['OD Discount'] = abs(discount_df['od_applied']) / discount_df['gross_bookings'].replace(0, 1) * 100
            discount_df['ILS Discount'] = abs(discount_df['ils_applied']) / discount_df['gross_bookings'].replace(0, 1) * 100

            disc_melt = discount_df.melt(
                id_vars='country_upper',
                value_vars=['OD Discount', 'ILS Discount'],
                var_name='Discount Type', value_name='% of Gross Bookings'
            )
            fig_discount = px.bar(
                disc_melt, x='country_upper', y='% of Gross Bookings',
                color='Discount Type', barmode='stack',
                title='Discount Impact as % of Gross Bookings',
                labels={'country_upper': 'Country'},
                color_discrete_map={
                    'OD Discount': '#9b59b6',
                    'ILS Discount': '#1abc9c'
                }
            )
            fig_discount.update_layout(height=420)
            st.plotly_chart(fig_discount, use_container_width=True)

        # --- Summary Table ---
        st.header("📋 Daily Summary Table")
        daily_summary = fdf.groupby(['order_created_date', 'country_upper']).agg(
            orders=('orders', 'sum'),
            gross_bookings=('gross_bookings', 'sum'),
            m1_vfm=('m1_vfm', 'sum'),
            m2_estimate=('m2_estimate', 'sum'),
            activations=('activations', 'sum'),
            reactivations=('reactivations', 'sum'),
            refunded_orders=('refunded_orders', 'sum'),
        ).reset_index().sort_values(['order_created_date', 'country_upper'], ascending=[False, True])

        st.dataframe(
            daily_summary, use_container_width=True, hide_index=True,
            column_config={
                "order_created_date": st.column_config.DateColumn("Date", format="YYYY-MM-DD"),
                "country_upper": st.column_config.TextColumn("Country"),
                "orders": st.column_config.NumberColumn("Orders", format="%d"),
                "gross_bookings": st.column_config.NumberColumn("Gross Bookings (USD)", format="$%.2f"),
                "m1_vfm": st.column_config.NumberColumn("M1 VFM (USD)", format="$%.2f"),
                "m2_estimate": st.column_config.NumberColumn("M2 Estimate (USD)", format="$%.2f"),
                "activations": st.column_config.NumberColumn("Activations", format="%d"),
                "reactivations": st.column_config.NumberColumn("Reactivations", format="%d"),
                "refunded_orders": st.column_config.NumberColumn("Refunds", format="%d"),
            }
        )
        st.download_button(
            label="📥 Download CSV",
            data=daily_summary.to_csv(index=False),
            file_name="financial_intl.csv",
            mime="text/csv",
            key="fin_download"
        )

    except Exception as e:
        st.error(f"Error loading financial data: {str(e)}")
        st.info("Please check that the financial_INTL_app table exists and contains data.")


# =============================================================================
# TAB 3: COHORT ANALYSIS
# =============================================================================
with tab_cohort:
    st.markdown("Weekly cohort analysis — how new users convert over their first 28 days across INTL markets.")

    try:
        coh_df = load_cohort_data()

        if coh_df.empty:
            st.warning("No cohort data available.")
            st.stop()

        # --- Filters ---
        cf1, cf2 = st.columns([2, 2])
        with cf1:
            coh_countries = sorted(coh_df['country'].unique())
            sel_coh_countries = st.multiselect(
                "Countries", coh_countries, default=coh_countries, key="coh_countries"
            )
        with cf2:
            available_weeks = sorted(coh_df['cohort_week'].dt.date.unique())
            sel_weeks = st.multiselect(
                "Cohort Weeks", available_weeks, default=available_weeks, key="coh_weeks"
            )

        cdf = coh_df[
            coh_df['country'].isin(sel_coh_countries) &
            coh_df['cohort_week'].dt.date.isin(sel_weeks)
        ]

        # --- KPIs ---
        st.header("📈 Key Metrics")
        kk1, kk2, kk3, kk4 = st.columns(4)
        with kk1:
            st.metric("Total New UVs", f"{cdf['new_uvs'].sum():,.0f}")
        with kk2:
            st.metric("Avg CVR Day 1-7", f"{cdf['cvr_d1_7_pct'].mean():.2f}%")
        with kk3:
            st.metric("Avg CVR Day 7-14", f"{cdf['cvr_d7_14_pct'].mean():.2f}%")
        with kk4:
            st.metric("Avg CVR Day 14-28", f"{cdf['cvr_d14_28_pct'].mean():.2f}%")

        st.markdown("---")

        # --- New UVs by Country & Week ---
        st.header("👥 New Users (UV) by Country & Cohort Week")
        uvs_df = cdf.copy()
        uvs_df['cohort_week_str'] = uvs_df['cohort_week'].dt.strftime('%Y-%m-%d')
        fig_uvs = px.bar(
            uvs_df, x='country', y='new_uvs', color='cohort_week_str', barmode='group',
            title='New UVs by Country and Cohort Week',
            labels={'country': 'Country', 'new_uvs': 'New UVs', 'cohort_week_str': 'Cohort Week'}
        )
        fig_uvs.update_layout(height=400)
        st.plotly_chart(fig_uvs, use_container_width=True)

        st.markdown("---")

        # --- CVR by Conversion Window + M1 VFM per UV ---
        st.header("🔄 Conversion Funnel by Time Window")
        cc1, cc2 = st.columns(2)

        with cc1:
            cvr_country = cdf.groupby('country').agg(
                cvr_d1_7=('cvr_d1_7_pct', 'mean'),
                cvr_d7_14=('cvr_d7_14_pct', 'mean'),
                cvr_d14_28=('cvr_d14_28_pct', 'mean'),
            ).reset_index()
            cvr_melt = cvr_country.melt(
                id_vars='country', value_vars=['cvr_d1_7', 'cvr_d7_14', 'cvr_d14_28'],
                var_name='Window', value_name='CVR (%)'
            )
            cvr_melt['Window'] = cvr_melt['Window'].map({
                'cvr_d1_7': 'Day 1–7', 'cvr_d7_14': 'Day 7–14', 'cvr_d14_28': 'Day 14–28'
            })
            fig_cvr = px.bar(
                cvr_melt, x='country', y='CVR (%)', color='Window', barmode='group',
                title='Avg CVR by Country & Time Window',
                labels={'country': 'Country'},
                color_discrete_map={'Day 1–7': '#2196F3', 'Day 7–14': '#FF9800', 'Day 14–28': '#9C27B0'}
            )
            fig_cvr.update_layout(height=420)
            st.plotly_chart(fig_cvr, use_container_width=True)

        with cc2:
            vfm_country = cdf.groupby('country').agg(
                vfm_d1_7=('m1vfm_per_uv_d1_7_usd', 'mean'),
                vfm_d7_14=('m1vfm_per_uv_d7_14_usd', 'mean'),
                vfm_d14_28=('m1vfm_per_uv_d14_28_usd', 'mean'),
            ).reset_index()
            vfm_melt = vfm_country.melt(
                id_vars='country', value_vars=['vfm_d1_7', 'vfm_d7_14', 'vfm_d14_28'],
                var_name='Window', value_name='M1 VFM per UV (USD)'
            )
            vfm_melt['Window'] = vfm_melt['Window'].map({
                'vfm_d1_7': 'Day 1–7', 'vfm_d7_14': 'Day 7–14', 'vfm_d14_28': 'Day 14–28'
            })
            fig_vfm = px.bar(
                vfm_melt, x='country', y='M1 VFM per UV (USD)', color='Window', barmode='group',
                title='Avg M1 VFM per UV by Country & Time Window',
                labels={'country': 'Country'},
                color_discrete_map={'Day 1–7': '#2196F3', 'Day 7–14': '#FF9800', 'Day 14–28': '#9C27B0'}
            )
            fig_vfm.update_layout(height=420)
            st.plotly_chart(fig_vfm, use_container_width=True)

        st.markdown("---")

        # --- CVR Heatmap ---
        st.header("🗓️ CVR Heatmap — Country × Cohort Week")
        heatmap_window = st.selectbox(
            "Time Window",
            ['Day 1–7 CVR (%)', 'Day 7–14 CVR (%)', 'Day 14–28 CVR (%)'],
            key="coh_heatmap_window"
        )
        heatmap_col_map = {
            'Day 1–7 CVR (%)': 'cvr_d1_7_pct',
            'Day 7–14 CVR (%)': 'cvr_d7_14_pct',
            'Day 14–28 CVR (%)': 'cvr_d14_28_pct'
        }
        heatmap_col = heatmap_col_map[heatmap_window]

        heat_df = cdf.copy()
        heat_df['cohort_week_str'] = heat_df['cohort_week'].dt.strftime('%Y-%m-%d')
        heat_pivot = heat_df.pivot_table(
            values=heatmap_col, index='country', columns='cohort_week_str', aggfunc='mean'
        )
        fig_heat = px.imshow(
            heat_pivot, text_auto='.2f',
            title=f'{heatmap_window} — Country × Cohort Week',
            labels={'color': heatmap_window},
            color_continuous_scale='Blues', aspect='auto'
        )
        fig_heat.update_layout(height=420)
        st.plotly_chart(fig_heat, use_container_width=True)

        st.markdown("---")

        # --- Country Deep Dive ---
        st.header("🔭 Country Deep Dive")
        deep_country = st.selectbox(
            "Select Country", sorted(cdf['country'].unique()), key="coh_deep_country"
        )
        deep_df = cdf[cdf['country'] == deep_country].sort_values('cohort_week').copy()
        deep_df['week_label'] = deep_df['cohort_week'].dt.strftime('Wk %b %d')

        if not deep_df.empty:
            dd1, dd2 = st.columns(2)
            with dd1:
                deep_cvr = deep_df.melt(
                    id_vars='week_label',
                    value_vars=['cvr_d1_7_pct', 'cvr_d7_14_pct', 'cvr_d14_28_pct'],
                    var_name='Window', value_name='CVR (%)'
                )
                deep_cvr['Window'] = deep_cvr['Window'].map({
                    'cvr_d1_7_pct': 'Day 1–7', 'cvr_d7_14_pct': 'Day 7–14', 'cvr_d14_28_pct': 'Day 14–28'
                })
                fig_deep_cvr = px.line(
                    deep_cvr, x='week_label', y='CVR (%)', color='Window', markers=True,
                    title=f'{deep_country} — CVR Trend by Cohort Week',
                    color_discrete_map={'Day 1–7': '#2196F3', 'Day 7–14': '#FF9800', 'Day 14–28': '#9C27B0'}
                )
                fig_deep_cvr.update_layout(height=320)
                st.plotly_chart(fig_deep_cvr, use_container_width=True)

            with dd2:
                fig_deep_uvs = px.bar(
                    deep_df, x='week_label', y='new_uvs',
                    title=f'{deep_country} — New UVs per Cohort Week',
                    labels={'week_label': 'Cohort Week', 'new_uvs': 'New UVs'},
                    color_discrete_sequence=['#3498db'], text='new_uvs'
                )
                fig_deep_uvs.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
                fig_deep_uvs.update_layout(height=320)
                st.plotly_chart(fig_deep_uvs, use_container_width=True)

            # M1 VFM per UV trend for selected country
            deep_vfm = deep_df.melt(
                id_vars='week_label',
                value_vars=['m1vfm_per_uv_d1_7_usd', 'm1vfm_per_uv_d7_14_usd', 'm1vfm_per_uv_d14_28_usd'],
                var_name='Window', value_name='M1 VFM per UV (USD)'
            )
            deep_vfm['Window'] = deep_vfm['Window'].map({
                'm1vfm_per_uv_d1_7_usd': 'Day 1–7',
                'm1vfm_per_uv_d7_14_usd': 'Day 7–14',
                'm1vfm_per_uv_d14_28_usd': 'Day 14–28'
            })
            fig_deep_vfm = px.bar(
                deep_vfm, x='week_label', y='M1 VFM per UV (USD)', color='Window', barmode='group',
                title=f'{deep_country} — M1 VFM per UV by Cohort Week & Window',
                labels={'week_label': 'Cohort Week'},
                color_discrete_map={'Day 1–7': '#2196F3', 'Day 7–14': '#FF9800', 'Day 14–28': '#9C27B0'}
            )
            fig_deep_vfm.update_layout(height=320)
            st.plotly_chart(fig_deep_vfm, use_container_width=True)

        # --- Data Table ---
        st.header("📋 Cohort Data Table")
        coh_display = cdf.copy().sort_values(['cohort_week', 'country'], ascending=[False, True])
        coh_display['cohort_week'] = coh_display['cohort_week'].dt.strftime('%Y-%m-%d')
        st.dataframe(
            coh_display, use_container_width=True, hide_index=True,
            column_config={
                "country": st.column_config.TextColumn("Country"),
                "cohort_week": st.column_config.TextColumn("Cohort Week"),
                "new_uvs": st.column_config.NumberColumn("New UVs", format="%d"),
                "buyers_d1_7": st.column_config.NumberColumn("Buyers D1-7", format="%d"),
                "cvr_d1_7_pct": st.column_config.NumberColumn("CVR D1-7 (%)", format="%.2f%%"),
                "m1vfm_per_uv_d1_7_usd": st.column_config.NumberColumn("M1 VFM/UV D1-7", format="$%.4f"),
                "buyers_d7_14": st.column_config.NumberColumn("Buyers D7-14", format="%d"),
                "cvr_d7_14_pct": st.column_config.NumberColumn("CVR D7-14 (%)", format="%.2f%%"),
                "m1vfm_per_uv_d7_14_usd": st.column_config.NumberColumn("M1 VFM/UV D7-14", format="$%.4f"),
                "buyers_d14_28": st.column_config.NumberColumn("Buyers D14-28", format="%d"),
                "cvr_d14_28_pct": st.column_config.NumberColumn("CVR D14-28 (%)", format="%.2f%%"),
                "m1vfm_per_uv_d14_28_usd": st.column_config.NumberColumn("M1 VFM/UV D14-28", format="$%.4f"),
            }
        )
        st.download_button(
            label="📥 Download CSV",
            data=coh_display.to_csv(index=False),
            file_name="cohort_intl.csv",
            mime="text/csv",
            key="coh_download"
        )

    except Exception as e:
        st.error(f"Error loading cohort data: {str(e)}")
        st.info("Please check that the cohort_INTL_app table exists and contains data.")


# =============================================================================
# TAB 4: KAI AI CHAT
# =============================================================================
with tab_kai:
    st.header("🤖 Ask Kai about INTL Markets Data")
    st.markdown("Chat with Kai AI to analyze your data across all three tables — user behaviour, financial performance, and cohorts.")
    if not KAI_AVAILABLE:
        st.warning("Kai AI is not available in this environment (kai_client module not found).")
        st.stop()

    with st.expander("🔧 Debug Info"):
        st.write(f"**Token available:** {'Yes ✓' if STORAGE_API_TOKEN else 'No ✗'}")
        st.write(f"**API URL:** {STORAGE_API_URL or 'Not set'}")
        st.write(f"**Chat ID:** `{st.session_state.kai_chat_id[:8]}...`")

    if not STORAGE_API_TOKEN:
        st.error("""
        ⚠️ **Storage API Token not configured.**

        To enable Kai AI chat, you need to add your Storage API token:
        1. Click your profile icon → **Settings** → **API Tokens** → **Create New Token**
        2. Give it a name (e.g., "Data App Kai") and select **Full Access**
        3. Go to your Data App Configuration → **Secrets** → find `STORAGE_API_TOKEN` and paste your token
        4. Click **Deploy** to apply changes
        """)
    else:
        with st.sidebar:
            st.markdown("---")
            st.header("🤖 Kai Chat")
            if st.button("🗑️ New Chat", key="new_chat"):
                st.session_state.kai_messages = []
                st.session_state.kai_chat_id = KaiClient.new_chat_id()
                st.session_state.pending_approval = None
                st.rerun()

        st.markdown("**💡 Quick questions:**")
        suggestions = [
            "Which country has the highest M1 VFM per active user?",
            "Compare conversion rates across all markets",
            "Which cohort week had the best D1-7 CVR and why?",
            "What's the impact of OD discounts on gross bookings by country?",
            "Which platform drives the most orders per country?",
            "Show me countries where reactivation rate exceeds activation rate"
        ]
        cols = st.columns(2)
        for i, suggestion in enumerate(suggestions):
            with cols[i % 2]:
                if st.button(suggestion, key=f"suggest_{i}", use_container_width=True):
                    st.session_state.pending_prompt = suggestion
                    st.rerun()

        st.markdown("---")

        for msg in st.session_state.kai_messages:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"])

        if st.session_state.pending_approval:
            approval = st.session_state.pending_approval
            st.warning("⚠️ **A tool requires your approval to proceed.**")
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

        prompt = st.chat_input("Ask Kai about your INTL markets data...")

        if st.session_state.pending_prompt:
            prompt = st.session_state.pending_prompt
            st.session_state.pending_prompt = None

        if prompt:
            st.session_state.kai_messages.append({"role": "user", "content": prompt})
            with st.chat_message("user"):
                st.markdown(prompt)
            with st.chat_message("assistant"):
                container = st.container()
                full_prompt = CONTEXT_STRING + prompt
                result_text, pending = run_async(collect_chat_response(
                    st.session_state.kai_chat_id, full_prompt, container
                ))
            st.session_state.kai_messages.append({"role": "assistant", "content": result_text})
            if pending:
                st.session_state.pending_approval = pending
            st.rerun()
