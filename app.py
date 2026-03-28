
import io
from pathlib import Path
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(page_title="FBB Order Command Center", layout="wide")

st.markdown("""
<style>
.main {background: #F8FAFC;}
.kpi-card {
    background: white; padding: 18px 20px; border-radius: 16px;
    border: 1px solid #CBD5E1; box-shadow: 0 4px 16px rgba(15,23,42,0.05);
}
.kpi-label {font-size: 0.9rem; color: #475569; font-weight: 600;}
.kpi-value {font-size: 2rem; font-weight: 800; color: #0F172A; line-height: 1.1;}
.small-note {font-size: 0.8rem; color: #64748B;}
.block-title {font-size: 1.1rem; font-weight: 700; color: #0F172A;}
</style>
""", unsafe_allow_html=True)

@st.cache_data
def load_data(uploaded_file):
    if uploaded_file.name.lower().endswith('.csv'):
        df = pd.read_csv(uploaded_file)
    else:
        df = pd.read_excel(uploaded_file)

    for c in ['BC Order','SalesDocument','BatchNumber','MaterialNumber']:
        if c in df.columns:
            df[c] = df[c].fillna('').astype(str)
    for c in ['OrderDate','BC Order Date','CDD']:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors='coerce')
    for c in ['Status','Order Status']:
        if c in df.columns:
            df[c] = df[c].fillna('')

    df['Month'] = df['OrderDate'].dt.strftime('%B')
    df['ISO Week'] = df['OrderDate'].dt.isocalendar().week.astype('Int64')

    dup = df.groupby(['BC Order','MaterialNumber'])['BatchNumber'].nunique().reset_index(name='Batch Count')
    dup['Is Duplicate'] = dup['Batch Count'] > 1
    df = df.merge(dup[['BC Order','MaterialNumber','Batch Count','Is Duplicate']], on=['BC Order','MaterialNumber'], how='left')
    df['Duplicate Flag'] = np.where(df['Is Duplicate'], 'Duplicate', 'Unique')

    def first_nonblank(s):
        for v in s:
            if pd.notna(v) and str(v).strip():
                return v
        return s.iloc[0] if len(s) else None

    orders = df.groupby('BC Order').agg(
        OrderDate=('OrderDate','min'),
        ClubName=('ClubName', first_nonblank),
        OrderType=('OrderType', first_nonblank),
        SalesDocument=('SalesDocument', first_nonblank),
        AnyOpen=('Order Status', lambda s: s.astype(str).str.contains('open', case=False, na=False).any()),
        AllShipped=('Order Status', lambda s: s.astype(str).str.contains('shipped', case=False, na=False).all()),
        AnyCancel=('Order Status', lambda s: s.astype(str).str.contains('cancel', case=False, na=False).any()),
        AnySwitchedPA=('Status', lambda s: s.astype(str).str.contains('switched to pa', case=False, na=False).any()),
        DuplicateLines=('Is Duplicate','sum'),
        TotalLines=('MaterialNumber','size')
    ).reset_index()

    def categorize(r):
        if r['AnyCancel']:
            return 'Cancelled'
        if r['AnySwitchedPA']:
            return 'Switched to PA'
        if r['AnyOpen']:
            return 'Open Still'
        if r['AllShipped']:
            return 'Shipped'
        return 'Other'

    orders['Order Category'] = orders.apply(categorize, axis=1)
    orders['Month'] = orders['OrderDate'].dt.strftime('%B')
    orders['ISO Week'] = orders['OrderDate'].dt.isocalendar().week.astype('Int64')
    orders['Has Duplicate'] = np.where(orders['DuplicateLines'] > 0, 'Yes', 'No')

    df = df.merge(orders[['BC Order','Order Category']], on='BC Order', how='left')
    return df, orders

def to_excel_bytes(dataframe):
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine='openpyxl') as writer:
        dataframe.to_excel(writer, index=False, sheet_name='Analyzer')
    bio.seek(0)
    return bio.getvalue()

st.title("FBB Order Command Center")
st.caption("Upload the order file to get the dashboard and analyzer from anywhere.")

uploaded = st.file_uploader("Upload Excel or CSV", type=["xlsx", "xls", "csv"])
if not uploaded:
    st.info("Upload a file to start.")
    st.stop()

df, orders = load_data(uploaded)

with st.sidebar:
    st.header("Filters")
    clubs = ['All'] + sorted([x for x in orders['ClubName'].dropna().astype(str).unique().tolist() if x.strip()])
    months = ['All'] + [m for m in ['January','February','March','April','May','June','July','August','September','October','November','December'] if m in set(orders['Month'].dropna())]
    selected_club = st.selectbox("Club", clubs, index=0)
    selected_month = st.selectbox("Month", months, index=0)
    page = st.radio("View", ["Dashboard", "Analyzer"], index=0)

orders_f = orders.copy()
if selected_club != 'All':
    orders_f = orders_f[orders_f['ClubName'] == selected_club]
if selected_month != 'All':
    orders_f = orders_f[orders_f['Month'] == selected_month]

df_f = df[df['BC Order'].isin(orders_f['BC Order'])].copy()

def metric_block(label, value, note=""):
    st.markdown(
        f"<div class='kpi-card'><div class='kpi-label'>{label}</div><div class='kpi-value'>{value:,}</div><div class='small-note'>{note}</div></div>",
        unsafe_allow_html=True
    )

if page == "Dashboard":
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    with c1: metric_block("Total Orders", len(orders_f), "Distinct orders")
    with c2: metric_block("Shipped", int((orders_f['Order Category'] == 'Shipped').sum()), "Distinct orders")
    with c3: metric_block("Open Still", int((orders_f['Order Category'] == 'Open Still').sum()), "Distinct orders")
    with c4: metric_block("Cancelled", int((orders_f['Order Category'] == 'Cancelled').sum()), "Distinct orders")
    with c5: metric_block("Switched to PA", int((orders_f['Order Category'] == 'Switched to PA').sum()), "Distinct orders")
    with c6: metric_block("Duplicate Orders", int((orders_f['Has Duplicate'] == 'Yes').sum()), "Same order + material + different batch")

    left, right = st.columns([1.15, 1])
    with left:
        weekly = orders_f.groupby('ISO Week').agg(
            Orders_Received=('BC Order','count'),
            Pending_to_Ship=('Order Category', lambda s: (s == 'Open Still').sum())
        ).reset_index()
        fig = go.Figure()
        fig.add_bar(x=weekly['ISO Week'], y=weekly['Orders_Received'], name='Orders Received')
        fig.add_scatter(x=weekly['ISO Week'], y=weekly['Pending_to_Ship'], name='Pending to Ship', yaxis='y2', mode='lines+markers')
        fig.update_layout(
            title="Weekly Orders Received vs Pending to Ship",
            yaxis=dict(title='Orders'),
            yaxis2=dict(title='Pending', overlaying='y', side='right'),
            legend=dict(orientation='h'),
            height=420,
            margin=dict(l=20, r=20, t=60, b=20)
        )
        st.plotly_chart(fig, use_container_width=True)

    with right:
        status_counts = orders_f['Order Category'].value_counts().reindex(['Shipped','Open Still','Cancelled','Switched to PA'], fill_value=0).reset_index()
        status_counts.columns = ['Category','Orders']
        fig2 = px.pie(status_counts, names='Category', values='Orders', hole=0.55, title='Order Status Split')
        fig2.update_layout(height=420, margin=dict(l=20, r=20, t=60, b=20))
        st.plotly_chart(fig2, use_container_width=True)

    b1, b2 = st.columns([1.1, 0.9])
    with b1:
        clubs_df = orders_f.groupby('ClubName')['BC Order'].count().reset_index(name='Orders').sort_values('Orders', ascending=False).head(10)
        fig3 = px.bar(clubs_df, x='Orders', y='ClubName', orientation='h', title='Top Clubs by Orders')
        fig3.update_layout(height=420, yaxis={'categoryorder': 'total ascending'}, margin=dict(l=20, r=20, t=60, b=20))
        st.plotly_chart(fig3, use_container_width=True)
    with b2:
        st.markdown("<div class='block-title'>Snapshot</div>", unsafe_allow_html=True)
        latest_date = orders_f['OrderDate'].max()
        st.write(f"Source rows: **{len(df_f):,}**")
        st.write(f"Unique orders: **{len(orders_f):,}**")
        st.write(f"Weeks covered: **{int(orders_f['ISO Week'].max()) if len(orders_f) else 0}**")
        st.write(f"Latest order date: **{latest_date.strftime('%d-%b-%Y') if pd.notna(latest_date) else '-'}**")
        st.write(f"Duplicate rows found: **{int(df_f['Is Duplicate'].sum()):,}**")
        if int(df_f['Is Duplicate'].sum()) == 0:
            st.success("No duplicate order + material combinations across batches were found in the current file.")
        else:
            st.warning("Duplicate combinations found. Review the Analyzer page.")

else:
    st.subheader("Analyzer")
    a1, a2, a3, a4 = st.columns(4)
    with a1:
        status_filter = st.multiselect("Order Category", sorted(df_f['Order Category'].dropna().unique()), default=sorted(df_f['Order Category'].dropna().unique()))
    with a2:
        dup_filter = st.selectbox("Duplicate Flag", ['All','Duplicate','Unique'], index=0)
    with a3:
        week_filter = st.multiselect("ISO Week", sorted([int(x) for x in df_f['ISO Week'].dropna().unique()]), default=sorted([int(x) for x in df_f['ISO Week'].dropna().unique()])[:])
    with a4:
        search_order = st.text_input("Search Order No")

    view = df_f.copy()
    if status_filter:
        view = view[view['Order Category'].isin(status_filter)]
    if dup_filter != 'All':
        view = view[view['Duplicate Flag'] == dup_filter]
    if week_filter:
        view = view[view['ISO Week'].isin(week_filter)]
    if search_order.strip():
        view = view[view['BC Order'].astype(str).str.contains(search_order.strip(), case=False, na=False)]

    st.write(f"Filtered rows: **{len(view):,}**")
    st.dataframe(view, use_container_width=True, height=560)

    st.download_button(
        "Download filtered analyzer as Excel",
        data=to_excel_bytes(view),
        file_name="filtered_analyzer.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
