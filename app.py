import io
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from supabase import create_client

st.set_page_config(
    page_title="FBB Order Command Center",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
.stApp {background: linear-gradient(180deg, #08101d 0%, #0b1220 100%); color: #E5E7EB;}
.block-container {padding-top: 1.2rem; padding-bottom: 1rem;}
.main-title {font-size: 2rem; font-weight: 800; color: white; margin-bottom: 0.25rem;}
.sub-title {color: #94A3B8; font-size: 0.95rem; margin-bottom: 1rem;}
.kpi-card {background: linear-gradient(180deg, #111827 0%, #0f172a 100%); padding: 18px; border-radius: 18px; border: 1px solid #22304a;}
.kpi-label {font-size: 0.88rem; color: #A5B4FC; font-weight: 700; margin-bottom: 6px;}
.kpi-value {font-size: 2rem; font-weight: 800; color: #F8FAFC; line-height: 1.05;}
.small-note {font-size: 0.8rem; color: #94A3B8; margin-top: 8px;}
.section-card {background: linear-gradient(180deg, #111827 0%, #0f172a 100%); padding: 16px 16px 8px 16px; border-radius: 18px; border: 1px solid #22304a;}
.section-title {font-size: 1.05rem; font-weight: 800; color: #F8FAFC; margin-bottom: 8px;}
.status-pill-open {display:inline-block; padding:4px 10px; border-radius:999px; background:rgba(245,158,11,0.15); color:#FBBF24; font-weight:700; font-size:0.78rem; margin:2px 4px 2px 0;}
.status-pill-closed {display:inline-block; padding:4px 10px; border-radius:999px; background:rgba(34,197,94,0.15); color:#4ADE80; font-weight:700; font-size:0.78rem; margin:2px 4px 2px 0;}
div[data-testid="stDataFrame"] {border: 1px solid #22304a; border-radius: 14px; overflow: hidden;}
</style>
""", unsafe_allow_html=True)

SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]

client = create_client(SUPABASE_URL, SUPABASE_KEY)

def first_nonblank(series):
    for value in series:
        if pd.notna(value) and str(value).strip():
            return value
    return None

def categorize_order(order_statuses, workflow_statuses):
    status_order = " | ".join([str(x).strip() for x in order_statuses if str(x).strip()])
    status_flow = " | ".join([str(x).strip() for x in workflow_statuses if str(x).strip()])
    combined = f"{status_order} || {status_flow}".lower()

    if "cancel" in combined:
        return "Cancelled"
    if "switched to pa" in combined:
        return "Switched to PA"
    if "open" in combined or "pending" in combined or "backorder" in combined:
        return "Open Still"
    if "shipped" in combined or "complete" in combined or "delivered" in combined:
        return "Shipped"
    return "Other"

def week_status_from_categories(categories):
    categories = [str(x).strip() for x in categories if str(x).strip()]
    if not categories:
        return "Unknown"
    non_closed = [x for x in categories if x not in ["Shipped", "Cancelled", "Switched to PA"]]
    return "Open" if len(non_closed) > 0 else "Closed"

def to_excel_bytes(dataframe):
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        dataframe.to_excel(writer, index=False, sheet_name="Analyzer")
    bio.seek(0)
    return bio.getvalue()

def metric_block(label, value, note=""):
    st.markdown(
        f"<div class='kpi-card'><div class='kpi-label'>{label}</div><div class='kpi-value'>{value:,}</div><div class='small-note'>{note}</div></div>",
        unsafe_allow_html=True
    )

@st.cache_data(ttl=300)
def load_data():
    response = client.table("order_data").select("*").execute()
    data = response.data or []
    df = pd.DataFrame(data)

    if df.empty:
        return None, None, None

    for col in ["bc_order", "material_number", "batch_number", "order_status", "workflow_status", "club_name", "order_type", "sales_document"]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()

    for col in ["order_date", "cdd", "uploaded_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    df["Month"] = df["order_date"].dt.strftime("%B")
    df["Year"] = df["order_date"].dt.year
    df["ISO Week"] = df["order_date"].dt.isocalendar().week.astype("Int64")
    df["Year-Week"] = (
        df["order_date"].dt.isocalendar().year.astype("Int64").astype(str)
        + "-W"
        + df["ISO Week"].astype("Int64").astype(str).str.zfill(2)
    )

    dup = (
        df.groupby(["bc_order", "material_number"])["batch_number"]
        .nunique(dropna=True)
        .reset_index(name="Batch Count")
    )
    dup["Is Duplicate"] = dup["Batch Count"] > 1

    df = df.merge(
        dup[["bc_order", "material_number", "Batch Count", "Is Duplicate"]],
        on=["bc_order", "material_number"],
        how="left"
    )
    df["Is Duplicate"] = df["Is Duplicate"].fillna(False)
    df["Duplicate Flag"] = np.where(df["Is Duplicate"], "Duplicate", "Unique")

    orders = (
        df.groupby("bc_order")
        .agg(
            OrderDate=("order_date", "min"),
            ClubName=("club_name", first_nonblank),
            OrderType=("order_type", first_nonblank),
            SalesDocument=("sales_document", first_nonblank),
            AnyOrderStatus=("order_status", lambda s: list(s)),
            AnyWorkflowStatus=("workflow_status", lambda s: list(s)),
            DuplicateLines=("Is Duplicate", "sum"),
            TotalLines=("material_number", "size")
        )
        .reset_index()
    )

    orders["Order Category"] = orders.apply(
        lambda r: categorize_order(r["AnyOrderStatus"], r["AnyWorkflowStatus"]),
        axis=1
    )

    orders["Month"] = orders["OrderDate"].dt.strftime("%B")
    orders["Year"] = orders["OrderDate"].dt.year
    orders["ISO Week"] = orders["OrderDate"].dt.isocalendar().week.astype("Int64")
    orders["Year-Week"] = (
        orders["OrderDate"].dt.isocalendar().year.astype("Int64").astype(str)
        + "-W"
        + orders["ISO Week"].astype("Int64").astype(str).str.zfill(2)
    )
    orders["Has Duplicate"] = np.where(orders["DuplicateLines"] > 0, "Yes", "No")

    df = df.merge(
        orders[["bc_order", "Order Category"]],
        on="bc_order",
        how="left"
    )

    weekly_status = (
        orders.groupby("Year-Week")
        .agg(
            ISO_Week=("ISO Week", "min"),
            Orders_Received=("bc_order", "count"),
            Pending_to_Ship=("Order Category", lambda s: int((s == "Open Still").sum())),
            Week_Status=("Order Category", lambda s: week_status_from_categories(list(s)))
        )
        .reset_index()
        .sort_values(["ISO_Week", "Year-Week"])
    )

    return df, orders, weekly_status

st.markdown("""
<div style='font-size: 2rem; font-weight: 800; color: white; margin-left: 5px;'>
FBB Order Command Center
</div>
""", unsafe_allow_html=True)
st.markdown("<div class='sub-title'>Centralized database view. Only admin updates data.</div>", unsafe_allow_html=True)

df, orders, weekly_status = load_data()

if df is None or orders is None or weekly_status is None:
    st.warning("No data found in Supabase table `order_data`.")
    st.stop()

with st.sidebar:
    st.header("SAP-Style Filters")
    page = st.radio("View", ["Dashboard", "Analyzer"], index=0)

    clubs = ["All"] + sorted([x for x in orders["ClubName"].dropna().astype(str).unique().tolist() if x.strip()])
    months_sorted = ["January","February","March","April","May","June","July","August","September","October","November","December"]
    months = ["All"] + [m for m in months_sorted if m in set(orders["Month"].dropna())]
    order_types = ["All"] + sorted([x for x in orders["OrderType"].dropna().astype(str).unique().tolist() if x.strip()])
    year_weeks = ["All"] + sorted([x for x in orders["Year-Week"].dropna().astype(str).unique().tolist() if x.strip()])

    selected_club = st.selectbox("Club", clubs, index=0)
    selected_month = st.selectbox("Month", months, index=0)
    selected_order_type = st.selectbox("Order Type", order_types, index=0)
    selected_year_week = st.selectbox("Week", year_weeks, index=0)

    if st.button("Refresh Data"):
        st.cache_data.clear()
        st.rerun()

orders_f = orders.copy()
if selected_club != "All":
    orders_f = orders_f[orders_f["ClubName"] == selected_club]
if selected_month != "All":
    orders_f = orders_f[orders_f["Month"] == selected_month]
if selected_order_type != "All":
    orders_f = orders_f[orders_f["OrderType"] == selected_order_type]
if selected_year_week != "All":
    orders_f = orders_f[orders_f["Year-Week"] == selected_year_week]

df_f = df[df["bc_order"].isin(orders_f["bc_order"])].copy()
weekly_status_f = weekly_status.copy()
if selected_year_week != "All":
    weekly_status_f = weekly_status_f[weekly_status_f["Year-Week"] == selected_year_week]

if page == "Dashboard":
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    with c1: metric_block("Total Orders", len(orders_f), "Distinct orders")
    with c2: metric_block("Shipped", int((orders_f["Order Category"] == "Shipped").sum()), "Distinct orders")
    with c3: metric_block("Open Still", int((orders_f["Order Category"] == "Open Still").sum()), "Distinct orders")
    with c4: metric_block("Cancelled", int((orders_f["Order Category"] == "Cancelled").sum()), "Distinct orders")
    with c5: metric_block("Switched to PA", int((orders_f["Order Category"] == "Switched to PA").sum()), "Distinct orders")
    with c6: metric_block("Duplicate Orders", int((orders_f["Has Duplicate"] == "Yes").sum()), "Same order + material + different batch")

    st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)

    c7, c8, c9 = st.columns(3)
    with c7: metric_block("Open Weeks", int((weekly_status_f["Week_Status"] == "Open").sum()) if len(weekly_status_f) else 0, "Weeks not fully closed")
    with c8: metric_block("Closed Weeks", int((weekly_status_f["Week_Status"] == "Closed").sum()) if len(weekly_status_f) else 0, "Weeks fully completed")
    with c9: metric_block("Pending to Ship", int((orders_f["Order Category"] == "Open Still").sum()), "Open orders still pending")

    left, right = st.columns([1.2, 1])

    with left:
        st.markdown("<div class='section-card'>", unsafe_allow_html=True)
        st.markdown("<div class='section-title'>Weekly Orders Received vs Pending to Ship</div>", unsafe_allow_html=True)
        weekly_chart = (
            orders_f.groupby("Year-Week")
            .agg(
                Orders_Received=("bc_order", "count"),
                Pending_to_Ship=("Order Category", lambda s: int((s == "Open Still").sum()))
            )
            .reset_index()
        )
        fig = go.Figure()
        fig.add_bar(x=weekly_chart["Year-Week"], y=weekly_chart["Orders_Received"], name="Orders Received")
        fig.add_scatter(x=weekly_chart["Year-Week"], y=weekly_chart["Pending_to_Ship"], name="Pending to Ship", mode="lines+markers", yaxis="y2")
        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            yaxis=dict(title="Orders"),
            yaxis2=dict(title="Pending", overlaying="y", side="right"),
            legend=dict(orientation="h", y=1.1),
            height=420,
            margin=dict(l=20, r=20, t=20, b=20)
        )
        st.plotly_chart(fig, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with right:
        st.markdown("<div class='section-card'>", unsafe_allow_html=True)
        st.markdown("<div class='section-title'>Order Status Split</div>", unsafe_allow_html=True)
        status_counts = orders_f["Order Category"].value_counts().reindex(["Shipped", "Open Still", "Cancelled", "Switched to PA", "Other"], fill_value=0).reset_index()
        status_counts.columns = ["Category", "Orders"]
        fig2 = px.pie(status_counts, names="Category", values="Orders", hole=0.58)
        fig2.update_layout(
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            height=420,
            margin=dict(l=20, r=20, t=20, b=20)
        )
        st.plotly_chart(fig2, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    bottom_left, bottom_right = st.columns([1.1, 0.9])

    with bottom_left:
        st.markdown("<div class='section-card'>", unsafe_allow_html=True)
        st.markdown("<div class='section-title'>Top Clubs by Orders</div>", unsafe_allow_html=True)
        clubs_df = (
            orders_f.groupby("ClubName")["bc_order"]
            .count()
            .reset_index(name="Orders")
            .sort_values("Orders", ascending=False)
            .head(10)
        )
        if len(clubs_df) > 0:
            fig3 = px.bar(clubs_df, x="Orders", y="ClubName", orientation="h")
            fig3.update_layout(
                template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                yaxis={"categoryorder": "total ascending"},
                height=400,
                margin=dict(l=20, r=20, t=20, b=20)
            )
            st.plotly_chart(fig3, use_container_width=True)
        else:
            st.info("No club data available for current filter.")
        st.markdown("</div>", unsafe_allow_html=True)

    with bottom_right:
        st.markdown("<div class='section-card'>", unsafe_allow_html=True)
        st.markdown("<div class='section-title'>Week Close Monitor</div>", unsafe_allow_html=True)
        open_weeks_df = weekly_status_f[weekly_status_f["Week_Status"] == "Open"].copy()
        closed_weeks_df = weekly_status_f[weekly_status_f["Week_Status"] == "Closed"].copy()
        latest_date = orders_f["OrderDate"].max() if len(orders_f) else pd.NaT

        st.write(f"Source rows: **{len(df_f):,}**")
        st.write(f"Unique orders: **{len(orders_f):,}**")
        st.write(f"Latest order date: **{latest_date.strftime('%d-%b-%Y') if pd.notna(latest_date) else '-'}**")
        st.write(f"Duplicate rows found: **{int(df_f['Is Duplicate'].sum()):,}**")

        st.markdown("**Open Weeks**")
        if len(open_weeks_df) > 0:
            for week in open_weeks_df["Year-Week"].tolist():
                st.markdown(f"<span class='status-pill-open'>{week}</span>", unsafe_allow_html=True)
        else:
            st.markdown("<span class='status-pill-closed'>All weeks closed</span>", unsafe_allow_html=True)

        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
        st.markdown("**Closed Weeks**")
        if len(closed_weeks_df) > 0:
            st.write(", ".join(closed_weeks_df["Year-Week"].astype(str).tolist()[:10]))
        else:
            st.write("-")
        st.markdown("</div>", unsafe_allow_html=True)

else:
    st.markdown("<div class='section-card'>", unsafe_allow_html=True)
    st.markdown("<div class='section-title'>Analyzer</div>", unsafe_allow_html=True)

    a1, a2, a3, a4 = st.columns(4)
    with a1:
        status_filter = st.multiselect(
            "Order Category",
            sorted(df_f["Order Category"].dropna().unique()),
            default=sorted(df_f["Order Category"].dropna().unique())
        )
    with a2:
        dup_filter = st.selectbox("Duplicate Flag", ["All", "Duplicate", "Unique"], index=0)
    with a3:
        weeks_available = sorted([str(x) for x in df_f["Year-Week"].dropna().unique()])
        week_filter = st.multiselect("Year-Week", weeks_available, default=weeks_available)
    with a4:
        search_order = st.text_input("Search Order No")

    b1, b2, b3 = st.columns(3)
    with b1:
        material_search = st.text_input("Search Material")
    with b2:
        sales_doc_search = st.text_input("Search Sales Document")
    with b3:
        cleaned_export_name = st.text_input("Export file name", value="filtered_analyzer.xlsx")

    view = df_f.copy()

    if status_filter:
        view = view[view["Order Category"].isin(status_filter)]
    if dup_filter != "All":
        view = view[view["Duplicate Flag"] == dup_filter]
    if week_filter:
        view = view[view["Year-Week"].astype(str).isin(week_filter)]
    if search_order.strip():
        view = view[view["bc_order"].astype(str).str.contains(search_order.strip(), case=False, na=False)]
    if material_search.strip():
        view = view[view["material_number"].astype(str).str.contains(material_search.strip(), case=False, na=False)]
    if sales_doc_search.strip():
        view = view[view["sales_document"].astype(str).str.contains(sales_doc_search.strip(), case=False, na=False)]

    preferred_cols = [
        "bc_order", "sales_document", "club_name", "order_type", "material_number",
        "batch_number", "order_status", "workflow_status", "Order Category",
        "order_date", "cdd", "Month", "ISO Week", "Year-Week",
        "Batch Count", "Duplicate Flag"
    ]
    display_cols = [c for c in preferred_cols if c in view.columns]
    view = view[display_cols].copy()

    st.write(f"Filtered rows: **{len(view):,}**")
    st.dataframe(view, use_container_width=True, height=560)

    download_col1, download_col2 = st.columns([1, 1])
    with download_col1:
        export_name = cleaned_export_name.strip() or "filtered_analyzer.xlsx"
        if not export_name.lower().endswith(".xlsx"):
            export_name = f"{export_name}.xlsx"
        st.download_button(
            "Download filtered analyzer as Excel",
            data=to_excel_bytes(view),
            file_name=export_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    with download_col2:
        st.download_button(
            "Download current view as CSV",
            data=view.to_csv(index=False).encode("utf-8"),
            file_name="filtered_analyzer.csv",
            mime="text/csv"
        )

    st.markdown("</div>", unsafe_allow_html=True)
