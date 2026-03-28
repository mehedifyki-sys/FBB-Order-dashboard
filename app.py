
import io
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(
    page_title="FBB Order Command Center",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
:root {
    --bg: #0B1220;
    --panel: #111827;
    --panel-2: #172033;
    --card: #0F172A;
    --border: #243045;
    --text: #E5E7EB;
    --muted: #94A3B8;
    --accent: #38BDF8;
    --accent-2: #22C55E;
    --warn: #F59E0B;
    --danger: #EF4444;
}
.stApp {
    background: linear-gradient(180deg, #08101d 0%, #0b1220 100%);
    color: var(--text);
}
.block-container {
    padding-top: 1.2rem;
    padding-bottom: 1rem;
}
.main-title {
    font-size: 2rem;
    font-weight: 800;
    color: white;
    margin-bottom: 0.25rem;
}
.sub-title {
    color: var(--muted);
    font-size: 0.95rem;
    margin-bottom: 1rem;
}
.kpi-card {
    background: linear-gradient(180deg, #111827 0%, #0f172a 100%);
    padding: 18px 18px;
    border-radius: 18px;
    border: 1px solid #22304a;
    box-shadow: 0 8px 30px rgba(0,0,0,0.18);
}
.kpi-label {
    font-size: 0.88rem;
    color: #A5B4FC;
    font-weight: 700;
    margin-bottom: 6px;
}
.kpi-value {
    font-size: 2rem;
    font-weight: 800;
    color: #F8FAFC;
    line-height: 1.05;
}
.small-note {
    font-size: 0.8rem;
    color: #94A3B8;
    margin-top: 8px;
}
.section-card {
    background: linear-gradient(180deg, #111827 0%, #0f172a 100%);
    padding: 16px 16px 8px 16px;
    border-radius: 18px;
    border: 1px solid #22304a;
    box-shadow: 0 8px 30px rgba(0,0,0,0.18);
}
.section-title {
    font-size: 1.05rem;
    font-weight: 800;
    color: #F8FAFC;
    margin-bottom: 8px;
}
.status-pill-open {
    display: inline-block;
    padding: 4px 10px;
    border-radius: 999px;
    background: rgba(245,158,11,0.15);
    color: #FBBF24;
    font-weight: 700;
    font-size: 0.78rem;
}
.status-pill-closed {
    display: inline-block;
    padding: 4px 10px;
    border-radius: 999px;
    background: rgba(34,197,94,0.15);
    color: #4ADE80;
    font-weight: 700;
    font-size: 0.78rem;
}
div[data-testid="stDataFrame"] {
    border: 1px solid #22304a;
    border-radius: 14px;
    overflow: hidden;
}
</style>
""", unsafe_allow_html=True)


# -----------------------------
# Header detection and mapping
# -----------------------------
HEADER_ALIASES = {
    "BC Order": [
        "BC Order", "BCOrder", "Order No", "Order Number", "Order", "Sales Order",
        "Customer Order", "Order ID", "BC Order Number"
    ],
    "MaterialNumber": [
        "MaterialNumber", "Material Number", "Material", "Material No", "Item Code",
        "SKU", "Product Code"
    ],
    "BatchNumber": [
        "BatchNumber", "Batch Number", "Batch", "Lot", "Lot Number"
    ],
    "Order Status": [
        "Order Status", "Status", "Shipment Status", "Delivery Status", "Overall Status"
    ],
    "OrderDate": [
        "OrderDate", "Order Date", "BC Order Date", "Created Date", "Document Date",
        "SO Date", "Sales Order Date", "Date"
    ],
    "ClubName": [
        "ClubName", "Club Name", "Club", "Customer", "Customer Name"
    ],
    "OrderType": [
        "OrderType", "Order Type", "Type", "Document Type", "Sales Doc Type"
    ],
    "SalesDocument": [
        "SalesDocument", "Sales Document", "Sales Doc", "SalesDocumentNumber", "SAP SO"
    ],
    "CDD": [
        "CDD", "Customer Delivery Date", "Delivery Date", "Requested Delivery Date"
    ],
    "Status": [
        "Status", "Workflow Status", "Internal Status", "Processing Status"
    ]
}


def clean_header_name(col_name: str) -> str:
    return str(col_name).strip()


def find_matching_column(actual_columns, possible_names):
    actual_lookup = {clean_header_name(c).lower(): c for c in actual_columns}
    for alias in possible_names:
        alias_key = clean_header_name(alias).lower()
        if alias_key in actual_lookup:
            return actual_lookup[alias_key]
    return None


def standardize_headers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [clean_header_name(c) for c in df.columns]

    rename_map = {}
    used_actual_columns = set()

    for standard_name, aliases in HEADER_ALIASES.items():
        match = find_matching_column(df.columns, aliases)
        if match and match not in used_actual_columns:
            rename_map[match] = standard_name
            used_actual_columns.add(match)

    df = df.rename(columns=rename_map)
    return df


def ensure_optional_columns(df: pd.DataFrame) -> pd.DataFrame:
    optional_cols = ["ClubName", "OrderType", "SalesDocument", "CDD", "Status"]
    for col in optional_cols:
        if col not in df.columns:
            df[col] = np.nan
    return df


def validate_required_columns(df: pd.DataFrame):
    required_cols = ["BC Order", "MaterialNumber", "BatchNumber", "Order Status", "OrderDate"]
    missing_cols = [c for c in required_cols if c not in df.columns]
    return missing_cols


# -----------------------------
# Utility functions
# -----------------------------
def first_nonblank(series):
    for value in series:
        if pd.notna(value) and str(value).strip():
            return value
    return None


def normalize_text_col(df: pd.DataFrame, cols):
    for col in cols:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()
    return df


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
        f"""
        <div class='kpi-card'>
            <div class='kpi-label'>{label}</div>
            <div class='kpi-value'>{value:,}</div>
            <div class='small-note'>{note}</div>
        </div>
        """,
        unsafe_allow_html=True
    )


# -----------------------------
# Data load
# -----------------------------
@st.cache_data
def load_data(uploaded_file):
    if uploaded_file.name.lower().endswith(".csv"):
        df = pd.read_csv(uploaded_file)
    else:
        df = pd.read_excel(uploaded_file)

    original_columns = list(df.columns)

    df = standardize_headers(df)
    df = ensure_optional_columns(df)

    missing_cols = validate_required_columns(df)
    if missing_cols:
        return {
            "ok": False,
            "missing_cols": missing_cols,
            "original_columns": original_columns,
            "df": None,
            "orders": None,
            "weekly_status": None
        }

    df = normalize_text_col(
        df,
        ["BC Order", "SalesDocument", "BatchNumber", "MaterialNumber",
         "Status", "Order Status", "ClubName", "OrderType"]
    )

    for date_col in ["OrderDate", "CDD"]:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

    df["Month"] = df["OrderDate"].dt.strftime("%B")
    df["Year"] = df["OrderDate"].dt.year
    df["ISO Week"] = df["OrderDate"].dt.isocalendar().week.astype("Int64")
    df["Year-Week"] = (
        df["OrderDate"].dt.isocalendar().year.astype("Int64").astype(str)
        + "-W"
        + df["ISO Week"].astype("Int64").astype(str).str.zfill(2)
    )

    # Duplicate rule:
    # same BC Order + same MaterialNumber + different BatchNumber
    dup = (
        df.groupby(["BC Order", "MaterialNumber"])["BatchNumber"]
        .nunique(dropna=True)
        .reset_index(name="Batch Count")
    )
    dup["Is Duplicate"] = dup["Batch Count"] > 1

    df = df.merge(
        dup[["BC Order", "MaterialNumber", "Batch Count", "Is Duplicate"]],
        on=["BC Order", "MaterialNumber"],
        how="left"
    )
    df["Is Duplicate"] = df["Is Duplicate"].fillna(False)
    df["Duplicate Flag"] = np.where(df["Is Duplicate"], "Duplicate", "Unique")

    orders = (
        df.groupby("BC Order")
        .agg(
            OrderDate=("OrderDate", "min"),
            ClubName=("ClubName", first_nonblank),
            OrderType=("OrderType", first_nonblank),
            SalesDocument=("SalesDocument", first_nonblank),
            AnyOrderStatus=("Order Status", lambda s: list(s)),
            AnyWorkflowStatus=("Status", lambda s: list(s)),
            DuplicateLines=("Is Duplicate", "sum"),
            TotalLines=("MaterialNumber", "size")
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
        orders[["BC Order", "Order Category"]],
        on="BC Order",
        how="left"
    )

    weekly_status = (
        orders.groupby("Year-Week")
        .agg(
            ISO_Week=("ISO Week", "min"),
            Orders_Received=("BC Order", "count"),
            Pending_to_Ship=("Order Category", lambda s: int((s == "Open Still").sum())),
            Week_Status=("Order Category", lambda s: week_status_from_categories(list(s)))
        )
        .reset_index()
        .sort_values(["ISO_Week", "Year-Week"])
    )

    return {
        "ok": True,
        "missing_cols": [],
        "original_columns": original_columns,
        "df": df,
        "orders": orders,
        "weekly_status": weekly_status
    }


# -----------------------------
# Page header
# -----------------------------
st.markdown("<div class='main-title'>FBB Order Command Center</div>", unsafe_allow_html=True)
st.markdown(
    "<div class='sub-title'>Premium dashboard for order tracking, duplicate checking, weekly close visibility, and analyzer export.</div>",
    unsafe_allow_html=True
)

uploaded = st.file_uploader("Upload Excel or CSV", type=["xlsx", "xls", "csv"])

if not uploaded:
    st.info("Upload a file to start.")
    st.stop()

result = load_data(uploaded)

if not result["ok"]:
    st.error("The uploaded file is missing required columns.")
    st.write("Missing columns:", result["missing_cols"])
    st.write("Detected columns in your uploaded file:", result["original_columns"])
    st.stop()

df = result["df"]
orders = result["orders"]
weekly_status = result["weekly_status"]


# -----------------------------
# Sidebar filters
# -----------------------------
with st.sidebar:
    st.header("SAP-Style Filters")

    page = st.radio("View", ["Dashboard", "Analyzer"], index=0)

    clubs = ["All"] + sorted(
        [x for x in orders["ClubName"].dropna().astype(str).unique().tolist() if x.strip()]
    )
    months_sorted = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ]
    months = ["All"] + [m for m in months_sorted if m in set(orders["Month"].dropna())]
    order_types = ["All"] + sorted(
        [x for x in orders["OrderType"].dropna().astype(str).unique().tolist() if x.strip()]
    )
    year_weeks = ["All"] + sorted(
        [x for x in orders["Year-Week"].dropna().astype(str).unique().tolist() if x.strip()]
    )

    selected_club = st.selectbox("Club", clubs, index=0)
    selected_month = st.selectbox("Month", months, index=0)
    selected_order_type = st.selectbox("Order Type", order_types, index=0)
    selected_year_week = st.selectbox("Week", year_weeks, index=0)

    st.markdown("---")
    st.caption("Filters apply to both Dashboard and Analyzer.")


# -----------------------------
# Filter data
# -----------------------------
orders_f = orders.copy()

if selected_club != "All":
    orders_f = orders_f[orders_f["ClubName"] == selected_club]

if selected_month != "All":
    orders_f = orders_f[orders_f["Month"] == selected_month]

if selected_order_type != "All":
    orders_f = orders_f[orders_f["OrderType"] == selected_order_type]

if selected_year_week != "All":
    orders_f = orders_f[orders_f["Year-Week"] == selected_year_week]

df_f = df[df["BC Order"].isin(orders_f["BC Order"])].copy()

weekly_status_f = weekly_status.copy()
if selected_year_week != "All":
    weekly_status_f = weekly_status_f[weekly_status_f["Year-Week"] == selected_year_week]


# -----------------------------
# Dashboard
# -----------------------------
if page == "Dashboard":
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    with c1:
        metric_block("Total Orders", len(orders_f), "Distinct orders")
    with c2:
        metric_block("Shipped", int((orders_f["Order Category"] == "Shipped").sum()), "Distinct orders")
    with c3:
        metric_block("Open Still", int((orders_f["Order Category"] == "Open Still").sum()), "Distinct orders")
    with c4:
        metric_block("Cancelled", int((orders_f["Order Category"] == "Cancelled").sum()), "Distinct orders")
    with c5:
        metric_block("Switched to PA", int((orders_f["Order Category"] == "Switched to PA").sum()), "Distinct orders")
    with c6:
        metric_block("Duplicate Orders", int((orders_f["Has Duplicate"] == "Yes").sum()), "Same order + material + different batch")

    st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)

    c7, c8, c9 = st.columns(3)
    with c7:
        open_weeks = int((weekly_status_f["Week_Status"] == "Open").sum()) if len(weekly_status_f) else 0
        metric_block("Open Weeks", open_weeks, "Weeks not fully closed")
    with c8:
        closed_weeks = int((weekly_status_f["Week_Status"] == "Closed").sum()) if len(weekly_status_f) else 0
        metric_block("Closed Weeks", closed_weeks, "Weeks fully completed")
    with c9:
        pending_lines = int((orders_f["Order Category"] == "Open Still").sum())
        metric_block("Pending to Ship", pending_lines, "Open orders still pending")

    left, right = st.columns([1.2, 1])

    with left:
        st.markdown("<div class='section-card'>", unsafe_allow_html=True)
        st.markdown("<div class='section-title'>Weekly Orders Received vs Pending to Ship</div>", unsafe_allow_html=True)

        weekly_chart = (
            orders_f.groupby("Year-Week")
            .agg(
                Orders_Received=("BC Order", "count"),
                Pending_to_Ship=("Order Category", lambda s: int((s == "Open Still").sum()))
            )
            .reset_index()
        )

        fig = go.Figure()
        fig.add_bar(
            x=weekly_chart["Year-Week"],
            y=weekly_chart["Orders_Received"],
            name="Orders Received"
        )
        fig.add_scatter(
            x=weekly_chart["Year-Week"],
            y=weekly_chart["Pending_to_Ship"],
            name="Pending to Ship",
            mode="lines+markers",
            yaxis="y2"
        )
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

        status_counts = (
            orders_f["Order Category"]
            .value_counts()
            .reindex(["Shipped", "Open Still", "Cancelled", "Switched to PA", "Other"], fill_value=0)
            .reset_index()
        )
        status_counts.columns = ["Category", "Orders"]

        fig2 = px.pie(
            status_counts,
            names="Category",
            values="Orders",
            hole=0.58
        )
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
            orders_f.groupby("ClubName")["BC Order"]
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
            closed_text = ", ".join(closed_weeks_df["Year-Week"].astype(str).tolist()[:10])
            st.write(closed_text if closed_text else "-")
        else:
            st.write("-")

        st.markdown("</div>", unsafe_allow_html=True)


# -----------------------------
# Analyzer
# -----------------------------
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
        view = view[
            view["BC Order"].astype(str).str.contains(search_order.strip(), case=False, na=False)
        ]

    if material_search.strip():
        view = view[
            view["MaterialNumber"].astype(str).str.contains(material_search.strip(), case=False, na=False)
        ]

    if sales_doc_search.strip():
        view = view[
            view["SalesDocument"].astype(str).str.contains(sales_doc_search.strip(), case=False, na=False)
        ]

    preferred_cols = [
        "BC Order", "SalesDocument", "ClubName", "OrderType", "MaterialNumber",
        "BatchNumber", "Order Status", "Status", "Order Category",
        "OrderDate", "CDD", "Month", "ISO Week", "Year-Week",
        "Batch Count", "Duplicate Flag"
    ]
    display_cols = [c for c in preferred_cols if c in view.columns]
    view = view[display_cols].copy()

    st.write(f"Filtered rows: **{len(view):,}**")

    st.dataframe(view, use_container_width=True, height=560)

    download_col1, download_col2 = st.columns([1, 1])
    with download_col1:
        st.download_button(
            "Download filtered analyzer as Excel",
            data=to_excel_bytes(view),
            file_name=cleaned_export_name if cleaned_export_name.strip().lower().endswith(".xlsx") else f"{cleaned_export_name.strip()}.xlsx",
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
