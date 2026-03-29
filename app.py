import io
import re
import math
import hashlib
import hmac
from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st
from supabase import create_client, Client


# =========================================================
# PAGE CONFIG
# =========================================================
st.set_page_config(
    page_title="Operations Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =========================================================
# STYLES
# =========================================================
st.markdown("""
<style>
.block-container {
    padding-top: 2.8rem !important;
    padding-bottom: 2rem !important;
    padding-left: 2rem !important;
    padding-right: 2rem !important;
}
h1, h2, h3 {
    padding-top: 0.15rem !important;
    margin-top: 0 !important;
}
.page-title {
    font-size: 2.15rem;
    font-weight: 800;
    margin-bottom: 0.35rem;
    line-height: 1.25;
}
.page-subtitle {
    color: #9aa0a6;
    font-size: 1rem;
    margin-bottom: 1rem;
}
.card {
    border: 1px solid rgba(140, 140, 140, 0.22);
    border-radius: 18px;
    padding: 20px 18px;
    background: rgba(255,255,255,0.03);
    min-height: 138px;
    display: flex;
    flex-direction: column;
    justify-content: center;
}
.metric-card {
    border: 1px solid rgba(140, 140, 140, 0.22);
    border-radius: 16px;
    padding: 14px 16px;
    background: rgba(255,255,255,0.03);
    min-height: 92px;
}
.small-muted {
    color: #9aa0a6;
    font-size: 0.92rem;
    margin-bottom: 6px;
}
.big-number {
    font-size: 1.75rem;
    font-weight: 750;
    line-height: 1.2;
}
div[data-testid="stMetric"] {
    background: rgba(255,255,255,0.02);
    border: 1px solid rgba(140, 140, 140, 0.15);
    padding: 14px 16px;
    border-radius: 16px;
}
div[data-testid="stDownloadButton"] > button,
div[data-testid="stButton"] > button,
div[data-testid="baseButton-secondary"] {
    border-radius: 12px !important;
    min-height: 42px !important;
}
</style>
""", unsafe_allow_html=True)

# =========================================================
# CONFIG
# =========================================================
EXPORT_CHUNK_ROWS = 500
DB_INSERT_CHUNK = 40
MAX_OPEN_ORDER_ROWS = 3000
MAX_DUP_ROWS = 3000
MAX_INVOICE_DETAIL_ROWS = 3000
TOP_SHIPMENT_REF_CHART_ROWS = 50

DATASETS = {
    "order_dashboard": {
        "label": "Order Dashboard",
        "download_name": "Order_Dashboard.xlsx",
        "cleanup_tables": [
            "dataset_export_chunks",
            "dataset_metrics",
            "order_weekly_summary",
            "order_open_orders",
            "order_duplicate_lines",
        ],
    },
    "fbb_shipment_details": {
        "label": "FBB-Shipment Details",
        "download_name": "FBB_Shipment_Details.xlsx",
        "cleanup_tables": [
            "dataset_export_chunks",
            "dataset_metrics",
            "shipment_weekly_summary",
            "shipment_ref_summary",
        ],
    },
    "fbb_invoice_status": {
        "label": "FBB Invoice Status",
        "download_name": "FBB_Invoice_Status.xlsx",
        "cleanup_tables": [
            "dataset_export_chunks",
            "dataset_metrics",
            "invoice_status_summary",
            "invoice_team_summary",
            "invoice_detail_compact",
        ],
    },
}

# =========================================================
# BASIC HELPERS
# =========================================================
def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def safe_equal(a: str, b: str) -> bool:
    return hmac.compare_digest(a or "", b or "")


def init_state():
    if "page" not in st.session_state:
        st.session_state.page = "home"
    if "admin_logged_in" not in st.session_state:
        st.session_state.admin_logged_in = False
    if "admin_user" not in st.session_state:
        st.session_state.admin_user = ""
    if "export_ready_for" not in st.session_state:
        st.session_state.export_ready_for = None


@st.cache_resource
def get_supabase() -> Client:
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])


def current_admin_name():
    return st.secrets.get("ADMIN_DISPLAY_NAME", st.secrets.get("ADMIN_USERNAME", "Admin"))


def first_existing_column(df: pd.DataFrame, candidates):
    for col in candidates:
        if col in df.columns:
            return col
    return None


def parse_numeric_series(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def normalize_value_for_json(value: Any):
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    return value


def safe_text(value: Any):
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    return str(value)


def safe_num(value: Any):
    if pd.isna(value):
        return None
    try:
        num = float(value)
        if math.isfinite(num):
            return num
        return None
    except Exception:
        return None


def trim_text(value: Any, max_len: int):
    text = safe_text(value)
    if text is None:
        return None
    return text[:max_len]


def batched(seq, size=DB_INSERT_CHUNK):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def clear_caches():
    load_active_upload_meta.clear()
    load_metrics_map.clear()
    load_table_records.clear()
    load_export_df.clear()


def insert_in_chunks(table_name: str, rows: list[dict], chunk_size: int = DB_INSERT_CHUNK):
    if not rows:
        return
    sb = get_supabase()
    for chunk in batched(rows, chunk_size):
        sb.table(table_name).insert(chunk).execute()


def deactivate_old_uploads(dataset_key: str, new_upload_id: int) -> list[int]:
    sb = get_supabase()
    resp = (
        sb.table("app_uploads")
        .select("id")
        .eq("dataset_key", dataset_key)
        .eq("is_active", True)
        .neq("id", new_upload_id)
        .execute()
    )
    old_ids = [r["id"] for r in (resp.data or [])]
    if old_ids:
        sb.table("app_uploads").update({"is_active": False}).in_("id", old_ids).execute()
    return old_ids


def delete_old_upload_related_data(old_upload_ids: list[int], dataset_key: str):
    if not old_upload_ids:
        return
    sb = get_supabase()
    for table_name in DATASETS[dataset_key]["cleanup_tables"]:
        sb.table(table_name).delete().in_("upload_id", old_upload_ids).execute()


def week_sort_parts(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return (999999, 999999, "")

    text = str(value).strip()
    main_match = re.search(r"(\d+)", text)
    main_num = int(main_match.group(1)) if main_match else 999999

    suffix_match = re.search(r"#(\d+)", text)
    suffix_num = int(suffix_match.group(1)) if suffix_match else 0

    return (main_num, suffix_num, text.lower())


def sort_week_dataframe(df: pd.DataFrame, week_col: str) -> pd.DataFrame:
    if df.empty or week_col not in df.columns:
        return df
    tmp = df.copy()
    tmp["_week_sort"] = tmp[week_col].apply(week_sort_parts)
    tmp = tmp.sort_values("_week_sort").drop(columns=["_week_sort"])
    tmp = tmp.reset_index(drop=True)
    return tmp


def search_dataframe(df: pd.DataFrame, query: str) -> pd.DataFrame:
    if not query or df.empty:
        return df
    mask = df.astype(str).apply(lambda col: col.str.contains(query, case=False, na=False))
    return df[mask.any(axis=1)]


def clean_export_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = out[col].dt.strftime("%Y-%m-%d")
        elif out[col].dtype == object:
            def fix_obj(v):
                if pd.isna(v):
                    return None
                if isinstance(v, pd.Timestamp):
                    return v.strftime("%Y-%m-%d")
                return v
            out[col] = out[col].apply(fix_obj)
    return out


def dataframe_to_export_chunks(df: pd.DataFrame) -> list[list[dict]]:
    normalized_records = []
    for _, row in df.iterrows():
        row_dict = {col: normalize_value_for_json(row[col]) for col in df.columns}
        normalized_records.append(row_dict)

    chunks = []
    for i in range(0, len(normalized_records), EXPORT_CHUNK_ROWS):
        chunks.append(normalized_records[i:i + EXPORT_CHUNK_ROWS])
    return chunks


# =========================================================
# DB READ
# =========================================================
@st.cache_data(ttl=600)
def load_active_upload_meta(dataset_key: str):
    sb = get_supabase()
    resp = (
        sb.table("app_uploads")
        .select("id,dataset_key,original_filename,uploaded_by,uploaded_at,row_count,column_order,sheet_name,is_active")
        .eq("dataset_key", dataset_key)
        .eq("is_active", True)
        .order("id", desc=True)
        .limit(1)
        .execute()
    )
    if not resp.data:
        return None
    return resp.data[0]


@st.cache_data(ttl=600)
def load_metrics_map(upload_id: int):
    sb = get_supabase()
    resp = (
        sb.table("dataset_metrics")
        .select("metric_key,metric_num,metric_text")
        .eq("upload_id", upload_id)
        .execute()
    )
    metrics = {}
    for row in (resp.data or []):
        if row.get("metric_num") is not None:
            metrics[row["metric_key"]] = row["metric_num"]
        else:
            metrics[row["metric_key"]] = row.get("metric_text")
    return metrics


@st.cache_data(ttl=600)
def load_table_records(table_name: str, upload_id: int, limit_rows: int | None = None):
    sb = get_supabase()
    query = sb.table(table_name).select("*").eq("upload_id", upload_id).order("id")
    if limit_rows is not None:
        query = query.limit(limit_rows)
    resp = query.execute()
    return resp.data or []


@st.cache_data(ttl=600)
def load_export_df(upload_id: int, column_order: tuple):
    sb = get_supabase()

    all_chunks = []
    last_chunk_index = -1
    page_size = 1000

    while True:
        resp = (
            sb.table("dataset_export_chunks")
            .select("chunk_index,chunk_data")
            .eq("upload_id", upload_id)
            .gt("chunk_index", last_chunk_index)
            .order("chunk_index")
            .limit(page_size)
            .execute()
        )
        batch = resp.data or []
        if not batch:
            break
        all_chunks.extend(batch)
        last_chunk_index = batch[-1]["chunk_index"]

    records = []
    for row in all_chunks:
        chunk_data = row.get("chunk_data", [])
        if isinstance(chunk_data, list):
            records.extend(chunk_data)

    df = pd.DataFrame(records)

    ordered_cols = [c for c in column_order if c in df.columns]
    extra_cols = [c for c in df.columns if c not in ordered_cols]

    if len(df.columns) > 0:
        df = df[ordered_cols + extra_cols]
    else:
        df = pd.DataFrame(columns=list(column_order))

    return clean_export_dataframe(df)


def excel_bytes_from_df(df: pd.DataFrame, sheet_name: str):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl", datetime_format="YYYY-MM-DD") as writer:
        df.to_excel(writer, index=False, sheet_name=(sheet_name[:31] if sheet_name else "Data"))
    output.seek(0)
    return output.getvalue()


# =========================================================
# BUILD DATA
# =========================================================
def build_order_dashboard_data(df: pd.DataFrame, upload_id: int):
    order_col = first_existing_column(df, ["BC Order", "SalesDocument"])
    material_col = first_existing_column(df, ["MaterialNumber"])
    batch_col = first_existing_column(df, ["BatchNumber"])
    status_col = first_existing_column(df, ["Order Status", "Status"])
    date_col = first_existing_column(df, ["OrderDate", "BC Order Date"])
    cdd_col = first_existing_column(df, ["CDD"])
    club_col = first_existing_column(df, ["ClubName"])
    type_col = first_existing_column(df, ["OrderType"])
    sales_doc_col = first_existing_column(df, ["SalesDocument"])

    work = df.copy()

    total_orders = work[order_col].nunique() if order_col else len(work)
    status_series = work[status_col].astype(str).str.strip().str.lower() if status_col else pd.Series([], dtype="object")

    open_count = int(status_series.str.contains("open", na=False).sum()) if status_col else 0
    shipped_count = int(status_series.str.contains("ship", na=False).sum()) if status_col else 0
    cancel_count = int(status_series.str.contains("cancel", na=False).sum()) if status_col else 0

    metrics_rows = [
        {"upload_id": upload_id, "metric_key": "total_orders", "metric_num": total_orders, "metric_text": None},
        {"upload_id": upload_id, "metric_key": "open_lines", "metric_num": open_count, "metric_text": None},
        {"upload_id": upload_id, "metric_key": "shipped_lines", "metric_num": shipped_count, "metric_text": None},
        {"upload_id": upload_id, "metric_key": "cancelled_lines", "metric_num": cancel_count, "metric_text": None},
    ]

    weekly_rows = []
    if batch_col and order_col:
        weekly = (
            work.groupby(batch_col, dropna=False)[order_col]
            .nunique()
            .reset_index(name="orders_count")
        )
        if status_col:
            week_status = (
                work.groupby(batch_col)[status_col]
                .apply(lambda s: "Week Closed" if s.astype(str).str.lower().str.contains("open", na=False).sum() == 0 else "Open")
                .reset_index(name="week_state")
            )
            weekly = weekly.merge(week_status, on=batch_col, how="left")
        else:
            weekly["week_state"] = None

        weekly = sort_week_dataframe(weekly, batch_col)

        for _, row in weekly.iterrows():
            weekly_rows.append({
                "upload_id": upload_id,
                "batch_number": safe_text(row[batch_col]),
                "orders_count": int(row["orders_count"]) if pd.notna(row["orders_count"]) else None,
                "week_state": safe_text(row["week_state"]),
            })

    open_rows = []
    if status_col:
        open_df = work[work[status_col].astype(str).str.lower().str.contains("open", na=False)].copy()
    else:
        open_df = work.copy()

    open_df = open_df.head(MAX_OPEN_ORDER_ROWS)

    for _, row in open_df.iterrows():
        open_rows.append({
            "upload_id": upload_id,
            "bc_order": trim_text(row[order_col], 100) if order_col else None,
            "sales_document": trim_text(row[sales_doc_col], 100) if sales_doc_col else None,
            "material_number": trim_text(row[material_col], 150) if material_col else None,
            "status_value": trim_text(row[status_col], 100) if status_col else None,
            "batch_number": trim_text(row[batch_col], 60) if batch_col else None,
            "order_date": safe_text(row[date_col]) if date_col else None,
            "cdd": safe_text(row[cdd_col]) if cdd_col else None,
            "club_name": trim_text(row[club_col], 150) if club_col else None,
            "order_type": trim_text(row[type_col], 100) if type_col else None,
        })

    duplicate_rows = []
    if order_col and material_col:
        dup_df = work.copy()
        dup_df["dup_key"] = dup_df[order_col].astype(str).str.strip() + "||" + dup_df[material_col].astype(str).str.strip()
        dup_df["dup_count"] = dup_df.groupby("dup_key")["dup_key"].transform("count")
        duplicates = dup_df[dup_df["dup_count"] > 1].copy().head(MAX_DUP_ROWS)

        for _, row in duplicates.iterrows():
            duplicate_rows.append({
                "upload_id": upload_id,
                "bc_order": trim_text(row[order_col], 100) if order_col else None,
                "sales_document": trim_text(row[sales_doc_col], 100) if sales_doc_col else None,
                "material_number": trim_text(row[material_col], 150) if material_col else None,
                "batch_number": trim_text(row[batch_col], 60) if batch_col else None,
                "status_value": trim_text(row[status_col], 100) if status_col else None,
                "order_date": safe_text(row[date_col]) if date_col else None,
                "club_name": trim_text(row[club_col], 150) if club_col else None,
                "dup_count": int(row["dup_count"]) if pd.notna(row["dup_count"]) else None,
            })

    return metrics_rows, weekly_rows, open_rows, duplicate_rows


def build_shipment_data(df: pd.DataFrame, upload_id: int):
    order_col = first_existing_column(df, ["ORDER #"])
    qty_col = first_existing_column(df, ["Order qty"])
    week_col = first_existing_column(df, ["WEEK"])
    ship_ref_col = first_existing_column(df, ["Shipment Ref#"])

    work = df.copy()
    if qty_col:
        work[qty_col] = parse_numeric_series(work[qty_col])

    if ship_ref_col:
        shipment_ref_series = work[ship_ref_col].astype(str).str.strip()
        bd_mask = shipment_ref_series.str.upper().str.startswith("BD", na=False)
        shipped_df = work[bd_mask].copy()
    else:
        shipped_df = pd.DataFrame(columns=work.columns)

    total_rows = len(work)
    total_shipment_refs = shipped_df[ship_ref_col].nunique() if ship_ref_col and not shipped_df.empty else 0
    total_shipped_orders = shipped_df[order_col].nunique() if order_col and not shipped_df.empty else 0
    total_qty_shipped = shipped_df[qty_col].sum() if qty_col and not shipped_df.empty else 0

    metrics_rows = [
        {"upload_id": upload_id, "metric_key": "total_rows", "metric_num": total_rows, "metric_text": None},
        {"upload_id": upload_id, "metric_key": "total_shipment_refs_bd", "metric_num": total_shipment_refs, "metric_text": None},
        {"upload_id": upload_id, "metric_key": "total_shipped_orders_bd", "metric_num": total_shipped_orders, "metric_text": None},
        {"upload_id": upload_id, "metric_key": "total_qty_shipped_bd", "metric_num": float(total_qty_shipped) if pd.notna(total_qty_shipped) else 0, "metric_text": None},
    ]

    weekly_rows = []
    if week_col and qty_col and not shipped_df.empty:
        weekly = (
            shipped_df.groupby(week_col, dropna=False)[qty_col]
            .sum(min_count=1)
            .reset_index(name="total_order_qty")
        )
        weekly = sort_week_dataframe(weekly, week_col)

        for _, row in weekly.iterrows():
            weekly_rows.append({
                "upload_id": upload_id,
                "week_value": safe_text(row[week_col]),
                "total_order_qty": safe_num(row["total_order_qty"]),
            })

    ref_summary_rows = []
    if ship_ref_col and not shipped_df.empty:
        if order_col and qty_col:
            ref_summary = (
                shipped_df.groupby(ship_ref_col, dropna=False)
                .agg(unique_orders=(order_col, "nunique"), total_qty_shipped=(qty_col, "sum"))
                .reset_index()
            )
        elif order_col:
            ref_summary = (
                shipped_df.groupby(ship_ref_col, dropna=False)
                .agg(unique_orders=(order_col, "nunique"))
                .reset_index()
            )
            ref_summary["total_qty_shipped"] = None
        elif qty_col:
            ref_summary = (
                shipped_df.groupby(ship_ref_col, dropna=False)
                .agg(total_qty_shipped=(qty_col, "sum"))
                .reset_index()
            )
            ref_summary["unique_orders"] = None
        else:
            ref_summary = (
                shipped_df.groupby(ship_ref_col, dropna=False)
                .size()
                .reset_index(name="unique_orders")
            )
            ref_summary["total_qty_shipped"] = None

        for _, row in ref_summary.iterrows():
            ref_summary_rows.append({
                "upload_id": upload_id,
                "shipment_ref": safe_text(row[ship_ref_col]),
                "unique_orders": int(row["unique_orders"]) if pd.notna(row["unique_orders"]) else 0,
                "total_qty_shipped": safe_num(row["total_qty_shipped"]),
            })

    return metrics_rows, weekly_rows, ref_summary_rows


def build_invoice_data(df: pd.DataFrame, upload_id: int):
    num_orders_col = first_existing_column(df, ["Number of Orders"])
    num_invoiced_col = first_existing_column(df, ["Number of Invoiced Orders"])
    rem_orders_col = first_existing_column(df, ["Remaining Orders to Invoice"])
    total_qty_col = first_existing_column(df, ["Total Qty Shipped"])
    total_amount_col = first_existing_column(df, ["Total Amount"])
    invoiced_qty_col = first_existing_column(df, ["Invoiced Qty"])
    rem_qty_col = first_existing_column(df, ["Remaining Qty to invoice"])
    rem_amt_col = first_existing_column(df, ["Remaining Amount to invoice"])
    handover_col = first_existing_column(df, ["Hand Over"])
    pickup_col = first_existing_column(df, ["UPS Pickup Date"])
    days_col = first_existing_column(df, ["#Days"])
    status_col = first_existing_column(df, ["Status"])
    team_col = first_existing_column(df, ["Team"])
    sp_col = first_existing_column(df, ["SP#"])
    bd_col = first_existing_column(df, ["BD Ref#"])
    cs_col = first_existing_column(df, ["CS Ref#"])

    work = df.copy()

    numeric_cols = [
        num_orders_col, num_invoiced_col, rem_orders_col, total_qty_col,
        total_amount_col, invoiced_qty_col, rem_qty_col, rem_amt_col, days_col
    ]
    for c in [x for x in numeric_cols if x]:
        work[c] = parse_numeric_series(work[c])

    total_orders = float(work[num_orders_col].sum()) if num_orders_col else 0
    total_invoiced = float(work[num_invoiced_col].sum()) if num_invoiced_col else 0
    total_remaining = float(work[rem_orders_col].sum()) if rem_orders_col else 0
    total_amount = float(work[total_amount_col].sum()) if total_amount_col else 0

    metrics_rows = [
        {"upload_id": upload_id, "metric_key": "number_of_orders", "metric_num": total_orders, "metric_text": None},
        {"upload_id": upload_id, "metric_key": "invoiced_orders", "metric_num": total_invoiced, "metric_text": None},
        {"upload_id": upload_id, "metric_key": "remaining_orders", "metric_num": total_remaining, "metric_text": None},
        {"upload_id": upload_id, "metric_key": "total_amount", "metric_num": total_amount, "metric_text": None},
    ]

    status_rows = []
    if status_col:
        summary = work.groupby(status_col, dropna=False).size().reset_index(name="row_count")
        for _, row in summary.iterrows():
            status_rows.append({
                "upload_id": upload_id,
                "status_value": trim_text(row[status_col], 100),
                "row_count": int(row["row_count"]) if pd.notna(row["row_count"]) else None,
            })

    team_rows = []
    if team_col and rem_amt_col:
        summary = (
            work.groupby(team_col, dropna=False)[rem_amt_col]
            .sum(min_count=1)
            .reset_index(name="remaining_amount")
        )
        for _, row in summary.iterrows():
            team_rows.append({
                "upload_id": upload_id,
                "team_value": trim_text(row[team_col], 100),
                "remaining_amount": safe_num(row["remaining_amount"]),
            })

    compact_rows = []
    compact_df = work.head(MAX_INVOICE_DETAIL_ROWS)
    for _, row in compact_df.iterrows():
        compact_rows.append({
            "upload_id": upload_id,
            "sp_no": trim_text(row[sp_col], 100) if sp_col else None,
            "bd_ref": trim_text(row[bd_col], 100) if bd_col else None,
            "cs_ref": trim_text(row[cs_col], 100) if cs_col else None,
            "number_of_orders": safe_num(row[num_orders_col]) if num_orders_col else None,
            "number_of_invoiced_orders": safe_num(row[num_invoiced_col]) if num_invoiced_col else None,
            "remaining_orders_to_invoice": safe_num(row[rem_orders_col]) if rem_orders_col else None,
            "total_qty_shipped": safe_num(row[total_qty_col]) if total_qty_col else None,
            "total_amount": safe_num(row[total_amount_col]) if total_amount_col else None,
            "invoiced_qty": safe_num(row[invoiced_qty_col]) if invoiced_qty_col else None,
            "remaining_qty_to_invoice": safe_num(row[rem_qty_col]) if rem_qty_col else None,
            "remaining_amount_to_invoice": safe_num(row[rem_amt_col]) if rem_amt_col else None,
            "hand_over": safe_text(row[handover_col]) if handover_col else None,
            "ups_pickup_date": safe_text(row[pickup_col]) if pickup_col else None,
            "days_value": safe_num(row[days_col]) if days_col else None,
            "status_value": trim_text(row[status_col], 100) if status_col else None,
            "team_value": trim_text(row[team_col], 100) if team_col else None,
        })

    return metrics_rows, status_rows, team_rows, compact_rows


# =========================================================
# UPLOAD
# =========================================================
def upload_dataset(dataset_key: str, uploaded_file, admin_name: str):
    progress = st.progress(0, text="Reading file...")

    try:
        excel = pd.ExcelFile(uploaded_file)
        sheet_name = excel.sheet_names[0]
        df = pd.read_excel(uploaded_file, sheet_name=sheet_name)
    except Exception as e:
        progress.empty()
        return False, f"Could not read Excel file: {e}"

    df.columns = [str(c).strip() for c in df.columns]
    df = df.dropna(how="all").reset_index(drop=True)

    if df.empty:
        progress.empty()
        return False, "Uploaded file is empty after removing blank rows."

    sb = get_supabase()

    upload_payload = {
        "dataset_key": dataset_key,
        "original_filename": uploaded_file.name,
        "uploaded_by": admin_name,
        "row_count": int(len(df)),
        "column_order": list(df.columns),
        "sheet_name": sheet_name,
        "is_active": True,
    }

    try:
        progress.progress(5, text="Creating upload metadata...")
        upload_resp = sb.table("app_uploads").insert(upload_payload).execute()
        upload_id = upload_resp.data[0]["id"]

        progress.progress(15, text="Preparing export chunks...")
        export_chunks = dataframe_to_export_chunks(df)
        export_rows = []
        for idx, chunk in enumerate(export_chunks):
            export_rows.append({
                "upload_id": upload_id,
                "chunk_index": idx,
                "row_count": len(chunk),
                "chunk_data": chunk,
            })

        progress.progress(30, text="Saving export data...")
        insert_in_chunks("dataset_export_chunks", export_rows, 20)

        if dataset_key == "order_dashboard":
            progress.progress(45, text="Building order dashboard...")
            metrics_rows, weekly_rows, open_rows, duplicate_rows = build_order_dashboard_data(df, upload_id)
            insert_in_chunks("dataset_metrics", metrics_rows, DB_INSERT_CHUNK)
            insert_in_chunks("order_weekly_summary", weekly_rows, DB_INSERT_CHUNK)
            insert_in_chunks("order_open_orders", open_rows, DB_INSERT_CHUNK)
            insert_in_chunks("order_duplicate_lines", duplicate_rows, DB_INSERT_CHUNK)

        elif dataset_key == "fbb_shipment_details":
            progress.progress(45, text="Building shipment dashboard...")
            metrics_rows, weekly_rows, ref_summary_rows = build_shipment_data(df, upload_id)
            insert_in_chunks("dataset_metrics", metrics_rows, DB_INSERT_CHUNK)
            insert_in_chunks("shipment_weekly_summary", weekly_rows, DB_INSERT_CHUNK)
            insert_in_chunks("shipment_ref_summary", ref_summary_rows, DB_INSERT_CHUNK)

        elif dataset_key == "fbb_invoice_status":
            progress.progress(45, text="Building invoice dashboard...")
            metrics_rows, status_rows, team_rows, compact_rows = build_invoice_data(df, upload_id)
            insert_in_chunks("dataset_metrics", metrics_rows, DB_INSERT_CHUNK)
            insert_in_chunks("invoice_status_summary", status_rows, DB_INSERT_CHUNK)
            insert_in_chunks("invoice_team_summary", team_rows, DB_INSERT_CHUNK)
            insert_in_chunks("invoice_detail_compact", compact_rows, DB_INSERT_CHUNK)

        progress.progress(90, text="Cleaning previous active upload...")
        old_upload_ids = deactivate_old_uploads(dataset_key, upload_id)
        delete_old_upload_related_data(old_upload_ids, dataset_key)

        clear_caches()
        st.session_state.export_ready_for = None
        progress.progress(100, text="Upload complete.")
        progress.empty()

        return True, f"{DATASETS[dataset_key]['label']} uploaded successfully. Rows: {len(df):,}"

    except Exception as e:
        progress.empty()
        return False, f"Upload failed: {e}"


# =========================================================
# UI HELPERS
# =========================================================
def render_page_header(title: str, subtitle: str = ""):
    st.markdown(f'<div class="page-title">{title}</div>', unsafe_allow_html=True)
    if subtitle:
        st.markdown(f'<div class="page-subtitle">{subtitle}</div>', unsafe_allow_html=True)


def render_last_updated(upload_meta: dict | None):
    st.subheader("Last Updated")
    if not upload_meta:
        st.info("No upload found yet.")
        return

    uploaded_by = upload_meta.get("uploaded_by", "-")
    uploaded_at = upload_meta.get("uploaded_at", "")
    row_count = upload_meta.get("row_count", 0)
    filename = upload_meta.get("original_filename", "-")

    try:
        dt = pd.to_datetime(uploaded_at)
        uploaded_at_fmt = dt.strftime("%d %b %Y, %I:%M %p")
    except Exception:
        uploaded_at_fmt = str(uploaded_at)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(
            f'<div class="metric-card"><div class="small-muted">Updated by</div><div class="big-number" style="font-size:1.05rem;">{uploaded_by}</div></div>',
            unsafe_allow_html=True
        )
    with c2:
        st.markdown(
            f'<div class="metric-card"><div class="small-muted">Updated at</div><div class="big-number" style="font-size:1.05rem;">{uploaded_at_fmt}</div></div>',
            unsafe_allow_html=True
        )
    with c3:
        st.markdown(
            f'<div class="metric-card"><div class="small-muted">Rows</div><div class="big-number">{row_count:,}</div></div>',
            unsafe_allow_html=True
        )
    with c4:
        st.markdown(
            f'<div class="metric-card"><div class="small-muted">Source file</div><div class="big-number" style="font-size:1rem;">{filename}</div></div>',
            unsafe_allow_html=True
        )


def render_admin_upload_section(dataset_key: str):
    if not st.session_state.admin_logged_in:
        return

    cfg = DATASETS[dataset_key]
    st.subheader("Admin Upload")
    st.caption("Upload Excel to replace the active dataset for this page.")

    with st.form(f"upload_form_{dataset_key}", clear_on_submit=True):
        uploaded_file = st.file_uploader(
            f"Upload Excel for {cfg['label']}",
            type=["xlsx", "xls"],
            key=f"uploader_{dataset_key}"
        )
        submitted = st.form_submit_button("Upload and Replace")

        if submitted:
            if not uploaded_file:
                st.error("Please select an Excel file first.")
            else:
                ok, msg = upload_dataset(
                    dataset_key,
                    uploaded_file,
                    st.session_state.admin_user or current_admin_name()
                )
                if ok:
                    st.success(msg)
                    st.rerun()
                else:
                    st.error(msg)


def render_export_section(dataset_key: str, upload_meta: dict | None):
    st.subheader("Export")

    if not upload_meta:
        st.warning("No data available for export.")
        return

    upload_id = int(upload_meta["id"])
    original_filename = upload_meta.get("original_filename", DATASETS[dataset_key]["download_name"])
    sheet_name = upload_meta.get("sheet_name", DATASETS[dataset_key]["label"])
    column_order = tuple(upload_meta.get("column_order", []) or [])

    prep_col, dl_col = st.columns([1, 2])

    with prep_col:
        if st.button("Prepare export file", key=f"prepare_export_{dataset_key}", use_container_width=True):
            st.session_state.export_ready_for = dataset_key

    with dl_col:
        if st.session_state.export_ready_for == dataset_key:
            with st.spinner("Preparing export..."):
                export_df = load_export_df(upload_id, column_order)
                export_bytes = excel_bytes_from_df(export_df, sheet_name)

            st.download_button(
                label="⬇️ Download current dataset",
                data=export_bytes,
                file_name=original_filename,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                key=f"download_export_{dataset_key}"
            )
        else:
            st.info("Click 'Prepare export file' to enable download.")


def admin_sidebar():
    with st.sidebar:
        st.title("Control Panel")
        st.caption("Production dashboard")

        if st.session_state.admin_logged_in:
            st.success(f"Admin logged in: {st.session_state.admin_user}")
            if st.button("Logout", use_container_width=True):
                st.session_state.admin_logged_in = False
                st.session_state.admin_user = ""
                st.rerun()
        else:
            st.markdown("### Admin Login")
            username = st.text_input("Username", key="login_username")
            password = st.text_input("Password", type="password", key="login_password")

            if st.button("Login", use_container_width=True):
                user_ok = safe_equal(username, st.secrets["ADMIN_USERNAME"])
                pw_ok = safe_equal(sha256_text(password), st.secrets["ADMIN_PASSWORD_HASH"])

                if user_ok and pw_ok:
                    st.session_state.admin_logged_in = True
                    st.session_state.admin_user = current_admin_name()
                    st.success("Admin login successful.")
                    st.rerun()
                else:
                    st.error("Invalid username or password.")

        st.divider()
        st.markdown("### Navigation")

        if st.button("🏠 Home", use_container_width=True):
            st.session_state.page = "home"
            st.rerun()
        if st.button("📦 Order Dashboard", use_container_width=True):
            st.session_state.page = "order_dashboard"
            st.rerun()
        if st.button("🚚 FBB-Shipment Details", use_container_width=True):
            st.session_state.page = "fbb_shipment_details"
            st.rerun()
        if st.button("🧾 FBB Invoice Status", use_container_width=True):
            st.session_state.page = "fbb_invoice_status"
            st.rerun()


# =========================================================
# PAGES
# =========================================================
def home_page():
    render_page_header("Operations Dashboard", "Select a dashboard.")

    c1, c2, c3 = st.columns(3)

    with c1:
        st.markdown("""
        <div class="card">
            <div>
                <h3 style="margin-bottom:10px;">Order Dashboard</h3>
                <p style="margin:0;">Order KPIs, weekly view, open orders, duplicate checker.</p>
            </div>
        </div>
        """, unsafe_allow_html=True)
        if st.button("Open Order Dashboard", key="go_order", use_container_width=True):
            st.session_state.page = "order_dashboard"
            st.rerun()

    with c2:
        st.markdown("""
        <div class="card">
            <div>
                <h3 style="margin-bottom:10px;">FBB-Shipment Details</h3>
                <p style="margin:0;">Shipment overview by Shipment Ref# and week.</p>
            </div>
        </div>
        """, unsafe_allow_html=True)
        if st.button("Open FBB-Shipment Details", key="go_ship", use_container_width=True):
            st.session_state.page = "fbb_shipment_details"
            st.rerun()

    with c3:
        st.markdown("""
        <div class="card">
            <div>
                <h3 style="margin-bottom:10px;">FBB Invoice Status</h3>
                <p style="margin:0;">Invoice progress, remaining quantity, status/team analysis.</p>
            </div>
        </div>
        """, unsafe_allow_html=True)
        if st.button("Open FBB Invoice Status", key="go_invoice", use_container_width=True):
            st.session_state.page = "fbb_invoice_status"
            st.rerun()

    st.divider()
    st.info("Viewer mode can analyze and export. Only admin can upload and replace data.")


def page_order_dashboard():
    render_page_header("Order Dashboard", "Order-level summary, week analysis, open lines and duplicate check.")

    upload_meta = load_active_upload_meta("order_dashboard")
    render_last_updated(upload_meta)
    st.markdown("<br>", unsafe_allow_html=True)
    render_admin_upload_section("order_dashboard")
    st.markdown("<br>", unsafe_allow_html=True)
    render_export_section("order_dashboard", upload_meta)
    st.divider()

    if not upload_meta:
        st.warning("No Order Dashboard data uploaded yet.")
        return

    upload_id = int(upload_meta["id"])
    metrics = load_metrics_map(upload_id)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Orders", f"{int(metrics.get('total_orders', 0)):,}")
    c2.metric("Open Lines", f"{int(metrics.get('open_lines', 0)):,}")
    c3.metric("Shipped Lines", f"{int(metrics.get('shipped_lines', 0)):,}")
    c4.metric("Cancelled Lines", f"{int(metrics.get('cancelled_lines', 0)):,}")

    weekly_rows = load_table_records("order_weekly_summary", upload_id)
    if weekly_rows:
        weekly_df = pd.DataFrame(weekly_rows)[["batch_number", "orders_count", "week_state"]]
        weekly_df = sort_week_dataframe(weekly_df, "batch_number")
        st.subheader("Orders by Week / Batch")
        fig = px.bar(weekly_df, x="batch_number", y="orders_count", hover_data=weekly_df.columns)
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(weekly_df, use_container_width=True, height=260)

    st.subheader("Open Orders")
    open_rows = load_table_records("order_open_orders", upload_id, limit_rows=MAX_OPEN_ORDER_ROWS)
    if open_rows:
        open_df = pd.DataFrame(open_rows)[[
            "bc_order", "sales_document", "material_number", "status_value",
            "batch_number", "order_date", "cdd", "club_name", "order_type"
        ]]
        search_text = st.text_input("Search Open Orders", key="search_open_orders")
        open_df = search_dataframe(open_df, search_text)
        st.caption(f"Showing up to {MAX_OPEN_ORDER_ROWS:,} open-order rows for faster viewing.")
        st.dataframe(open_df, use_container_width=True, height=360)
    else:
        st.info("No open orders found.")

    st.subheader("Duplicate Line Checker")
    dup_rows = load_table_records("order_duplicate_lines", upload_id, limit_rows=MAX_DUP_ROWS)
    if dup_rows:
        dup_df = pd.DataFrame(dup_rows)[[
            "bc_order", "sales_document", "material_number", "batch_number",
            "status_value", "order_date", "club_name", "dup_count"
        ]]
        dup_search = st.text_input("Search Duplicate Lines", key="search_duplicate_lines")
        dup_df = search_dataframe(dup_df, dup_search)
        st.caption(f"Showing up to {MAX_DUP_ROWS:,} duplicate rows for faster viewing.")
        st.dataframe(dup_df, use_container_width=True, height=340)
    else:
        st.success("No duplicate order + material combinations found.")


def page_fbb_shipment_details():
    render_page_header("FBB-Shipment Details", "Shipment overview by BD Shipment Ref#.")

    upload_meta = load_active_upload_meta("fbb_shipment_details")
    render_last_updated(upload_meta)
    st.markdown("<br>", unsafe_allow_html=True)
    render_admin_upload_section("fbb_shipment_details")
    st.markdown("<br>", unsafe_allow_html=True)
    render_export_section("fbb_shipment_details", upload_meta)
    st.divider()

    if not upload_meta:
        st.warning("No FBB-Shipment Details data uploaded yet.")
        return

    upload_id = int(upload_meta["id"])
    metrics = load_metrics_map(upload_id)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Rows", f"{int(metrics.get('total_rows', 0)):,}")
    c2.metric("BD Shipment Refs", f"{int(metrics.get('total_shipment_refs_bd', 0)):,}")
    c3.metric("Unique Shipped Orders", f"{int(metrics.get('total_shipped_orders_bd', 0)):,}")
    c4.metric("Total Qty Shipped", f"{float(metrics.get('total_qty_shipped_bd', 0)):,.0f}")

    weekly_rows = load_table_records("shipment_weekly_summary", upload_id)
    if weekly_rows:
        weekly_df = pd.DataFrame(weekly_rows)[["week_value", "total_order_qty"]]
        weekly_df = sort_week_dataframe(weekly_df, "week_value")

        st.subheader("Qty Shipped by Week")
        fig = px.bar(weekly_df, x="week_value", y="total_order_qty", hover_data=weekly_df.columns)
        st.plotly_chart(fig, use_container_width=True)

    ref_rows = load_table_records("shipment_ref_summary", upload_id)
    if ref_rows:
        ref_df = pd.DataFrame(ref_rows)[["shipment_ref", "unique_orders", "total_qty_shipped"]]

        st.subheader("Shipment Reference Overview")
        search_text = st.text_input("Search Shipment Ref#", key="search_shipment_ref")
        ref_df = search_dataframe(ref_df, search_text)

        chart_df = ref_df.head(TOP_SHIPMENT_REF_CHART_ROWS).copy()
        if not chart_df.empty:
            st.subheader("Top Shipment References by Qty Shipped")
            fig = px.bar(chart_df, x="shipment_ref", y="total_qty_shipped", hover_data=chart_df.columns)
            st.plotly_chart(fig, use_container_width=True)

        st.dataframe(ref_df, use_container_width=True, height=420)
    else:
        st.info("No BD shipment references found.")


def page_fbb_invoice_status():
    render_page_header("FBB Invoice Status", "Invoice progress, remaining quantity and team/status analysis.")

    upload_meta = load_active_upload_meta("fbb_invoice_status")
    render_last_updated(upload_meta)
    st.markdown("<br>", unsafe_allow_html=True)
    render_admin_upload_section("fbb_invoice_status")
    st.markdown("<br>", unsafe_allow_html=True)
    render_export_section("fbb_invoice_status", upload_meta)
    st.divider()

    if not upload_meta:
        st.warning("No FBB Invoice Status data uploaded yet.")
        return

    upload_id = int(upload_meta["id"])
    metrics = load_metrics_map(upload_id)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Number of Orders", f"{int(metrics.get('number_of_orders', 0)):,}")
    c2.metric("Invoiced Orders", f"{int(metrics.get('invoiced_orders', 0)):,}")
    c3.metric("Remaining Orders", f"{int(metrics.get('remaining_orders', 0)):,}")
    c4.metric("Total Amount", f"{float(metrics.get('total_amount', 0)):,.2f}")

    status_rows = load_table_records("invoice_status_summary", upload_id)
    if status_rows:
        status_df = pd.DataFrame(status_rows)[["status_value", "row_count"]]
        st.subheader("By Status")
        fig = px.bar(status_df, x="status_value", y="row_count", hover_data=status_df.columns)
        st.plotly_chart(fig, use_container_width=True)

    team_rows = load_table_records("invoice_team_summary", upload_id)
    if team_rows:
        team_df = pd.DataFrame(team_rows)[["team_value", "remaining_amount"]]
        st.subheader("Remaining Amount by Team")
        fig = px.bar(team_df, x="team_value", y="remaining_amount", hover_data=team_df.columns)
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Invoice Detail Table")
    detail_rows = load_table_records("invoice_detail_compact", upload_id, limit_rows=MAX_INVOICE_DETAIL_ROWS)
    if detail_rows:
        detail_df = pd.DataFrame(detail_rows)[[
            "sp_no", "bd_ref", "cs_ref", "number_of_orders", "number_of_invoiced_orders",
            "remaining_orders_to_invoice", "total_qty_shipped", "total_amount",
            "invoiced_qty", "remaining_qty_to_invoice", "remaining_amount_to_invoice",
            "hand_over", "ups_pickup_date", "days_value", "status_value", "team_value"
        ]]
        invoice_search = st.text_input("Search Invoice Details", key="search_invoice_details")
        detail_df = search_dataframe(detail_df, invoice_search)
        st.caption(f"Showing up to {MAX_INVOICE_DETAIL_ROWS:,} rows for faster viewing.")
        st.dataframe(detail_df, use_container_width=True, height=420)
    else:
        st.info("No invoice rows found.")


# =========================================================
# MAIN
# =========================================================
def main():
    init_state()
    admin_sidebar()

    page = st.session_state.page

    if page == "home":
        home_page()
    elif page == "order_dashboard":
        page_order_dashboard()
    elif page == "fbb_shipment_details":
        page_fbb_shipment_details()
    elif page == "fbb_invoice_status":
        page_fbb_invoice_status()
    else:
        home_page()


if __name__ == "__main__":
    main()
