import io
import os
import re
import hashlib
import hmac
from datetime import datetime, timezone
from pathlib import Path

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
    padding-top: 0.1rem;
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
hr {
    margin-top: 1.5rem !important;
    margin-bottom: 1.5rem !important;
}
div[data-testid="stMetric"] {
    background: rgba(255,255,255,0.02);
    border: 1px solid rgba(140, 140, 140, 0.15);
    padding: 14px 16px;
    border-radius: 16px;
}
div[data-testid="stFileUploader"] {
    border-radius: 14px;
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
# DATASET CONFIG
# =========================================================
DATASETS = {
    "order_dashboard": {
        "label": "Order Dashboard",
        "download_name": "Order_Dashboard.xlsx",
    },
    "fbb_shipment_details": {
        "label": "FBB-Shipment Details",
        "download_name": "FBB_Shipment_Details.xlsx",
    },
    "fbb_invoice_status": {
        "label": "FBB Invoice Status",
        "download_name": "FBB_Invoice_Status.xlsx",
    },
}

# =========================================================
# HELPERS
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


@st.cache_resource
def get_supabase() -> Client:
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])


def current_admin_name():
    return st.secrets.get("ADMIN_DISPLAY_NAME", st.secrets.get("ADMIN_USERNAME", "Admin"))


def storage_bucket() -> str:
    return st.secrets.get("SUPABASE_STORAGE_BUCKET", "dashboard-files")


def first_existing_column(df: pd.DataFrame, candidates):
    for col in candidates:
        if col in df.columns:
            return col
    return None


def parse_date_series(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def parse_numeric_series(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def sanitize_filename(name: str) -> str:
    name = Path(name).name
    return re.sub(r"[^A-Za-z0-9._-]+", "_", name)


def timestamp_string() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def file_ext_lower(filename: str) -> str:
    return Path(filename).suffix.lower()


def excel_bytes_to_df(file_bytes: bytes):
    bio = io.BytesIO(file_bytes)
    xls = pd.ExcelFile(bio)
    sheet_name = xls.sheet_names[0]
    df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet_name)
    df.columns = [str(c).strip() for c in df.columns]
    df = df.dropna(how="all").reset_index(drop=True)
    return df, sheet_name


def clear_caches():
    load_active_upload_meta.clear()
    load_file_bytes_from_storage.clear()
    load_dataset_df.clear()


# =========================================================
# DATABASE + STORAGE READ
# =========================================================
@st.cache_data(ttl=600)
def load_active_upload_meta(dataset_key: str):
    sb = get_supabase()
    try:
        resp = (
            sb.table("app_uploads")
            .select(
                "id,dataset_key,original_filename,uploaded_by,uploaded_at,row_count,"
                "column_order,sheet_name,is_active,storage_bucket,storage_path,file_size_bytes"
            )
            .eq("dataset_key", dataset_key)
            .eq("is_active", True)
            .order("id", desc=True)
            .limit(1)
            .execute()
        )
        if not resp.data:
            return None
        return resp.data[0]
    except Exception as e:
        st.error(f"Metadata load error: {e}")
        return None


@st.cache_data(ttl=600)
def load_file_bytes_from_storage(bucket: str, path: str):
    sb = get_supabase()
    try:
        return sb.storage.from_(bucket).download(path)
    except Exception as e:
        raise RuntimeError(f"Storage download error: {e}") from e


@st.cache_data(ttl=600)
def load_dataset_df(dataset_key: str):
    upload_meta = load_active_upload_meta(dataset_key)
    if not upload_meta:
        return pd.DataFrame(), None, b""

    bucket = upload_meta["storage_bucket"]
    path = upload_meta["storage_path"]

    try:
        file_bytes = load_file_bytes_from_storage(bucket, path)
        df, _sheet_name = excel_bytes_to_df(file_bytes)
    except Exception as e:
        st.error(str(e))
        return pd.DataFrame(), upload_meta, b""

    column_order = upload_meta.get("column_order", []) or []
    ordered_cols = [c for c in column_order if c in df.columns]
    extra_cols = [c for c in df.columns if c not in ordered_cols]

    if len(df.columns) > 0:
        df = df[ordered_cols + extra_cols]
    else:
        df = pd.DataFrame(columns=column_order)

    return df, upload_meta, file_bytes


# =========================================================
# DATABASE + STORAGE WRITE
# =========================================================
def set_old_uploads_inactive(sb: Client, dataset_key: str, new_upload_id: int):
    existing = (
        sb.table("app_uploads")
        .select("id")
        .eq("dataset_key", dataset_key)
        .eq("is_active", True)
        .neq("id", new_upload_id)
        .execute()
    )
    old_ids = [r["id"] for r in (existing.data or [])]
    if old_ids:
        sb.table("app_uploads").update({"is_active": False}).in_("id", old_ids).execute()
    return old_ids


def delete_storage_file_if_exists(bucket: str, path: str):
    sb = get_supabase()
    try:
        sb.storage.from_(bucket).remove([path])
    except Exception:
        pass


def upload_dataset(dataset_key: str, uploaded_file, admin_name: str):
    cfg = DATASETS[dataset_key]
    sb = get_supabase()

    original_filename = sanitize_filename(uploaded_file.name)
    file_bytes = uploaded_file.getvalue()

    try:
        df, sheet_name = excel_bytes_to_df(file_bytes)
    except Exception as e:
        return False, f"Could not read Excel file: {e}"

    if df.empty:
        return False, "Uploaded file is empty after removing blank rows."

    bucket = storage_bucket()
    storage_path = f"{dataset_key}/{timestamp_string()}__{original_filename}"
    ext = file_ext_lower(original_filename)
    content_type = (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        if ext == ".xlsx"
        else "application/vnd.ms-excel"
    )

    try:
        sb.storage.from_(bucket).upload(
            path=storage_path,
            file=file_bytes,
            file_options={
                "content-type": content_type,
                "upsert": "false",
            },
        )
    except Exception as e:
        return False, f"Storage upload failed: {e}"

    upload_payload = {
        "dataset_key": dataset_key,
        "original_filename": original_filename,
        "uploaded_by": admin_name,
        "row_count": int(len(df)),
        "column_order": list(df.columns),
        "sheet_name": sheet_name,
        "is_active": True,
        "storage_bucket": bucket,
        "storage_path": storage_path,
        "file_size_bytes": len(file_bytes),
    }

    try:
        upload_resp = sb.table("app_uploads").insert(upload_payload).execute()
        upload_id = upload_resp.data[0]["id"]

        old_active = (
            sb.table("app_uploads")
            .select("id,storage_bucket,storage_path")
            .eq("dataset_key", dataset_key)
            .eq("is_active", True)
            .neq("id", upload_id)
            .execute()
        )
        old_rows = old_active.data or []

        set_old_uploads_inactive(sb, dataset_key, upload_id)

        # Optional cleanup of old files after new upload succeeds
        for row in old_rows:
            old_bucket = row.get("storage_bucket")
            old_path = row.get("storage_path")
            if old_bucket and old_path:
                delete_storage_file_if_exists(old_bucket, old_path)

        clear_caches()
        return True, f"{cfg['label']} uploaded successfully. Rows: {len(df):,}"

    except Exception as e:
        # rollback storage file if metadata insert fails
        delete_storage_file_if_exists(bucket, storage_path)
        return False, f"Metadata save failed: {e}"


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


def render_export_section(dataset_key: str, df: pd.DataFrame, upload_meta: dict | None, file_bytes: bytes):
    cfg = DATASETS[dataset_key]
    st.subheader("Export")

    if df.empty or not upload_meta or not file_bytes:
        st.warning("No data available for export.")
        return

    original_filename = upload_meta.get("original_filename", cfg["download_name"])

    st.download_button(
        label="⬇️ Download original uploaded file",
        data=file_bytes,
        file_name=original_filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
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
# HOME PAGE
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
                <p style="margin:0;">Shipment references, weekly shipment analysis, tracking overview.</p>
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


# =========================================================
# ORDER DASHBOARD PAGE
# =========================================================
def page_order_dashboard():
    render_page_header("Order Dashboard", "Order-level summary, week analysis, open lines and duplicate check.")

    df, upload_meta, file_bytes = load_dataset_df("order_dashboard")

    if upload_meta:
        expected_rows = int(upload_meta.get("row_count", 0))
        st.caption(f"Loaded rows in dashboard: {len(df):,} / Expected rows: {expected_rows:,}")
        if len(df) != expected_rows:
            st.error("Loaded row count does not match metadata. Please re-upload the dataset.")

    render_last_updated(upload_meta)
    st.markdown("<br>", unsafe_allow_html=True)
    render_admin_upload_section("order_dashboard")
    st.markdown("<br>", unsafe_allow_html=True)
    render_export_section("order_dashboard", df, upload_meta, file_bytes)
    st.divider()

    if df.empty:
        st.warning("No Order Dashboard data uploaded yet.")
        return

    order_col = first_existing_column(df, ["BC Order", "SalesDocument"])
    material_col = first_existing_column(df, ["MaterialNumber"])
    batch_col = first_existing_column(df, ["BatchNumber"])
    status_col = first_existing_column(df, ["Order Status", "Status"])
    date_col = first_existing_column(df, ["OrderDate", "BC Order Date"])
    club_col = first_existing_column(df, ["ClubName"])
    cdd_col = first_existing_column(df, ["CDD"])
    type_col = first_existing_column(df, ["OrderType"])

    work = df.copy()

    if date_col:
        work[date_col] = parse_date_series(work[date_col])
    if cdd_col:
        work[cdd_col] = parse_date_series(work[cdd_col])

    total_orders = work[order_col].nunique() if order_col else len(work)

    status_series = (
        work[status_col].astype(str).str.strip().str.lower()
        if status_col else pd.Series([], dtype="object")
    )

    open_count = int(status_series.str.contains("open", na=False).sum()) if status_col else 0
    shipped_count = int(status_series.str.contains("ship", na=False).sum()) if status_col else 0
    cancel_count = int(status_series.str.contains("cancel", na=False).sum()) if status_col else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Orders", f"{total_orders:,}")
    c2.metric("Open Lines", f"{open_count:,}")
    c3.metric("Shipped Lines", f"{shipped_count:,}")
    c4.metric("Cancelled Lines", f"{cancel_count:,}")

    if batch_col and order_col:
        weekly = (
            work.groupby(batch_col, dropna=False)[order_col]
            .nunique()
            .reset_index(name="Orders")
            .sort_values(by=batch_col)
        )

        if status_col:
            week_status = (
                work.groupby(batch_col)[status_col]
                .apply(lambda s: "Week Closed" if s.astype(str).str.lower().str.contains("open", na=False).sum() == 0 else "Open")
                .reset_index(name="Week State")
            )
            weekly = weekly.merge(week_status, on=batch_col, how="left")

        st.subheader("Orders by Week / Batch")
        fig = px.bar(weekly, x=batch_col, y="Orders", hover_data=weekly.columns)
        st.plotly_chart(fig, use_container_width=True)

        if "Week State" in weekly.columns:
            st.dataframe(weekly, use_container_width=True, height=260)

    st.subheader("Open Orders")
    if status_col:
        open_df = work[work[status_col].astype(str).str.lower().str.contains("open", na=False)].copy()
    else:
        open_df = work.copy()

    open_cols = [c for c in [order_col, date_col, material_col, status_col, batch_col, club_col, type_col, cdd_col] if c]
    if open_cols:
        st.dataframe(open_df[open_cols], use_container_width=True, height=360)

    st.subheader("Duplicate Line Checker")
    if order_col and material_col:
        dup_df = work.copy()
        dup_df["dup_key"] = dup_df[order_col].astype(str).str.strip() + "||" + dup_df[material_col].astype(str).str.strip()
        dup_df["dup_count"] = dup_df.groupby("dup_key")["dup_key"].transform("count")
        duplicates = dup_df[dup_df["dup_count"] > 1].copy()

        if duplicates.empty:
            st.success("No duplicate order + material combinations found.")
        else:
            show_cols = [c for c in [order_col, material_col, batch_col, status_col, date_col, club_col] if c] + ["dup_count"]
            st.warning(f"Found {len(duplicates):,} duplicate lines.")
            st.dataframe(duplicates[show_cols], use_container_width=True, height=340)
    else:
        st.info("Duplicate checker needs both 'BC Order/SalesDocument' and 'MaterialNumber' columns.")


# =========================================================
# SHIPMENT PAGE
# =========================================================
def page_fbb_shipment_details():
    render_page_header("FBB-Shipment Details", "Shipment summary, weekly shipped quantity and tracking visibility.")

    df, upload_meta, file_bytes = load_dataset_df("fbb_shipment_details")

    if upload_meta:
        expected_rows = int(upload_meta.get("row_count", 0))
        st.caption(f"Loaded rows in dashboard: {len(df):,} / Expected rows: {expected_rows:,}")
        if len(df) != expected_rows:
            st.error("Loaded row count does not match metadata. Please re-upload the dataset.")

    render_last_updated(upload_meta)
    st.markdown("<br>", unsafe_allow_html=True)
    render_admin_upload_section("fbb_shipment_details")
    st.markdown("<br>", unsafe_allow_html=True)
    render_export_section("fbb_shipment_details", df, upload_meta, file_bytes)
    st.divider()

    if df.empty:
        st.warning("No FBB-Shipment Details data uploaded yet.")
        return

    work = df.copy()

    sales_doc_col = first_existing_column(work, ["Sales Doc"])
    order_col = first_existing_column(work, ["ORDER #"])
    sku_col = first_existing_column(work, ["SKU/ ITEM #"])
    qty_col = first_existing_column(work, ["Order qty"])
    week_col = first_existing_column(work, ["WEEK"])
    date_col = first_existing_column(work, ["DATE"])
    code_col = first_existing_column(work, ["Code"])
    ship_ref_col = first_existing_column(work, ["Shipment Ref#"])
    ups_col = first_existing_column(work, ["UPS TRACKING # (NO SPACE)"])

    if date_col:
        work[date_col] = parse_date_series(work[date_col])
    if qty_col:
        work[qty_col] = parse_numeric_series(work[qty_col])

    total_rows = len(work)
    total_orders = work[order_col].nunique() if order_col else total_rows
    tracking_count = work[ups_col].notna().sum() if ups_col else 0
    shipped_refs = work[ship_ref_col].notna().sum() if ship_ref_col else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Rows", f"{total_rows:,}")
    c2.metric("Unique Orders", f"{total_orders:,}")
    c3.metric("Shipment Ref Available", f"{int(shipped_refs):,}")
    c4.metric("UPS Tracking Available", f"{int(tracking_count):,}")

    if week_col and qty_col:
        st.subheader("Shipment Qty by Week")
        week_summary = (
            work.groupby(week_col, dropna=False)[qty_col]
            .sum(min_count=1)
            .reset_index(name="Total Order Qty")
            .sort_values(by=week_col)
        )
        fig = px.bar(week_summary, x=week_col, y="Total Order Qty", hover_data=week_summary.columns)
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Shipment Detail Table")
    show_cols = [c for c in [sales_doc_col, order_col, sku_col, qty_col, week_col, date_col, code_col, ship_ref_col, ups_col] if c]
    st.dataframe(work[show_cols], use_container_width=True, height=400)

    if ups_col:
        st.subheader("Rows Missing UPS Tracking")
        missing = work[work[ups_col].isna() | (work[ups_col].astype(str).str.strip() == "")]
        st.dataframe(missing[show_cols], use_container_width=True, height=300)


# =========================================================
# INVOICE PAGE
# =========================================================
def page_fbb_invoice_status():
    render_page_header("FBB Invoice Status", "Invoice progress, remaining quantity and team/status analysis.")

    df, upload_meta, file_bytes = load_dataset_df("fbb_invoice_status")

    if upload_meta:
        expected_rows = int(upload_meta.get("row_count", 0))
        st.caption(f"Loaded rows in dashboard: {len(df):,} / Expected rows: {expected_rows:,}")
        if len(df) != expected_rows:
            st.error("Loaded row count does not match metadata. Please re-upload the dataset.")

    render_last_updated(upload_meta)
    st.markdown("<br>", unsafe_allow_html=True)
    render_admin_upload_section("fbb_invoice_status")
    st.markdown("<br>", unsafe_allow_html=True)
    render_export_section("fbb_invoice_status", df, upload_meta, file_bytes)
    st.divider()

    if df.empty:
        st.warning("No FBB Invoice Status data uploaded yet.")
        return

    work = df.copy()

    num_orders_col = first_existing_column(work, ["Number of Orders"])
    num_invoiced_col = first_existing_column(work, ["Number of Invoiced Orders"])
    rem_orders_col = first_existing_column(work, ["Remaining Orders to Invoice"])
    total_qty_col = first_existing_column(work, ["Total Qty Shipped"])
    total_amount_col = first_existing_column(work, ["Total Amount"])
    invoiced_qty_col = first_existing_column(work, ["Invoiced Qty"])
    rem_qty_col = first_existing_column(work, ["Remaining Qty to invoice"])
    rem_amt_col = first_existing_column(work, ["Remaining Amount to invoice"])
    handover_col = first_existing_column(work, ["Hand Over"])
    pickup_col = first_existing_column(work, ["UPS Pickup Date"])
    days_col = first_existing_column(work, ["#Days"])
    status_col = first_existing_column(work, ["Status"])
    team_col = first_existing_column(work, ["Team"])

    numeric_cols = [
        num_orders_col, num_invoiced_col, rem_orders_col, total_qty_col,
        total_amount_col, invoiced_qty_col, rem_qty_col, rem_amt_col, days_col
    ]
    for c in [x for x in numeric_cols if x]:
        work[c] = parse_numeric_series(work[c])

    for c in [handover_col, pickup_col]:
        if c:
            work[c] = parse_date_series(work[c])

    total_orders = int(work[num_orders_col].sum()) if num_orders_col else 0
    total_invoiced = int(work[num_invoiced_col].sum()) if num_invoiced_col else 0
    total_remaining = int(work[rem_orders_col].sum()) if rem_orders_col else 0
    total_amount = float(work[total_amount_col].sum()) if total_amount_col else 0.0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Number of Orders", f"{total_orders:,}")
    c2.metric("Invoiced Orders", f"{total_invoiced:,}")
    c3.metric("Remaining Orders", f"{total_remaining:,}")
    c4.metric("Total Amount", f"{total_amount:,.2f}")

    if status_col:
        st.subheader("By Status")
        status_summary = (
            work.groupby(status_col, dropna=False)[num_orders_col if num_orders_col else work.columns[0]]
            .count()
            .reset_index(name="Rows")
        )
        fig = px.bar(status_summary, x=status_col, y="Rows", hover_data=status_summary.columns)
        st.plotly_chart(fig, use_container_width=True)

    if team_col and rem_amt_col:
        st.subheader("Remaining Amount by Team")
        team_summary = (
            work.groupby(team_col, dropna=False)[rem_amt_col]
            .sum(min_count=1)
            .reset_index(name="Remaining Amount")
        )
        fig = px.bar(team_summary, x=team_col, y="Remaining Amount", hover_data=team_summary.columns)
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Invoice Detail Table")
    st.dataframe(work, use_container_width=True, height=420)


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
