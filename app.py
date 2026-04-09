import math
import time
from datetime import datetime, timezone

import pandas as pd
import streamlit as st


# =========================
# SAFE VALUE CONVERTER
# =========================
def safe_json_value(v):
    if pd.isna(v):
        return None
    if isinstance(v, pd.Timestamp):
        return v.isoformat()
    return str(v) if not isinstance(v, (int, float, bool, dict, list)) else v


# =========================
# DATAFRAME TO RECORDS
# =========================
def dataframe_to_records(df: pd.DataFrame):
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return [
        {str(col): safe_json_value(val) for col, val in row.items()}
        for row in df.to_dict(orient="records")
    ]


# =========================
# CHUNK GENERATOR
# =========================
def chunk_list(data, chunk_size=100):
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]


# =========================
# DELETE ALL ROWS SAFELY
# =========================
def delete_all_rows(supabase, table_name, known_column=None):
    """
    Delete all rows from table using a real column name.
    Set known_column to a column that always exists in the table.
    """
    if not known_column:
        raise Exception(f"Delete column not provided for table: {table_name}")

    # delete all rows where known_column is not null
    supabase.table(table_name).delete().not_.is_(known_column, "null").execute()


# =========================
# UPSERT / REPLACE METRICS
# =========================
def replace_dataset_metrics(supabase, page_name, table_name, row_count, uploaded_by="admin"):
    payload = {
        "page_name": page_name,
        "table_name": table_name,
        "row_count": row_count,
        "updated_by": uploaded_by,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }

    try:
        supabase.table("dataset_metrics").delete().eq("page_name", page_name).execute()
    except Exception:
        pass

    supabase.table("dataset_metrics").insert(payload).execute()


# =========================
# LOG APP UPLOAD
# =========================
def log_app_upload(supabase, page_name, table_name, row_count, uploaded_by="admin"):
    payload = {
        "page_name": page_name,
        "table_name": table_name,
        "row_count": row_count,
        "uploaded_by": uploaded_by,
        "uploaded_at": datetime.now(timezone.utc).isoformat()
    }

    try:
        supabase.table("app_uploads").insert(payload).execute()
    except Exception:
        pass


# =========================
# MAIN REPLACE UPLOAD FUNCTION
# =========================
def upload_dataset_replace(
    supabase,
    df: pd.DataFrame,
    table_name: str,
    page_name: str,
    delete_column: str,
    uploaded_by: str = "admin",
    chunk_size: int = 100,
    sleep_sec: float = 0.15,
    max_retries: int = 3,
):
    total_rows = len(df)

    if total_rows == 0:
        raise Exception("Uploaded file is empty.")

    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    records = dataframe_to_records(df)
    total_chunks = math.ceil(total_rows / chunk_size)

    progress_bar = st.progress(0)
    status_box = st.empty()

    try:
        status_box.info(f"Clearing old data from {table_name}...")
        delete_all_rows(supabase, table_name, known_column=delete_column)

        inserted_rows = 0

        for chunk_index, chunk in enumerate(chunk_list(records, chunk_size), start=1):
            chunk_done = False
            last_error = None

            for attempt in range(1, max_retries + 1):
                try:
                    supabase.table(table_name).insert(chunk).execute()
                    chunk_done = True
                    break
                except Exception as e:
                    last_error = e
                    time.sleep(1.5 * attempt)

            if not chunk_done:
                raise Exception(f"Chunk {chunk_index}/{total_chunks} failed after retries. Error: {last_error}")

            inserted_rows += len(chunk)
            progress_bar.progress(min(inserted_rows / total_rows, 1.0))
            status_box.info(
                f"Uploading {page_name}: {inserted_rows:,}/{total_rows:,} rows completed "
                f"({chunk_index}/{total_chunks} chunks)"
            )
            time.sleep(sleep_sec)

        status_box.info("Updating dataset metrics...")
        replace_dataset_metrics(
            supabase=supabase,
            page_name=page_name,
            table_name=table_name,
            row_count=total_rows,
            uploaded_by=uploaded_by,
        )

        status_box.info("Writing upload log...")
        log_app_upload(
            supabase=supabase,
            page_name=page_name,
            table_name=table_name,
            row_count=total_rows,
            uploaded_by=uploaded_by,
        )

        progress_bar.progress(1.0)
        status_box.success(f"Upload completed successfully. {total_rows:,} rows uploaded.")
        return True

    except Exception as e:
        status_box.error(f"Upload failed: {e}")
        return False


# =========================
# FBB-SHIPMENT DETAILS UPLOAD UI
# =========================
def render_fbb_shipment_upload(supabase):
    st.subheader("Upload Excel for FBB-Shipment Details")

    uploaded_file = st.file_uploader(
        "Upload Excel",
        type=["xlsx", "xls"],
        key="fbb_shipment_upload"
    )

    if st.button("Upload and Replace", key="btn_fbb_shipment_replace"):
        if uploaded_file is None:
            st.warning("Please upload a file first.")
            return

        try:
            # Read Excel as text-safe format
            df = pd.read_excel(uploaded_file, dtype=str)
            df.columns = [str(c).strip() for c in df.columns]

            if len(df) == 0:
                st.error("The uploaded file is empty.")
                return

            if len(df) > 50000:
                st.warning(
                    f"Large upload detected: {len(df):,} rows. "
                    f"Upload may take longer, but the app will use safer chunking automatically."
                )

            # IMPORTANT:
            # Replace delete_column below with a real column from your fbb_shipment_details table.
            # Best choice = a column always filled, like "Shipment Reference Number"
            ok = upload_dataset_replace(
                supabase=supabase,
                df=df,
                table_name="fbb_shipment_details",
                page_name="FBB-Shipment Details",
                delete_column="Shipment Reference Number",
                uploaded_by="admin",
                chunk_size=100,
                sleep_sec=0.15,
                max_retries=3,
            )

            if ok:
                st.success("Dataset replaced successfully.")
                st.cache_data.clear()

        except Exception as e:
            st.error(f"Upload failed: {e}")
