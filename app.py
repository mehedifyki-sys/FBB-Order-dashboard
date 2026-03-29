create table if not exists public.app_uploads (
    id bigserial primary key,
    dataset_key text not null check (dataset_key in (
        'order_dashboard',
        'fbb_shipment_details',
        'fbb_invoice_status'
    )),
    original_filename text not null,
    uploaded_by text not null,
    uploaded_at timestamptz not null default now(),
    row_count integer not null default 0,
    column_order jsonb not null default '[]'::jsonb,
    sheet_name text,
    is_active boolean not null default true
);

create index if not exists idx_app_uploads_dataset_key
    on public.app_uploads(dataset_key);

create index if not exists idx_app_uploads_active
    on public.app_uploads(dataset_key, is_active, uploaded_at desc);


create table if not exists public.dataset_raw_rows (
    id bigserial primary key,
    upload_id bigint not null references public.app_uploads(id) on delete cascade,
    row_number integer not null,
    row_data jsonb not null
);

create index if not exists idx_dataset_raw_rows_upload
    on public.dataset_raw_rows(upload_id);

create index if not exists idx_dataset_raw_rows_upload_row
    on public.dataset_raw_rows(upload_id, row_number);


create table if not exists public.dataset_metrics (
    id bigserial primary key,
    upload_id bigint not null references public.app_uploads(id) on delete cascade,
    metric_key text not null,
    metric_num numeric,
    metric_text text
);

create index if not exists idx_dataset_metrics_upload
    on public.dataset_metrics(upload_id);

create index if not exists idx_dataset_metrics_upload_key
    on public.dataset_metrics(upload_id, metric_key);


create table if not exists public.order_weekly_summary (
    id bigserial primary key,
    upload_id bigint not null references public.app_uploads(id) on delete cascade,
    batch_number text,
    orders_count integer,
    week_state text
);

create index if not exists idx_order_weekly_summary_upload
    on public.order_weekly_summary(upload_id);


create table if not exists public.order_open_orders (
    id bigserial primary key,
    upload_id bigint not null references public.app_uploads(id) on delete cascade,
    bc_order text,
    sales_document text,
    material_number text,
    status_value text,
    batch_number text,
    order_date text,
    cdd text,
    club_name text,
    order_type text
);

create index if not exists idx_order_open_orders_upload
    on public.order_open_orders(upload_id);


create table if not exists public.order_duplicate_lines (
    id bigserial primary key,
    upload_id bigint not null references public.app_uploads(id) on delete cascade,
    bc_order text,
    sales_document text,
    material_number text,
    batch_number text,
    status_value text,
    order_date text,
    club_name text,
    dup_count integer
);

create index if not exists idx_order_duplicate_lines_upload
    on public.order_duplicate_lines(upload_id);


create table if not exists public.shipment_weekly_summary (
    id bigserial primary key,
    upload_id bigint not null references public.app_uploads(id) on delete cascade,
    week_value text,
    total_order_qty numeric
);

create index if not exists idx_shipment_weekly_summary_upload
    on public.shipment_weekly_summary(upload_id);


create table if not exists public.shipment_ref_summary (
    id bigserial primary key,
    upload_id bigint not null references public.app_uploads(id) on delete cascade,
    shipment_ref text,
    unique_orders integer,
    total_qty_shipped numeric
);

create index if not exists idx_shipment_ref_summary_upload
    on public.shipment_ref_summary(upload_id);


create table if not exists public.shipment_detail_compact (
    id bigserial primary key,
    upload_id bigint not null references public.app_uploads(id) on delete cascade,
    sales_doc text,
    order_number text,
    sku_item text,
    order_qty numeric,
    week_value text,
    date_value text,
    code text,
    shipment_ref text,
    ups_tracking text
);

create index if not exists idx_shipment_detail_compact_upload
    on public.shipment_detail_compact(upload_id);


create table if not exists public.invoice_status_summary (
    id bigserial primary key,
    upload_id bigint not null references public.app_uploads(id) on delete cascade,
    status_value text,
    row_count integer
);

create index if not exists idx_invoice_status_summary_upload
    on public.invoice_status_summary(upload_id);


create table if not exists public.invoice_team_summary (
    id bigserial primary key,
    upload_id bigint not null references public.app_uploads(id) on delete cascade,
    team_value text,
    remaining_amount numeric
);

create index if not exists idx_invoice_team_summary_upload
    on public.invoice_team_summary(upload_id);


create table if not exists public.invoice_detail_compact (
    id bigserial primary key,
    upload_id bigint not null references public.app_uploads(id) on delete cascade,
    sp_no text,
    bd_ref text,
    cs_ref text,
    number_of_orders numeric,
    number_of_invoiced_orders numeric,
    remaining_orders_to_invoice numeric,
    total_qty_shipped numeric,
    total_amount numeric,
    invoiced_qty numeric,
    remaining_qty_to_invoice numeric,
    remaining_amount_to_invoice numeric,
    hand_over text,
    ups_pickup_date text,
    days_value numeric,
    status_value text,
    team_value text
);

create index if not exists idx_invoice_detail_compact_upload
    on public.invoice_detail_compact(upload_id);
