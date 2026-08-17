-- Blob Storage -> Snowflake RAW.
--
-- This is the one hop between the Python extract and the dbt project, and it
-- is what stamps the three metadata columns the bronze model orders by
-- (_SOURCE_FILE, _LOAD_DT, _INGESTED_AT).
--
-- Run once as an account/sysadmin role to set up. Object names are generic
-- placeholders - align them with the vars in dbt_project.yml.

-- ---------------------------------------------------------------------------
-- 1. Storage integration
--
-- Uses Azure AD rather than embedding the SAS token in Snowflake. After
-- creation, run DESC STORAGE INTEGRATION and consent to the multi-tenant app
-- URL in Azure, then grant it Storage Blob Data Contributor on the container.
-- ---------------------------------------------------------------------------
create storage integration if not exists AZURE_BLOB_INT
  type = external_stage
  storage_provider = 'AZURE'
  enabled = true
  azure_tenant_id = '<azure-tenant-id>'
  storage_allowed_locations = ('azure://<storage-account-name>.blob.core.windows.net/<container-name>/');

-- ---------------------------------------------------------------------------
-- 2. File format and external stage
-- ---------------------------------------------------------------------------
create file format if not exists RAW_DB.RISTA.PARQUET_FF
  type = parquet;

create stage if not exists RAW_DB.RISTA.ITEM_BALANCE_STAGE
  storage_integration = AZURE_BLOB_INT
  url = 'azure://<storage-account-name>.blob.core.windows.net/<container-name>/Item_Balance_Activity_Cost/'
  file_format = RAW_DB.RISTA.PARQUET_FF;

-- ---------------------------------------------------------------------------
-- 3. RAW table
--
-- The payload stays as a single VARIANT: RAW is a landing zone, not a
-- contract. Adding a column upstream must never break this table - typing and
-- renaming happen in the dbt bronze layer.
-- ---------------------------------------------------------------------------
create table if not exists RAW_DB.RISTA.ITEM_BALANCE_WITH_ACTIVITY_AND_COST_RAW (
    PARQUET       variant,
    _SOURCE_FILE  varchar,        -- blob path, e.g. .../business_date=2024-05-01.parquet
    _LOAD_DT      date,           -- business date parsed from the blob name
    _INGESTED_AT  timestamp_ntz   -- when Snowflake loaded it; drives source freshness
);

-- ---------------------------------------------------------------------------
-- 4. Load
--
-- The extract replaces one blob per business date, so re-running a window
-- reloads those files. Bronze deduplicates on (_LOAD_DT, _INGESTED_AT), so a
-- reload supersedes the earlier copy rather than duplicating the grain.
-- ---------------------------------------------------------------------------
copy into RAW_DB.RISTA.ITEM_BALANCE_WITH_ACTIVITY_AND_COST_RAW
  (PARQUET, _SOURCE_FILE, _LOAD_DT, _INGESTED_AT)
from (
  select
    $1,
    metadata$filename,
    try_to_date(
      regexp_substr(metadata$filename, 'business_date=(\\d{4}-\\d{2}-\\d{2})', 1, 1, 'e', 1)
    ),
    current_timestamp()
  from @RAW_DB.RISTA.ITEM_BALANCE_STAGE
)
file_format = (format_name = RAW_DB.RISTA.PARQUET_FF)
force = true;   -- re-load replaced partitions even if the filename was seen before

-- ---------------------------------------------------------------------------
-- 5. Optional: continuous ingestion instead of the COPY above
--
-- Requires an Event Grid notification integration on the container. With
-- auto-ingest the pipeline becomes event-driven and the Airflow DAG only has
-- to wait for freshness rather than trigger the load.
-- ---------------------------------------------------------------------------
-- create pipe RAW_DB.RISTA.ITEM_BALANCE_PIPE
--   auto_ingest = true
--   integration = '<notification-integration-name>'
-- as
-- copy into RAW_DB.RISTA.ITEM_BALANCE_WITH_ACTIVITY_AND_COST_RAW
--   (PARQUET, _SOURCE_FILE, _LOAD_DT, _INGESTED_AT)
-- from (
--   select
--     $1,
--     metadata$filename,
--     try_to_date(
--       regexp_substr(metadata$filename, 'business_date=(\\d{4}-\\d{2}-\\d{2})', 1, 1, 'e', 1)
--     ),
--     current_timestamp()
--   from @RAW_DB.RISTA.ITEM_BALANCE_STAGE
-- )
-- file_format = (format_name = RAW_DB.RISTA.PARQUET_FF);
