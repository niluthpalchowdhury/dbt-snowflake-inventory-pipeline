{{ config(materialized='table') }}

select distinct
  to_varchar(STORE_KEY) as STORE_KEY,
  "Store_Name",
  "Store_Region",
  OUTLET_TYPE,
  FIELD_COACH,
  AREA_COACH,
  current_timestamp() as audit_row_create_date,
  current_timestamp() as audit_row_modified_date
from {{ ref('slv_ibac_main') }}
where STORE_KEY is not null