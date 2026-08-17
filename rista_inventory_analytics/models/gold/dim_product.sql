{{ config(materialized='table') }}

select distinct
  to_varchar(PRODUCT_KEY) as PRODUCT_KEY,
  "NAME",
  current_timestamp() as audit_row_create_date,
  current_timestamp() as audit_row_modified_date
from {{ ref('slv_ibac_main') }}
where PRODUCT_KEY is not null