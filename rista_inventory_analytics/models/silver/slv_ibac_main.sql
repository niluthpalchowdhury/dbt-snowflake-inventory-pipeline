-- Silver 2/2: the conformed fact. Cleans item names, drops non-store rows,
-- and builds the surrogate keys the gold layer joins on.
--
-- Grain: one row per Date + Store + Product + SKU. Enforced by the
-- unique_combination_of_columns test in _silver__models.yml.

{{ config(materialized='table') }}

select distinct
  a."Date", a."Store_Name", a."Store_Code", a."Branch_Labels",
  a."SKU", a."TYPE", a."Category",

  -- Item names are hand-entered upstream: strip stray quotes and collapse
  -- runs of whitespace, otherwise the same product hashes to two PRODUCT_KEYs.
  regexp_replace(trim(regexp_replace(a."NAME", '["'']+', ' ')), '\\s+', ' ') as "NAME",

  a."MEASURING_UNIT",
  a."Opening_Balance", a."Opening_Cost", a."Closing_Balance", a."Closing_Cost",
  a."Activity", a."Yield",
  a."Sales", a."Sales_Cost", a."Returns", a."Returns_Cost",
  a."Voided", a."Voided_Cost", a."Purchase", a."Purchase_Cost",
  a."Audit_Variance", a."Audit_Variance_Cost", a."Adjustment", a."Adjustment_Cost",
  a."Shrinkage", a."Shrinkage_Cost", a."Transfer_In", a."Transfer_In_Cost",
  a."Transfer_Out", a."Transfer_Out_Cost", a."Housemade", a."Housemade_Cost",
  a."House_Consumed", a."House_Consumed_Cost",
  a."ADC_7_Days", a."ADC_28_Days",
  a."Store_Region",
  a."CRITICALITY", a."FROZEN_AMBIENT", a."CASE_SIZE",
  a."FROZEN_CAPACITY_IN_LTR_FOR_ONE_UNIT", a."AVAILABLE_SPACE_IN_LTR",
  a."OUTLET_TYPE", a."FIELD_COACH", a."AREA_COACH", a."STATE", a."CITY",
  a."LAST_INDENT_DATE", a."INDENT_CYCLE",

  -- Surrogate keys. Hashed on the normalised natural key so they stay stable
  -- across runs; abs() keeps them positive for BI tools that dislike negatives.
  to_number(to_char(a."Date",'YYYYMMDD'))          as DATE_KEY,
  abs(hash(upper(trim(a."Store_Name"::varchar))))  as STORE_KEY,
  abs(hash(upper(trim(a."NAME"::varchar))))        as PRODUCT_KEY
from {{ ref('slv_ibac_resolved') }} a

-- Exclude rows that are not real stores. Values below are illustrative:
-- the first two are spreadsheet artefacts that reach the master sheet, the
-- last two are one decommissioned outlet whose name also arrives with a
-- trailing newline, which makes it a distinct value to Snowflake.
where a."Store_Name" not in (
  'Loading...',
  '#REF!',
  'Retired Store',
  'Retired Store' || chr(10)
)
