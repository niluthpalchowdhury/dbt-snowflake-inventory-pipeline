-- Bronze: type the RAW VARIANT payload and pick one row per business grain.
--
-- Everything lands as a string, so every cast is a try_ cast: a single bad
-- value from the API degrades to NULL instead of failing the whole build.
--
-- Late-arriving corrections mean the same (Date, Store, SKU) can appear in
-- more than one file, so the qualify below keeps the most trustworthy copy.

{{ config(
    materialized='incremental',
    unique_key=['"Date"', '"Store_Name"', '"SKU"'],
    incremental_strategy='merge'
) }}

select
  try_to_date( to_varchar(r.PARQUET:Date) )                    as "Date",
  to_varchar(r.PARQUET:Store_Name)                             as "Store_Name",
  to_varchar(r.PARQUET:Store_Code)                             as "Store_Code",
  to_varchar(r.PARQUET:Branch_Labels)                          as "Branch_Labels",
  try_to_number( to_varchar(r.PARQUET:SKU) )                   as "SKU",
  to_varchar(r.PARQUET:TYPE)                                   as "TYPE",
  to_varchar(r.PARQUET:Category)                               as "Category",
  to_varchar(r.PARQUET:NAME)                                   as "NAME",
  to_varchar(r.PARQUET:MEASURING_UNIT)                         as "MEASURING_UNIT",

  try_to_double( to_varchar(r.PARQUET:Opening_Balance) )       as "Opening_Balance",
  try_to_double( to_varchar(r.PARQUET:Opening_Cost) )          as "Opening_Cost",
  try_to_double( to_varchar(r.PARQUET:Closing_Balance) )       as "Closing_Balance",
  try_to_double( to_varchar(r.PARQUET:Closing_Cost) )          as "Closing_Cost",
  to_varchar(r.PARQUET:Activity)                               as "Activity",
  try_to_double( to_varchar(r.PARQUET:Yield) )                 as "Yield",

  try_to_double( to_varchar(r.PARQUET:Sales) )                 as "Sales",
  try_to_double( to_varchar(r.PARQUET:Sales_Cost) )            as "Sales_Cost",
  try_to_double( to_varchar(r.PARQUET:Returns) )               as "Returns",
  try_to_double( to_varchar(r.PARQUET:Returns_Cost) )          as "Returns_Cost",
  try_to_double( to_varchar(r.PARQUET:Voided) )                as "Voided",
  try_to_double( to_varchar(r.PARQUET:Voided_Cost) )           as "Voided_Cost",
  try_to_double( to_varchar(r.PARQUET:Purchase) )              as "Purchase",
  try_to_double( to_varchar(r.PARQUET:Purchase_Cost) )         as "Purchase_Cost",
  try_to_double( to_varchar(r.PARQUET:Audit_Variance) )        as "Audit_Variance",
  try_to_double( to_varchar(r.PARQUET:Audit_Variance_Cost) )   as "Audit_Variance_Cost",
  try_to_double( to_varchar(r.PARQUET:Adjustment) )            as "Adjustment",
  try_to_double( to_varchar(r.PARQUET:Adjustment_Cost) )       as "Adjustment_Cost",
  try_to_double( to_varchar(r.PARQUET:Shrinkage) )             as "Shrinkage",
  try_to_double( to_varchar(r.PARQUET:Shrinkage_Cost) )        as "Shrinkage_Cost",
  try_to_double( to_varchar(r.PARQUET:Transfer_In) )           as "Transfer_In",
  try_to_double( to_varchar(r.PARQUET:Transfer_In_Cost) )      as "Transfer_In_Cost",
  try_to_double( to_varchar(r.PARQUET:Transfer_Out) )          as "Transfer_Out",
  try_to_double( to_varchar(r.PARQUET:Transfer_Out_Cost) )     as "Transfer_Out_Cost",
  try_to_double( to_varchar(r.PARQUET:Housemade) )             as "Housemade",
  try_to_double( to_varchar(r.PARQUET:Housemade_Cost) )        as "Housemade_Cost",
  try_to_double( to_varchar(r.PARQUET:House_Consumed) )        as "House_Consumed",
  try_to_double( to_varchar(r.PARQUET:House_Consumed_Cost) )   as "House_Consumed_Cost",

  r._source_file, r._load_dt, r._ingested_at
from {{ source('rista_raw', 'ITEM_BALANCE_WITH_ACTIVITY_AND_COST_RAW') }} r
{% if is_incremental() %}
  -- 8-day lookback: wide enough to absorb the extract's rolling re-load window
  -- and any late file, narrow enough to avoid rescanning history every run.
  where r._load_dt >= (select dateadd(day, -8, max(_load_dt)) from {{ this }})
{% endif %}
qualify row_number() over (
  partition by "Date", "Store_Name", "SKU"
  order by
    r._load_dt desc,                                                              -- newest load wins
    r._ingested_at desc,                                                          -- then newest file within that load
    (case when r.PARQUET:House_Consumed_Cost is not null then 0 else 1 end),      -- prefer the more complete payload
    to_varchar(r.PARQUET:Store_Code)                                              -- final deterministic tiebreak
) = 1
