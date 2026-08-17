-- Silver 1/2: enrich the bronze fact with master attributes and add the
-- rolling consumption averages every downstream stock metric depends on.
--
-- The master tables come from operational spreadsheets and are not guaranteed
-- unique on their join key, so any table that can fan out is deduped to one
-- row per key BEFORE the join. Deduping after the join would silently multiply
-- the fact rows and inflate every measure.

{{ config(materialized='table') }}

with store_master_dedup as (
  select *
  from {{ source('masters','STG_STORE_MASTER_DATA') }}
  qualify row_number() over (
    partition by trim(upper(OUTLET_NAME))
    -- Prefer a row with a usable city over a spreadsheet error value.
    order by iff(trim(City) = '#ERROR!', 1, 0) asc, City nulls last
  ) = 1
)

select
  a."Date", a."Store_Name", a."Store_Code", a."Branch_Labels",
  a."SKU", a."TYPE", a."Category", a."NAME", a."MEASURING_UNIT",
  a."Opening_Balance", a."Opening_Cost", a."Closing_Balance", a."Closing_Cost",
  a."Activity", a."Yield",
  a."Sales", a."Sales_Cost", a."Returns", a."Returns_Cost",
  a."Voided", a."Voided_Cost", a."Purchase", a."Purchase_Cost",
  a."Audit_Variance", a."Audit_Variance_Cost", a."Adjustment", a."Adjustment_Cost",
  a."Shrinkage", a."Shrinkage_Cost", a."Transfer_In", a."Transfer_In_Cost",
  a."Transfer_Out", a."Transfer_Out_Cost", a."Housemade", a."Housemade_Cost",
  a."House_Consumed", a."House_Consumed_Cost",

  -- Average daily consumption, per store + item, over trailing 7 and 28 rows.
  -- Row-based (not range-based) windows: a date with no activity produces no
  -- row, so these average over observed days rather than calendar days.
  avg(a."Sales") over (
    partition by a."Store_Name", a."NAME" order by a."Date"
    rows between 6 preceding and current row
  ) as "ADC_7_Days",
  avg(a."Sales") over (
    partition by a."Store_Name", a."NAME" order by a."Date"
    rows between 27 preceding and current row
  ) as "ADC_28_Days",

  b.Location  as "Store_Region",
  c.Criticality, c.Frozen_Ambient, c.Case_Size,
  d.Frozen_capacity_in_Ltr_for_one_unit,
  e.Available_Space_in_Ltr,
  f.Mode as Outlet_Type, f.Field_Coach, f.Area_Coach, f.City, f.State,
  g.Last_Indent_Date, g.Indent_Cycle,

  a._source_file, a._load_dt, a._ingested_at
from {{ ref('brz_ibac') }} a
left join {{ source('masters','STG_OUTLET_CATEGORY') }}       b on a."Store_Name"        = b.Store_Name
left join {{ source('masters','STG_ITEM_CATEGORY') }}         c on a."NAME"              = c.NAME
left join {{ source('masters','STG_CHEST_FREEZER_DATA') }}    d on a."NAME"              = d.ITEM_NAME
left join {{ source('masters','STG_FREEZER_CAPACITY_DATA') }} e on a."Store_Name"        = e.OUTLET_NAME
left join store_master_dedup                                  f on upper(a."Store_Name") = upper(f.OUTLET_NAME)
left join {{ source('masters','INDENT_CYCLE_DATA') }}         g on a."Store_Name"        = g.STORE_NAME
where a."Store_Name" is not null
