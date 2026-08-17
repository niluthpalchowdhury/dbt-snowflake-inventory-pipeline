{{ config(materialized='table') }}

select
  DATE_KEY,
  STORE_KEY,
  PRODUCT_KEY,
  "Opening_Balance", "Opening_Cost", "Closing_Balance", "Closing_Cost",
  "Activity", "Yield",
  "Sales", "Sales_Cost", "Returns", "Returns_Cost",
  "Voided", "Voided_Cost", "Purchase", "Purchase_Cost",
  "Audit_Variance", "Audit_Variance_Cost", "Adjustment", "Adjustment_Cost",
  "Shrinkage", "Shrinkage_Cost", "Transfer_In", "Transfer_In_Cost",
  "Transfer_Out", "Transfer_Out_Cost", "Housemade", "Housemade_Cost",
  "House_Consumed", "House_Consumed_Cost",
  "ADC_7_Days",
  "TYPE",
  "Category",
  "MEASURING_UNIT",
  "SKU",
  "ADC_28_Days",
  "CRITICALITY", "FROZEN_AMBIENT", "CASE_SIZE",
  "FROZEN_CAPACITY_IN_LTR_FOR_ONE_UNIT", "AVAILABLE_SPACE_IN_LTR",
  "LAST_INDENT_DATE", "INDENT_CYCLE",
  "STATE", "CITY"
from {{ ref('slv_ibac_main') }}