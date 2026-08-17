-- Gold mart: the wide, BI-facing view of stock position per store/item/day.
--
-- Answers three operational questions:
--   1. How many days of stock is on hand?          -> "Stock Left (in Days)"
--   2. How much should be indented to reach target? -> SI (suggested indent)
--   3. Is this position Critical / Normal / Excess? -> EXCESS
--
-- All policy thresholds are vars (see dbt_project.yml) so the rules can be
-- tuned without touching SQL. The committed values are ILLUSTRATIVE
-- PLACEHOLDERS, not a real service-level policy.

{{ config(
    materialized='table'
) }}

{%- set central_list = "'" ~ var('central_regions') | join("', '") ~ "'" -%}

with base as (

    select
        f.*,
        -- Days of cover on hand. NULLIF guards items with no recent
        -- consumption, which would otherwise divide by zero.
        f."Closing_Balance" / nullif(f."ADC_7_Days", 0) as COVER_DAYS
    from {{ ref('slv_ibac_main') }} f
    where f."Date" >= dateadd(year, -{{ var('summary_lookback_years') }}, current_date())

)

select
    f."Date"                               as "DATE",
    f."Store_Name"                         as "Store_Name",
    f."Store_Region"                       as "Store_Region",
    f.OUTLET_TYPE                          as OUTLET_TYPE,
    f.FIELD_COACH                          as FIELD_COACH,
    f.AREA_COACH                           as AREA_COACH,

    f.PRODUCT_KEY                          as PRODUCT_KEY,
    f."NAME"                               as NAME,

    f."Opening_Balance"                    as "Opening_Balance",
    f."Closing_Balance"                    as "Closing_Balance",

    ceil(f."ADC_7_Days" * 100) / 100       as "ADC 7 Days",
    f."ADC_28_Days"                        as "ADC 28 Days",

    f.CRITICALITY                          as CRITICALITY,
    f.FROZEN_AMBIENT                       as FROZEN_AMBIENT,
    f.CASE_SIZE                            as CASE_SIZE,
    f.FROZEN_CAPACITY_IN_LTR_FOR_ONE_UNIT  as FROZEN_CAPACITY_IN_LTR_FOR_ONE_UNIT,
    f.AVAILABLE_SPACE_IN_LTR               as AVAILABLE_SPACE_IN_LTR,
    f.LAST_INDENT_DATE                     as LAST_INDENT_DATE,
    f.INDENT_CYCLE                         as INDENT_CYCLE,
    f."TYPE"                               as TYPE,
    f."Category"                           as "Category",
    f.STATE                                as STATE,
    f.CITY                                 as CITY,
    f."Sales"                              as Sales,
    f."Sales_Cost"                         as Sales_Cost,

    dateadd(day, f.INDENT_CYCLE, f.LAST_INDENT_DATE) as "Upcoming Indent Cycle",

    -- Freezer utilisation, in litres.
    (f."Closing_Balance" * f.FROZEN_CAPACITY_IN_LTR_FOR_ONE_UNIT) as "CONSUMED SPACE",

    f.AVAILABLE_SPACE_IN_LTR               as TOTAL_FREEZER_SPACE_F,

    ((f."Closing_Balance" * f.FROZEN_CAPACITY_IN_LTR_FOR_ONE_UNIT) - f.AVAILABLE_SPACE_IN_LTR)
                                           as REMAINING_FREEZER_SPACE,

    -- Days of stock remaining, against the rounded ADC the business reports on.
    round(
        abs(
            round(f."Closing_Balance", 2)
            / nullif(ceil(f."ADC_7_Days" * 100) / 100, 0)
        )
    , 2) as "Stock Left (in Days)",

    -- Suggested indent: quantity needed to reach the target days of cover.
    -- Already well covered -> suggest nothing.
    round(
        abs(
            case
                when f."Store_Region" in ({{ central_list }})
                then case
                        when f.COVER_DAYS > {{ var('central_si_skip_cover_days') }} then 0
                        else f."ADC_7_Days" * {{ var('central_target_cover_days') }} - f."Closing_Balance"
                     end
                when f."Store_Region" = '{{ var('remote_region') }}'
                then case
                        when f.COVER_DAYS > {{ var('remote_si_skip_cover_days') }} then 0
                        else f."ADC_7_Days" * {{ var('remote_target_cover_days') }} - f."Closing_Balance"
                     end
                else null
            end
        )
    , 2) as SI,

    -- Stock health band. Regions not listed fall through to NULL rather than
    -- being forced into a band that does not apply to them.
    case
        when f."Store_Region" in ({{ central_list }})
             and f.COVER_DAYS <= {{ var('central_critical_cover_days') }} then 'Critical'

        when f."Store_Region" in ({{ central_list }})
             and f.COVER_DAYS >= {{ var('central_excess_cover_days') }} then 'Excess'

        when f."Store_Region" = '{{ var('remote_region') }}'
             and f.COVER_DAYS <= {{ var('remote_critical_cover_days') }} then 'Critical'

        when f."Store_Region" = '{{ var('remote_region') }}'
             and f.COVER_DAYS >= {{ var('remote_excess_cover_days') }} then 'Excess'

        when f."Store_Region" in ({{ central_list }})
             and f.COVER_DAYS between {{ var('central_critical_cover_days') }}
                                  and {{ var('central_excess_cover_days') }} then 'Normal'

        when f."Store_Region" = '{{ var('remote_region') }}'
             and f.COVER_DAYS between {{ var('remote_critical_cover_days') }}
                                  and {{ var('remote_excess_cover_days') }} then 'Normal'
    end as EXCESS

from base f
