{{ config(materialized='table') }}

select distinct
  to_varchar(to_char("Date",'YYYYMMDD')) as DATE_KEY,
  "Date"::date                            as DATE,
  dayofweek("Date")                       as DAY_OF_WEEK,
  day("Date")                             as DAY_OF_MONTH,
  dayofyear("Date")                       as DAY_OF_YEAR,
  weekofyear("Date")                      as WEEK_OF_YEAR,
  date_trunc('WEEK', "Date")              as WEEK_START_DATE,
  dateadd(day, 6, date_trunc('WEEK', "Date")) as WEEK_END_DATE,
  month("Date")                           as MONTH_NUM,
  monthname("Date")                       as MONTH_NAME,
  day(last_day("Date"))                   as DAYS_IN_MONTH,
  iff("Date" = last_day("Date"), true, false) as IS_LAST_DAY_OF_MONTH,
  quarter("Date")                         as QUARTER_NUM,
  'Q' || quarter("Date")                  as QUARTER_NAME,
  year("Date")                            as YEAR_NUM,
  iff(
    (mod(year("Date"),4)=0 and mod(year("Date"),100)!=0) or mod(year("Date"),400)=0,
    true, false
  ) as IS_LEAP_YEAR
from {{ ref('slv_ibac_main') }}