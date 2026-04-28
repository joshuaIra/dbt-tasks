{{
  config(materialized='table')
}}

select
  toStartOfMonth(report_at_utc) as report_month,
  toUInt32(count()) as completed_consults,
  toUInt32(countIf(wait_time_minutes is not null)) as corrected_wait_records,
  avg(wait_time_minutes) as avg_wait_time_minutes,
  avg(toFloat64(is_tz_corrected)) as pct_tz_corrected
from {{ ref('stg_consultations_fixed') }}
where status = 'completed'
group by
  1
order by
  1
