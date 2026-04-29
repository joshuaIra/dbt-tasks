{{
  /* Wait NULL if negative or > 1440 min (24h) after fix; exclude TEST_ patients. */
  config(materialized='view')
}}

with

src as (

  select
    consultation_id,
    patient_id,
    status,
    created_at,
    consultation_started_at,
    toString(consultation_started_at) as _started_s
  from {{ source('teleclinic_raw', 'consultations') }}
  where not startsWith(patient_id, 'TEST_')

),

x as (

  select
    *,

    if(
      position(_started_s, '+02') > 0
      or position(_started_s, 'UTC+2') > 0
      or position(_started_s, '+2:') > 0,
      1,
      0
    ) as is_tz_string

  from src

),

normalized as (

  select
    consultation_id,
    patient_id,
    status,
    toTimeZone(toDateTime(created_at), 'UTC') as created_at_utc,
    if(
      is_tz_string = 1,
      toTimeZone(parseDateTime64BestEffortOrNull(_started_s, 3), 'UTC'),
      toTimeZone(toDateTime(consultation_started_at), 'UTC')
    ) as started_at_utc,
    if(is_tz_string = 1, 1, 0) as is_tz_corrected
  from x

)

select
  consultation_id,
  patient_id,
  status,
  created_at_utc,
  started_at_utc,
  coalesce(
    started_at_utc,
    created_at_utc
  ) as report_at_utc,
  is_tz_corrected,
  multiIf(
    dateDiff(
      'minute',
      created_at_utc,
      started_at_utc
    ) < 0,
    null,
    dateDiff(
      'minute',
      created_at_utc,
      started_at_utc
    ) > 1440,
    null,
    toFloat32(
      dateDiff(
        'minute',
        created_at_utc,
        started_at_utc
      )
    )
  ) as wait_time_minutes
from normalized
