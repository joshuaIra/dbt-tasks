{{
  /* Wait NULL if negative or > 1440 min (24h) after fix; exclude TEST_ patients. */
  config(materialized='view')
}}

with

src as (

  select
    consultation_id,
    patient_id,
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

)

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
  coalesce(
    if(
      is_tz_string = 1,
      toTimeZone(parseDateTime64BestEffortOrNull(_started_s, 3), 'UTC'),
      toTimeZone(toDateTime(consultation_started_at), 'UTC')
    ),
    toTimeZone(toDateTime(created_at), 'UTC')
  ) as report_at_utc,
  if(is_tz_string = 1, 1, 0) as is_tz_corrected,
  multiIf(
    dateDiff(
      'minute',
      toTimeZone(toDateTime(created_at), 'UTC'),
      if(
        is_tz_string = 1,
        toTimeZone(parseDateTime64BestEffortOrNull(_started_s, 3), 'UTC'),
        toTimeZone(toDateTime(consultation_started_at), 'UTC')
      )
    ) < 0,
    null,
    dateDiff(
      'minute',
      toTimeZone(toDateTime(created_at), 'UTC'),
      if(
        is_tz_string = 1,
        toTimeZone(parseDateTime64BestEffortOrNull(_started_s, 3), 'UTC'),
        toTimeZone(toDateTime(consultation_started_at), 'UTC')
      )
    ) > 1440,
    null,
    toFloat32(
      dateDiff(
        'minute',
        toTimeZone(toDateTime(created_at), 'UTC'),
        if(
          is_tz_string = 1,
          toTimeZone(parseDateTime64BestEffortOrNull(_started_s, 3), 'UTC'),
          toTimeZone(toDateTime(consultation_started_at), 'UTC')
        )
      )
    )
  ) as wait_time_minutes
from x
