{{
  config(severity: error)
}}

-- description: "After UTC alignment, a negative wait is physically impossible; indicates bad joins or tz logic — must not reach MoH reports."

select
  consultation_id,
  wait_time_minutes,
  is_tz_corrected
from {{ ref('stg_consultations_fixed') }}
where
  is_tz_corrected = 1
  and wait_time_minutes is not null
  and wait_time_minutes < 0
