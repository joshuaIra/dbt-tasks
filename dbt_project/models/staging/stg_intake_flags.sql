{{
  config(materialized='view')
}}

select
  consultation_id,
  toUInt8(max(coalesce(referral_requested, 0)) > 0) as referral_requested
from {{ source('teleclinic_raw', 'intake_flags') }}
group by
  consultation_id
