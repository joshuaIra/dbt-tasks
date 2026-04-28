{{
  config(materialized='view')
}}

select
  consultation_id,
  toUInt8(max(coalesce(referral_issued, 0)) > 0) as referral_issued
from {{ source('teleclinic_raw', 'clinical_outcomes') }}
group by
  consultation_id
