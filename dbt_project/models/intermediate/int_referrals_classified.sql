{{
  config(materialized='view')
}}

/*
  One row per completed consult with separate doctor-issued and patient-requested signals.
*/

select
  c.consultation_id,
  c.report_at_utc,

  toUInt8(coalesce(o.referral_issued, 0) > 0) as is_doctor_referral,
  toUInt8(coalesce(f.referral_requested, 0) > 0) as is_patient_request,

  multiIf(
    (coalesce(o.referral_issued, 0) > 0) and (coalesce(f.referral_requested, 0) > 0), 'both',
    coalesce(o.referral_issued, 0) > 0, 'doctor_referral',
    coalesce(f.referral_requested, 0) > 0, 'patient_requested_only',
    'no_referral'
  ) as referral_class

from {{ ref('stg_consultations_fixed') }} as c
left join {{ ref('stg_clinical_outcomes') }} as o
  on c.consultation_id = o.consultation_id
left join {{ ref('stg_intake_flags') }} as f
  on c.consultation_id = f.consultation_id
where
  c.status = 'completed'
