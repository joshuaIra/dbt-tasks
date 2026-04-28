{{
  config(materialized='table')
}}

/*
  Metric definition (3–5 sentences for reviewers):

  Denominator: completed consults in the month. We report two rates so Feb–Apr are comparable:
  (1) doctor_referral_rate = share of consults with a doctor-issued outcome (class in ('doctor_referral', 'both')).
  (2) patient_request_rate = share with patient “referral requested” on intake (class in ('patient_requested_only', 'both')).
  The “both” case counts toward BOTH numerators: that consult showed intent on intake AND a doctor referral in outcomes,
  so each rate answers a different question (clinical pathway vs. patient triage) without double-counting the denominator.
  For Ministry “clinical” governance, the headline comparable series is doctor_referral_rate; patient_request_rate is
  a separate operational/product metric that only makes full sense from April (new checkbox), and should be footnoted
  in dashboards for pre-April months if intake is NULL/empty.
*/

select
  toStartOfMonth(r.report_at_utc) as report_month,
  toUInt32(count(*)) as completed_consults,

  toUInt32(countIf(r.referral_class in ('doctor_referral', 'both'))) as doctor_referral_consults,
  toFloat64(doctor_referral_consults) / nullIf(toFloat64(completed_consults), 0) as doctor_referral_rate,

  toUInt32(countIf(r.referral_class in ('patient_requested_only', 'both'))) as patient_request_consults,
  toFloat64(patient_request_consults) / nullIf(toFloat64(completed_consults), 0) as patient_request_referral_rate

from {{ ref('int_referrals_classified') }} as r
group by
  1
order by
  1
