{{
  config(severity: warn)
}}

/*
  Fails (returns rows) if “any referral” share (not no_referral) jumps >10pp vs prior month.
  Tuned to catch dashboard-style headline jumps; adjust threshold. Uses same grain as the mart.
  description: Catches month-over-month inflation in combined referral experience before it ships to the Ministry.
*/

with monthly as (

  select
    toStartOfMonth(r.report_at_utc) as report_month,
    toFloat64(countIf(r.referral_class != 'no_referral')) / toFloat64(nullIf(count(), 0)) as any_referral_rate
  from {{ ref('int_referrals_classified') }} as r
  group by
    1

)

select
  cur.report_month,
  cur.any_referral_rate,
  prev.any_referral_rate as prev_rate,
  (cur.any_referral_rate - prev.any_referral_rate) as jump
from monthly as cur
inner join monthly as prev
  on addMonths(prev.report_month, 1) = cur.report_month
where (cur.any_referral_rate - prev.any_referral_rate) > 0.1
