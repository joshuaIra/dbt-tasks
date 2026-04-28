# Irembo TeleClinic — take-home (Part 2 dbt)

## Overview

This repository contains the Part 2 dbt solution only. It addresses two issues from the case:

1. Referral reporting is split into doctor-issued referrals and patient-requested referrals so April remains comparable to prior months.
2. Wait time is recalculated after normalizing mixed timezone strings to UTC and nulling impossible values.

## Project structure

- `staging`: cleans raw consultation, outcome, and intake data.
- `intermediate`: classifies each completed consultation into referral groups.
- `marts`: produces monthly referral and wait-time reporting tables.

## Models

- `stg_consultations_fixed`: fixes timestamps, excludes test patients, and computes corrected wait time.
- `stg_clinical_outcomes`: standardizes the doctor-issued referral flag.
- `stg_intake_flags`: standardizes the patient-requested referral flag.
- `int_referrals_classified`: labels each completed consultation as `both`, `doctor_referral`, `patient_requested_only`, or `no_referral`.
- `mart_referral_rate_monthly`: monthly referral counts and rates using completed consultations as the denominator.
- `mart_wait_time_monthly`: monthly corrected wait-time summary.

## Metric design

- `doctor_referral_rate` is the main comparable clinical KPI.
- `patient_request_referral_rate` is reported separately because it reflects intake behavior, not the same concept as a doctor-issued referral.
- `both` contributes to both rates because the same consultation can include both patient request and clinician referral.

## Data quality and tests

- Mixed timezone strings are normalized to UTC.
- Negative waits and waits above 24 hours are set to `NULL`.
- Test patients prefixed with `TEST_` are excluded.
- Included tests cover schema integrity, nonnegative corrected waits, and large month-over-month jumps in combined referral experience.


## AI use disclosure

I used an AI coding assistant to help draft parts of the SQL, tests, and written explanations. I reviewed, modified, and finalized the submission myself, including the model logic, metric definitions, and documentation. Final responsibility for the submitted work is mine.
