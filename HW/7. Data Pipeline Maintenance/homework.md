# Week 5 Data Pipeline Maintenance Homework

## Introduction

Pacific Infra Group provides a SaaS platform that allows businesses to sign up, manage licenses, and purchase additional features or support. The following document outlines the five critical data pipelines that support financial reporting, business growth, and engagement metrics. These pipelines directly impact investor reporting and internal experimentation.

Each pipeline has designated primary and secondary owners, on-call schedules, and predefined SLAs to ensure smooth operations.

## 5 Pipelines to cover Business Areas

1. Profit
2. Growth
3. Engagement
4. Aggregate Pipeline for Investors/ Stakeholders/ Business Owners
5. Aggregate Pipeline for Experiment Team

The pipelines that will affect the investors are:

- Profit
- Growth
- Engagement
- Aggregate Pipeline for Investors/ Stakeholders/ Business Owners

These

The Aggregate Pipeline to Experiment team is for the Data Analytics/Science team to conduct experiments on features. Hence, it won't be covered in the runbook. It will be fixed as you go i.e. when an analyst or a data scientist will notice any discrepancies in data.

## Pipeline Runbooks

### Profit Pipeline

1. Pipeline Name: Profit
2. Types of Data:

   - Revenue from customer accounts
   - Expenses on assets and services
   - Aggregated salaries by team

3. Owners:

   - Primary Owner: Finance Team/ Risk Team
   - Secondary Owner: Data Engineering Team

4. Common Issues:

   - Mismatch in reported profit numbers vs financial filings (requires accountant verification)
   - Data discrepancies due to missing revenue or expenses
   - Example: In Q3 2024, a mismatch in expenses due to incorrect data ingestion led to a 5% overstatement of profit, which was later corrected through an internal audit.

5. SLA's:

   - Monthly review by the accounts team

6. On-Call Schedule:
   - Monitored by the BI in the profit team.
   - Weekly rotation for on-call support
   - Fixes required for any pipeline failures
   - Holiday Handling: If an on-call shift coincides with a holiday, engineers can swap shifts in advance. A backup engineer is also designated in case of urgent issues.

### Growth Pipeline

1. Pipeline Name: Growth
2. Types of Data:

   - Changes in account type (upgrades, downgrades, cancellations)
   - New account subscriptions
   - Accounts that cancelled subscriptions
   - Renewals for the next year

3. Owners:

   - Primary Owner: Accounts Team
   - Secondary Owner: Data Engineering Team

4. Common Issues:

   - Missing time-series data due to skipped update steps
   - Failure in tracking account status changes
   - Example: In a recent incident, an account renewal record was missing, leading to incorrect churn rate calculation. This issue was traced to a delay in syncing data from the CRM system.

5. SLA's:

   - Account status updates will be available by end of each week

6. On-Call Schedule:
   - No immediate on-call support
   - Issues are debugged during business hours
   - Holiday Handling: Engineers are responsible for scheduling coverage if out-of-office during critical reporting periods.

### Engagement Pipeline

1. Pipeline Name: Engagement
2. Types of Data:

   - Clickstream data from platform users
   - Time spent per user per day

3. Owners:

   - Primary Owner: Software Frontend Team
   - Secondary Owner: Data Engineering Team

4. Common Issues:

   - Delayed arrival of click data to Kafka queues
   - Kafka downtime resulting in data loss
   - Duplicate event records requiring deduplication
   - Example: Last quarter, a Kafka failure caused a 12-hour gap in engagement tracking, requiring a backfill of missing user activity logs.

5. SLA's:

   - Data will be available within 48 hours
   - Issues must be resolved within 1 week

6. On-Call Schedule:
   - Weekly rotation for DE team monitoring the pipeline
   - 30-minute onboarding meetings for the next on-call person
     - Details will be shared about anything that is currently broken or broke over the week.
   - Holiday Handling: Engineers coordinate backup coverage to ensure continuity of monitoring.

### Aggregated Data Pipeline for Executives and Investors

1. Pipeline Name: Aggregated Data for Investors and Executives
2. Types of Data:

   - Aggregated data for profit, growth, and engagement metrics across all customers and business units

3. Owners:

   - Primary Owner: Data Analytics/ Science Team
   - Secondary Owner: Data Engineering Team

4. Common Issues:

   - Spark JOIN Failures: When merging large datasets, Spark may run out of memory (OOM errors). This can occur due to inefficient joins, requiring repartitioning or optimized queries.
   - Stale Data: Reports may use outdated data if dependencies in upstream pipelines fail. Regular monitoring helps identify inconsistencies.
   - Missing Data: If critical data is absent, calculations may lead to NA values or divide-by-zero errors.
   - Example: In a previous quarter, stale engagement data led to incorrect investor KPIs. The issue was resolved by implementing a validation check before report generation.

5. SLAs:

   - Issues must be resolved by the end of the month before investor reports are finalized.

6. On-Call Schedule:
   - Data Engineering team actively monitors during the last week of the month
   - Continuous checks to ensure smooth report generation
   - Holiday Handling: Engineers handling investor reports must coordinate leave in advance to ensure coverage during reporting cycles.
