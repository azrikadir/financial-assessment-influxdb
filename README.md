# Financial Assessment — InfluxDB Loading and Verification

This repository contains the Python scripts and verification reports for the
InfluxDB loading stage of Test 1, Scenario D.

The work loads cleansed financial data exported from Databricks into InfluxDB,
compares stored records with the Parquet source files, and checks persistence
after restarting the database.

## 1. Assessment scope

Scenario D requires:

1. Applying Python cleansing and enrichment based on profiling findings.
2. Loading the cleansed data into InfluxDB.
3. Reporting before-and-after data-quality statistics.

Cleansing and before-and-after reporting were performed in Databricks and are
documented in the assessment notebooks and presentation. This repository
contains the subsequent InfluxDB loading and verification work.

The wider assessment also covers ingestion, profiling, data modeling and
Power BI analytics.

## 2. Data flow

1. Original users, cards and transactions CSV files were ingested into Databricks.
2. Profiling findings informed cleansing, type conversion and review flags.
3. Cleansed datasets were exported as Parquet files.
4. The Parquet files were uploaded to GitHub Codespaces and reconciled.
5. Python scripts loaded the data into InfluxDB.
6. Read-back checks compared stored data with the Parquet files.
7. Further verification was performed after restarting InfluxDB.

Power BI uses the analytical tables prepared in Databricks. It does not use
the InfluxDB database as its dashboard source in this implementation.

## 3. Environment

| Component | Environment used |
|---|---|
| Upstream preparation | Databricks Free Edition |
| Loading environment | GitHub Codespaces |
| Python | 3.14.2 |
| DuckDB | 1.5.6 |
| Database image | influxdb:2.9.1 |
| Container name | financial-influxdb |
| Local endpoint | http://127.0.0.1:8086 |
| Bucket | cleansed_financial_data |
| Retention | Forever |
| Shard-group duration | 31,449,600 seconds — 52 weeks |

Persistent container mounts:

| Codespaces directory | Container directory |
|---|---|
| .influxdb/config | /etc/influxdb2 |
| .influxdb/data | /var/lib/influxdb2 |

Databricks Free Edition was used instead of an Azure Databricks deployment.
Python and DuckDB were used for profiling instead of DQOps after the attempted
DQOps setup required a license key. These substitutions are disclosed in the
assessment documentation.

## 4. Input datasets

| File | Rows | Columns |
|---|---:|---:|
| users_clean.parquet | 2,000 | 14 |
| cards_clean.parquet | 6,146 | 13 |
| transactions_clean.parquet | 13,305,915 | 22 |

Transaction coverage is 1 January 2010 through 31 October 2019.

The input files, credentials and live database storage are excluded from Git.
A fresh clone therefore does not contain the datasets or a running database.

To reproduce the work, obtain the corresponding cleansed Parquet exports,
configure the file paths and authentication expected by the scripts, and
prepare the InfluxDB container and bucket before executing the loading stages.

## 5. Scripts and execution sequence

The scripts were used progressively to validate the setup before loading
the complete transaction history.

| Order | Script | Purpose |
|---|---|---|
| 1 | scripts/transaction_persistence_test.py | Pilot transaction write/read verification |
| 2 | scripts/january_load_test.py | January 2010 loading and verification |
| 3 | scripts/load_transaction_range.py | Loading and verification for subsequent date ranges |
| 4 | scripts/load_users_cards.py | Users and cards loading and field reconciliation |
| 5 | scripts/verify_transactions_after_restart.py | Transaction verification after database restart |

Users and cards were also verified again after restart using the verification
mode of their loading script.

Before running a script, review its configuration and command-line arguments.
Use its verification mode when checking data already loaded, where supported.
Authentication must be supplied using the environment variables expected by
the script; credentials must not be embedded in committed source code.

The execution sequence above documents the completed workflow. Exact arguments,
input paths and environment-variable names are defined by the scripts.

## 6. Verified results

### Record and field reconciliation

| Check | Verified result |
|---|---:|
| Users | 2,000 |
| Cards | 6,146 |
| Transactions | 13,305,915 |
| Daily transaction coverage | 3,591 days |
| Transaction field comparisons during loading | 292,730,130 |
| User field comparisons | 28,000 |
| Card field comparisons | 79,898 |

Transaction loading verification covered all 22 prepared transaction fields,
including null handling. Users and cards were reconciled by ID and field values.

### Transaction amount reconciliation

| Measure | Amount |
|---|---:|
| Net transaction amount | 571,835,522.28 |
| Positive transaction amount | 639,354,564.62 |
| Signed negative transaction amount | -67,519,042.34 |

Monetary reconciliation used integer cents to avoid floating-point comparison
differences. The verified net amount is 57,183,552,228 cents.

These are all-record totals. They differ from the qualifying-spend measure
used in the dashboard, which includes positive transactions with no recorded
error.

### Post-restart verification

Transaction checks after restart covered 118 months and reconciled record
counts, monetary totals, and negative/zero transaction counts.

Users and cards were checked again by ID and field values after restart.

The post-restart transaction checks were aggregate checks. They are distinct
from the detailed transaction field comparisons performed during loading.

## 7. Evidence index

All reports are stored in `reports/`.

| Report | Evidence |
|---|---|
| january_2010_write_verification.csv | January loading verification |
| january_2010_verify_verification.csv | January verification-only run |
| transactions_*_write.csv | Subsequent transaction-range verification |
| transactions_all_daily_verification.csv | Consolidated daily transaction verification |
| transactions_load_verification_summary.json | Overall transaction loading summary |
| transactions_post_restart_monthly.csv | Monthly transaction checks after restart |
| transactions_post_restart_summary.json | Post-restart transaction summary |
| users_cards_write_verification.csv | Users and cards loading verification |
| users_cards_verify_verification.csv | Users and cards subsequent verification |

Detailed cleansing rules and before-and-after statistics are supplied separately
in the Databricks assessment materials.

## 8. Operational issue and recovery

The initial InfluxDB container repeatedly exited and later failed during startup
while reopening 147 shards. The exact cause was not established. Memory pressure
was suspected but was not proven.

A replacement setup used explicit persistent storage paths and 52-week shard
groups. Authentication, restart behavior, pilot writes and progressively larger
loads were checked before completing the import.

The replacement setup completed loading and post-restart verification.
This successful outcome does not establish the original crash cause.

## 9. Repository contents and exclusions

Included:

- Python loading and verification scripts.
- CSV and JSON verification reports.
- Project documentation and Git exclusion rules.

Excluded:

- API tokens, passwords and environment files containing credentials.
- Live InfluxDB database and configuration storage.
- Raw datasets and cleansed Parquet files.
- Python virtual environments and compiled cache files.
- The separately retained evidence ZIP archive.

This repository supports the assessment submission alongside the Databricks
notebooks, ERDs, before-and-after reports, Power BI file and presentation.