import csv
import json
from datetime import datetime
from pathlib import Path

import duckdb
import transaction_persistence_test as mapping

metrics = [
    "rows", "net_cents", "positive_cents",
    "negative_cents", "negative_count", "zero_count",
]

con = duckdb.connect()
con.execute("SET memory_limit = '1GB'")
con.execute("SET threads = 2")

baseline = con.execute("""
    WITH prepared AS (
        SELECT
            date_trunc('month', transaction_timestamp) AS month,
            CAST(transaction_amount * 100 AS BIGINT) AS cents
        FROM read_parquet('data/transactions_clean.parquet')
    )
    SELECT
        month,
        COUNT(*),
        SUM(cents),
        SUM(CASE WHEN cents > 0 THEN cents ELSE 0 END),
        SUM(CASE WHEN cents < 0 THEN cents ELSE 0 END),
        COUNT(*) FILTER (WHERE cents < 0),
        COUNT(*) FILTER (WHERE cents = 0)
    FROM prepared
    GROUP BY month
    ORDER BY month
""").fetchall()
con.close()

expected_months = [
    datetime(year, month, 1)
    for year in range(2010, 2020)
    for month in range(1, 13)
    if (year, month) <= (2019, 10)
]
assert [row[0] for row in baseline] == expected_months

totals = {name: 0 for name in metrics}
report = Path("reports/transactions_post_restart_monthly.csv")
report.parent.mkdir(parents=True, exist_ok=True)

columns = ["month"]
for name in metrics:
    columns.extend([f"source_{name}", f"stored_{name}"])
columns.append("status")

with report.open("w", newline="", encoding="utf-8") as handle:
    writer = csv.DictWriter(handle, fieldnames=columns)
    writer.writeheader()

    for baseline_row in baseline:
        month = baseline_row[0]
        following = (
            datetime(month.year + 1, 1, 1)
            if month.month == 12
            else datetime(month.year, month.month + 1, 1)
        )
        expected = dict(zip(
            metrics, [int(value) for value in baseline_row[1:]]
        ))

        query = f'''
from(bucket: "{mapping.BUCKET}")
    |> range(
        start: {month.strftime("%Y-%m-%dT00:00:00Z")},
        stop: {following.strftime("%Y-%m-%dT00:00:00Z")}
    )
    |> filter(fn: (r) =>
        r._measurement == "{mapping.MEASUREMENT}" and
        r._field == "transaction_amount_cents"
    )
    |> keep(columns: ["_value"])
    |> group(columns: [])
    |> reduce(
        identity: {{
            rows: 0, net_cents: 0, positive_cents: 0,
            negative_cents: 0, negative_count: 0, zero_count: 0
        }},
        fn: (r, accumulator) => ({{
            rows: accumulator.rows + 1,
            net_cents: accumulator.net_cents + r._value,
            positive_cents: accumulator.positive_cents +
                (if r._value > 0 then r._value else 0),
            negative_cents: accumulator.negative_cents +
                (if r._value < 0 then r._value else 0),
            negative_count: accumulator.negative_count +
                (if r._value < 0 then 1 else 0),
            zero_count: accumulator.zero_count +
                (if r._value == 0 then 1 else 0)
        }})
    )
'''
        _, response = mapping.post(
            "/api/v2/query",
            {"orgID": mapping.ORG},
            query,
            "application/vnd.flux",
        )
        returned = mapping.parse_csv(response)
        assert len(returned) == 1, (
            f"Unexpected aggregate result for {month:%Y-%m}."
        )
        actual = {
            name: int(returned[0][name]) for name in metrics
        }
        assert actual == expected, (
            f"Mismatch for {month:%Y-%m}: "
            f"source={expected}, stored={actual}"
        )

        result = {"month": month.strftime("%Y-%m"), "status": "PASS"}
        for name in metrics:
            result[f"source_{name}"] = expected[name]
            result[f"stored_{name}"] = actual[name]
            totals[name] += actual[name]

        writer.writerow(result)
        handle.flush()
        print(
            f"PASS: {month:%Y-%m} — all six statistics match",
            flush=True,
        )

assert totals == {
    "rows": 13305915,
    "net_cents": 57183552228,
    "positive_cents": 63935456462,
    "negative_cents": -6751904234,
    "negative_count": 660049,
    "zero_count": 10639,
}

summary = {
    "status": "PASS",
    "verification_scope": "Post-restart monthly counts and monetary statistics",
    "months_verified": len(baseline),
    **totals,
}
summary_path = Path("reports/transactions_post_restart_summary.json")
summary_path.write_text(
    json.dumps(summary, indent=2) + "\n",
    encoding="utf-8",
)

print("\nPASS: All 118 months reconciled after restart")
for name, value in totals.items():
    print(f"{name}: {value:,}")
print(f"Saved: {report}")
print(f"Saved: {summary_path}")
