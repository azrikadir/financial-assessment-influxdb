import csv
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import duckdb
import transaction_persistence_test as mapping

SNAPSHOT_SECONDS = int(
    datetime(2026, 10, 13, tzinfo=timezone.utc).timestamp()
)

MONEY_FIELDS = {
    "per_capita_income", "yearly_income",
    "total_debt", "credit_limit",
}
COORDINATE_FIELDS = {"latitude", "longitude"}


def scaled_integer(value, scale):
    scaled = Decimal(value) * scale
    assert scaled == scaled.to_integral_value(), (
        "Scaling would lose decimal precision."
    )
    return int(scaled)


def transform(record):
    result = {}
    for name, value in record.items():
        if name in MONEY_FIELDS:
            result[name + "_cents"] = (
                None if value is None else scaled_integer(value, 100)
            )
        elif name in COORDINATE_FIELDS:
            result[name + "_microdegrees"] = (
                None if value is None
                else scaled_integer(value, 1_000_000)
            )
        else:
            result[name] = value
    return result


def main():
    mode = sys.argv[1] if len(sys.argv) == 2 else ""
    if mode not in {"write", "verify"}:
        raise SystemExit(
            "Usage: python scripts/load_users_cards.py write|verify"
        )

    con = duckdb.connect()
    con.execute("SET memory_limit = '1GB'")
    results = []

    datasets = [
        ("users", 2000, 14, "user_key"),
        ("cards", 6146, 13, "card_key"),
    ]

    for dataset, expected_count, expected_columns, tag in datasets:
        measurement = dataset + "_clean"
        relation = con.read_parquet(f"data/{measurement}.parquet")
        columns = relation.columns
        assert len(columns) == expected_columns

        records = [
            dict(zip(columns, row))
            for row in relation.fetchall()
        ]
        assert len(records) == expected_count

        expected = {}
        lines = []

        for record in records:
            identifier = record["id"]
            assert isinstance(identifier, str)
            assert identifier.isascii() and identifier.isdigit()

            fields, values = mapping.mapped_fields(transform(record))
            expected[identifier] = values
            lines.append(
                f"{measurement},{tag}={identifier} "
                f"{fields} {SNAPSHOT_SECONDS}"
            )

        assert len(expected) == expected_count, "Duplicate source IDs."

        if mode == "write":
            for start in range(0, len(lines), 500):
                batch = lines[start:start + 500]
                status, _ = mapping.post(
                    "/api/v2/write",
                    {
                        "org": mapping.ORG,
                        "bucket": mapping.BUCKET_ID,
                        "precision": "s",
                    },
                    "\n".join(batch),
                    "text/plain; charset=utf-8",
                )
                assert status == 204
            print(
                f"PASS: Written {expected_count:,} {dataset}",
                flush=True,
            )

        query = f'''
from(bucket: "{mapping.BUCKET}")
    |> range(
        start: 2026-10-13T00:00:00Z,
        stop: 2026-10-13T00:00:01Z
    )
    |> filter(fn: (r) => r._measurement == "{measurement}")
    |> pivot(
        rowKey: ["_time"],
        columnKey: ["_field"],
        valueColumn: "_value"
    )
'''
        _, response = mapping.post(
            "/api/v2/query",
            {"orgID": mapping.ORG},
            query,
            "application/vnd.flux",
        )

        returned = mapping.parse_csv(response)
        assert len(returned) == expected_count, (
            f"Stored record count mismatch for {dataset}."
        )
        actual = {row["id"]: row for row in returned}
        assert set(actual) == set(expected), (
            f"Stored IDs differ for {dataset}."
        )

        comparisons = 0
        for identifier, fields in expected.items():
            stored = actual[identifier]
            assert stored.get(tag) == identifier, "Tag/ID mismatch."

            for name, value in fields.items():
                assert stored.get(name, "") == (
                    "" if value is None else value
                ), f"Field mismatch: {dataset}.{name}"
                comparisons += 1

        assert comparisons == expected_count * expected_columns

        print(
            f"PASS: {dataset} — {expected_count:,} IDs and "
            f"{comparisons:,} field comparisons match",
            flush=True,
        )
        results.append({
            "dataset": dataset,
            "source_rows": expected_count,
            "stored_rows": len(returned),
            "field_comparisons": comparisons,
            "mode": mode,
            "status": "PASS",
        })

    con.close()

    report = Path(f"reports/users_cards_{mode}_verification.csv")
    report.parent.mkdir(parents=True, exist_ok=True)
    with report.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(results[0]))
        writer.writeheader()
        writer.writerows(results)

    print(f"Saved: {report}")
    print("Users and cards verification complete.")


if __name__ == "__main__":
    main()
