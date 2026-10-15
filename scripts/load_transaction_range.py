import csv
import sys
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import transaction_persistence_test as mapping


def main():
    import argparse
    import shutil

    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["write", "verify"])
    parser.add_argument("--start", required=True)
    parser.add_argument("--stop", required=True)
    args = parser.parse_args()

    mode = args.mode
    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    stop_date = datetime.strptime(args.stop, "%Y-%m-%d")
    assert datetime(2010, 1, 1) <= start_date < stop_date
    assert stop_date <= datetime(2019, 11, 1)

    con = duckdb.connect()
    con.execute("SET memory_limit = '1GB'")
    con.execute("SET threads = 2")

    report = Path(f"reports/transactions_{args.start}_{args.stop}_{mode}.csv")
    report.parent.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    total_cents = 0
    day = start_date
    end = stop_date

    with report.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=[
            "date", "source_rows", "stored_rows",
            "source_net_cents", "stored_net_cents",
            "field_comparisons", "status"
        ])
        writer.writeheader()

        while day < end:
            if shutil.disk_usage(".").free < 3 * 1024**3:
                raise SystemExit(
                    f"Paused before {day.date()}: less than 3 GiB free. "
                    "Share this message before continuing."
                )
            print(f"Processing {day.date()}...", flush=True)
            following_day = day + timedelta(days=1)

            cursor = con.execute("""
                SELECT *,
                    epoch_ns(transaction_timestamp)
                    + CAST(id AS BIGINT) AS _storage_ns
                FROM read_parquet('data/transactions_clean.parquet')
                WHERE transaction_timestamp >= ?
                  AND transaction_timestamp < ?
                ORDER BY transaction_timestamp, CAST(id AS BIGINT)
            """, [day, following_day])

            names = [column[0] for column in cursor.description]
            source = [
                dict(zip(names, row)) for row in cursor.fetchall()
            ]

            expected = {}
            lines = []
            storage_times = set()

            for record in source:
                numeric_id = int(record["id"])
                assert 0 <= numeric_id < 1_000_000_000
                assert record["transaction_timestamp"].microsecond == 0

                card = record["card_id"]
                assert isinstance(card, str)
                assert card.isascii() and card.isdigit()

                fields, values = mapping.mapped_fields(record)
                expected[record["id"]] = values
                storage_times.add(record["_storage_ns"])
                lines.append(
                    f"{mapping.MEASUREMENT},card_key={card} "
                    f"{fields} {record['_storage_ns']}"
                )

            assert len(expected) == len(source), "Duplicate source IDs."
            assert len(storage_times) == len(source), "Timestamp collision."

            if mode == "write":
                for start in range(0, len(lines), 500):
                    status, _ = mapping.post(
                        "/api/v2/write",
                        {
                            "org": mapping.ORG,
                            "bucket": mapping.BUCKET_ID,
                            "precision": "ns",
                        },
                        "\n".join(lines[start:start + 500]),
                        "text/plain; charset=utf-8",
                    )
                    assert status == 204

            query = f'''
from(bucket: "{mapping.BUCKET}")
    |> range(
        start: {day.strftime("%Y-%m-%dT00:00:00Z")},
        stop: {following_day.strftime("%Y-%m-%dT00:00:00Z")}
    )
    |> filter(fn: (r) => r._measurement == "{mapping.MEASUREMENT}")
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
            actual = {row["id"]: row for row in returned}

            assert len(returned) == len(source), (
                f"Point count mismatch on {day.date()}."
            )
            assert set(actual) == set(expected), (
                f"ID mismatch on {day.date()}."
            )

            comparisons = 0
            for transaction_id, fields in expected.items():
                stored = actual[transaction_id]
                for name, value in fields.items():
                    assert stored.get(name, "") == (
                        "" if value is None else value
                    ), f"Field mismatch on {day.date()}: {name}"
                    comparisons += 1

            source_cents = sum(
                int(row["transaction_amount_cents"])
                for row in expected.values()
            )
            stored_cents = sum(
                int(row["transaction_amount_cents"])
                for row in actual.values()
            )
            assert source_cents == stored_cents

            writer.writerow({
                "date": str(day.date()),
                "source_rows": len(source),
                "stored_rows": len(returned),
                "source_net_cents": source_cents,
                "stored_net_cents": stored_cents,
                "field_comparisons": comparisons,
                "status": "PASS",
            })
            handle.flush()

            total_rows += len(source)
            total_cents += source_cents
            print(
                f"PASS: {day.date()} — {len(source):,} transactions; "
                f"all fields match",
                flush=True,
            )
            day = following_day

    con.close()
    print(f"\nPASS: Requested-range transactions verified: {total_rows:,}")
    print(f"PASS: Requested-range net amount: {total_cents:,} cents")
    print(f"Report saved: {report}")
    print(f"Mode completed: {mode}")


if __name__ == "__main__":
    main()
