import csv
import io
import json
import os
import sys
from datetime import date, datetime
from decimal import Decimal
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError

import duckdb

BASE = "http://127.0.0.1:8086"
ORG = "0f90151884406864"
BUCKET_ID = "4e78489186a1fd9f"
BUCKET = "cleansed_financial_data"
MEASUREMENT = "transactions_clean"
SAMPLE_SIZE = 1000


def post(endpoint, parameters, body, content_type):
    token = os.environ.get("INFLUX_TOKEN", "").strip()
    if not token:
        raise SystemExit("Loading token missing. Enter INFLUX_TOKEN again.")

    request = Request(
        BASE + endpoint + "?" + urlencode(parameters),
        data=body.encode("utf-8"),
        headers={
            "Authorization": f"Token {token}",
            "Content-Type": content_type,
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=120) as response:
            return response.status, response.read().decode("utf-8")
    except HTTPError as error:
        # Do not print response bodies that could contain source values.
        raise SystemExit(
            f"{endpoint} failed: HTTP {error.code}"
        ) from None


def quoted(value):
    if "\n" in value or "\r" in value:
        raise ValueError("Unexpected newline in a source field.")
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def mapped_fields(record):
    encoded = []
    expected = {}

    for name, value in record.items():
        if name == "_storage_ns":
            continue

        if name == "transaction_amount":
            name = "transaction_amount_cents"
            if value is not None:
                scaled = Decimal(value) * 100
                assert scaled == scaled.to_integral_value()
                value = int(scaled)

        if isinstance(value, (datetime, date)):
            value = (
                value.isoformat(sep=" ")
                if isinstance(value, datetime)
                else value.isoformat()
            )

        if value is None:
            expected[name] = None
            continue
        if isinstance(value, bool):
            text = str(value).lower()
            literal = text
        elif isinstance(value, int):
            text = str(value)
            literal = text + "i"
        elif isinstance(value, str):
            text = value
            literal = quoted(value)
        else:
            raise TypeError(f"Unexpected type for field {name}")

        expected[name] = text
        encoded.append(f"{name}={literal}")

    return ",".join(encoded), expected


def parse_csv(text):
    header = None
    records = []
    for row in csv.reader(io.StringIO(text)):
        if not row or not any(row) or row[0].startswith("#"):
            continue
        if len(row) > 2 and row[1:3] == ["result", "table"]:
            header = row
            continue
        if header is None or len(row) != len(header):
            raise ValueError("Unexpected Flux CSV response.")
        records.append(dict(zip(header, row)))
    return records


def main():
    mode = sys.argv[1] if len(sys.argv) == 2 else ""
    if mode not in {"write", "verify"}:
        raise SystemExit(
            "Usage: python scripts/transaction_persistence_test.py "
            "write|verify"
        )

    con = duckdb.connect()
    con.execute("SET memory_limit = '1GB'")
    con.execute("SET threads = 2")
    cursor = con.execute("""
        SELECT *,
            epoch_ns(transaction_timestamp)
            + CAST(id AS BIGINT) AS _storage_ns
        FROM read_parquet('data/transactions_clean.parquet')
        ORDER BY transaction_timestamp, CAST(id AS BIGINT)
        LIMIT 1000
    """)
    names = [column[0] for column in cursor.description]
    source = [dict(zip(names, row)) for row in cursor.fetchall()]
    con.close()

    assert len(source) == SAMPLE_SIZE
    assert len({r["id"] for r in source}) == SAMPLE_SIZE
    assert len({r["_storage_ns"] for r in source}) == SAMPLE_SIZE

    expected = {}
    lines = []

    for record in source:
        numeric_id = int(record["id"])
        assert 0 <= numeric_id < 1_000_000_000
        assert record["transaction_timestamp"].microsecond == 0

        card = record["card_id"]
        assert isinstance(card, str) and card.isascii() and card.isdigit()

        fields, expected_values = mapped_fields(record)
        expected[record["id"]] = expected_values
        lines.append(
            f"{MEASUREMENT},card_key={card} "
            f"{fields} {record['_storage_ns']}"
        )

    if mode == "write":
        for start in range(0, SAMPLE_SIZE, 500):
            status, _ = post(
                "/api/v2/write",
                {"org": ORG, "bucket": BUCKET_ID, "precision": "ns"},
                "\n".join(lines[start:start + 500]),
                "text/plain; charset=utf-8",
            )
            assert status == 204
        print("PASS: Written 1,000 transaction points", flush=True)

    start_ns = min(r["_storage_ns"] for r in source)
    stop_ns = max(r["_storage_ns"] for r in source) + 1

    query = f'''
from(bucket: "{BUCKET}")
    |> range(start: time(v: {start_ns}), stop: time(v: {stop_ns}))
    |> filter(fn: (r) => r._measurement == "{MEASUREMENT}")
    |> pivot(
        rowKey: ["_time"],
        columnKey: ["_field"],
        valueColumn: "_value"
    )
'''
    _, response = post(
        "/api/v2/query",
        {"orgID": ORG},
        query,
        "application/vnd.flux",
    )

    returned = parse_csv(response)
    assert len(returned) == SAMPLE_SIZE, (
        f"Expected 1,000 points; received {len(returned)}."
    )
    actual = {row["id"]: row for row in returned}
    assert set(actual) == set(expected), "Stored transaction IDs differ."

    checked = 0
    for transaction_id, fields in expected.items():
        row = actual[transaction_id]
        for name, expected_value in fields.items():
            actual_value = row.get(name, "")
            if expected_value is None:
                assert actual_value == "", (
                    f"Expected an absent value for field {name}."
                )
            else:
                assert actual_value == expected_value, (
                    f"Value mismatch for field {name}."
                )
            checked += 1

    print("PASS: All 1,000 transaction IDs match")
    print(f"PASS: {checked:,} field comparisons, including nulls")
    print(f"PASS: Mode '{mode}' completed")


if __name__ == "__main__":
    main()
