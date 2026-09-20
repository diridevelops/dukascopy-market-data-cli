import csv
import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq

from dukascopy_market_data.candles import DownloadError
from dukascopy_market_data.cli import build_argument_parser, main
from dukascopy_market_data.ticks import (
    TICKS_BASE_URL,
    DataValidationError,
    decode_tick_json_bytes,
    decode_tick_payload,
    build_tick_endpoint_url,
    resolve_hours,
    run_tick_date,
    run_tick_hour,
    tick_csv_path,
    tick_json_path,
    tick_parquet_path,
    validate_hour,
)


REQUESTED_DATE = date(2026, 9, 1)


def timestamp_for_hour(requested_date: date, hour: int) -> int:
    return int(
        datetime.combine(
            requested_date,
            datetime.min.time(),
            tzinfo=timezone.utc,
        ).timestamp()
        * 1000
    ) + hour * 3_600_000


def tick_payload(requested_date: date = REQUESTED_DATE, hour: int = 1) -> dict:
    return {
        "timestamp": timestamp_for_hour(requested_date, hour),
        "multiplier": 0.01,
        "ask": 1.10,
        "bid": 1.00,
        "times": [10, 5, 0],
        "asks": [0, 1, -2],
        "bids": [0, 2, -1],
        "askVolumes": [1_500_000.0, 2_000_000.0, 2_500_000.0],
        "bidVolumes": [1_000_000.0, 1_250_000.0, 1_750_000.0],
    }


def tick_bytes(requested_date: date = REQUESTED_DATE, hour: int = 1) -> bytes:
    return json.dumps(tick_payload(requested_date, hour), separators=(",", ":")).encode("utf-8")


def empty_tick_bytes(requested_date: date = REQUESTED_DATE, hour: int = 1) -> bytes:
    return json.dumps(
        {
            "timestamp": timestamp_for_hour(requested_date, hour),
            "multiplier": 0.00001,
            "ask": None,
            "bid": None,
            "times": [],
            "asks": [],
            "bids": [],
            "askVolumes": [],
            "bidVolumes": [],
        },
        separators=(",", ":"),
    ).encode("utf-8")


class DukascopyTickTests(unittest.TestCase):
    def test_tick_url_hour_validation_and_paths(self) -> None:
        self.assertEqual(validate_hour("01"), 1)
        self.assertEqual(resolve_hours(None), tuple(range(24)))
        self.assertEqual(resolve_hours(3), (3,))
        self.assertEqual(
            build_tick_endpoint_url("A.US-USD", REQUESTED_DATE, 1),
            f"{TICKS_BASE_URL}/A.US-USD/2026/9/1/1",
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            self.assertEqual(
                tick_json_path(root, "EUR-USD", REQUESTED_DATE, 1),
                root
                / "instrument=EUR-USD"
                / "json"
                / "ticks"
                / "year=2026"
                / "month=09"
                / "day=01"
                / "EUR-USD-2026-09-01-01-TICKS.json",
            )
            self.assertEqual(
                tick_parquet_path(root, "EUR-USD", REQUESTED_DATE, 1),
                root
                / "instrument=EUR-USD"
                / "tf=1tick"
                / "year=2026"
                / "month=09"
                / "day=01"
                / "hour=01"
                / "EUR-USD-2026-09-01-01-TICKS.parquet",
            )
        for invalid_hour in (-1, 24, "x"):
            with self.assertRaises(ValueError):
                validate_hour(invalid_hour)

    def test_tick_decoder_accumulates_times_and_prices_and_preserves_rows(self) -> None:
        ticks = decode_tick_json_bytes(
            tick_bytes(),
            requested_date=REQUESTED_DATE,
            hour=1,
        )
        self.assertEqual(len(ticks), 3)
        self.assertEqual(
            [tick.timestamp_ms for tick in ticks],
            [timestamp_for_hour(REQUESTED_DATE, 1) + 10, timestamp_for_hour(REQUESTED_DATE, 1) + 15, timestamp_for_hour(REQUESTED_DATE, 1) + 15],
        )
        self.assertEqual(
            [tick.bid_price for tick in ticks],
            [Decimal("1"), Decimal("1.02"), Decimal("1.01")],
        )
        self.assertEqual(
            [tick.ask_price for tick in ticks],
            [Decimal("1.1"), Decimal("1.11"), Decimal("1.09")],
        )
        self.assertEqual([tick.bid_volume for tick in ticks], [1_000_000, 1_250_000, 1_750_000])
        self.assertEqual([tick.ask_volume for tick in ticks], [1_500_000, 2_000_000, 2_500_000])

    def test_tick_decoder_accepts_empty_payload_and_rejects_malformed_payloads(self) -> None:
        self.assertEqual(
            decode_tick_json_bytes(
                empty_tick_bytes(),
                requested_date=REQUESTED_DATE,
                hour=1,
            ),
            [],
        )

        mismatched = tick_payload()
        mismatched["bids"] = mismatched["bids"][:-1]
        with self.assertRaises(DataValidationError):
            decode_tick_payload(mismatched, requested_date=REQUESTED_DATE, hour=1)

        nonzero_first_delta = tick_payload()
        nonzero_first_delta["asks"][0] = 1
        with self.assertRaises(DataValidationError):
            decode_tick_payload(nonzero_first_delta, requested_date=REQUESTED_DATE, hour=1)

        fractional_volume = tick_payload()
        fractional_volume["bidVolumes"][0] = 1.5
        with self.assertRaises(DataValidationError):
            decode_tick_payload(fractional_volume, requested_date=REQUESTED_DATE, hour=1)

        nonempty_null_header = tick_payload()
        nonempty_null_header["bid"] = None
        with self.assertRaises(DataValidationError):
            decode_tick_payload(nonempty_null_header, requested_date=REQUESTED_DATE, hour=1)

        with self.assertRaises(DataValidationError):
            decode_tick_json_bytes(b"not json")

    def test_tick_parquet_schema_is_one_row_per_tick(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            result = run_tick_hour(
                "EUR-USD",
                REQUESTED_DATE,
                1,
                output_root=root,
                fetcher=lambda url: tick_bytes(REQUESTED_DATE, int(url.rsplit("/", 1)[1])),
            )
            self.assertTrue(result.parquet_created)
            parquet_path = tick_parquet_path(root, "EUR-USD", REQUESTED_DATE, 1)
            table = pq.read_table(parquet_path)
            self.assertEqual(
                table.column_names,
                ["timestamp", "bidPrice", "askPrice", "bidVolume", "askVolume"],
            )
            self.assertEqual(table.num_rows, 3)
            self.assertEqual(str(table.schema.field("timestamp").type), "timestamp[ms, tz=UTC]")
            self.assertEqual(str(table.schema.field("bidPrice").type), "double")
            self.assertEqual(str(table.schema.field("bidVolume").type), "int64")

    def test_tick_csv_output_has_exact_schema_and_iso_timestamps(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            result = run_tick_hour(
                "EUR-USD",
                REQUESTED_DATE,
                1,
                output_root=root,
                fetcher=lambda url: tick_bytes(REQUESTED_DATE, int(url.rsplit("/", 1)[1])),
                output_format="csv",
            )

            self.assertTrue(result.csv_path is not None)
            output_path = tick_csv_path(root, "EUR-USD", REQUESTED_DATE, 1)
            self.assertEqual(result.csv_path, output_path)
            with output_path.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.reader(handle))
            self.assertEqual(
                rows[0],
                ["timestamp", "bidPrice", "askPrice", "bidVolume", "askVolume"],
            )
            self.assertEqual(len(rows), 4)
            self.assertEqual(rows[1][0], "2026-09-01T01:00:00.010Z")
            self.assertEqual(rows[1][3:], ["1000000", "1500000"])

    def test_tick_csv_reuses_cache_and_does_not_collide_with_parquet(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            parquet_result = run_tick_hour(
                "EUR-USD",
                REQUESTED_DATE,
                1,
                output_root=root,
                fetcher=lambda _: tick_bytes(),
            )
            raw_before = parquet_result.json_path.read_bytes()

            csv_result = run_tick_hour(
                "EUR-USD",
                REQUESTED_DATE,
                1,
                output_root=root,
                fetcher=lambda _: self.fail("CSV tick output must reuse cached JSON"),
                output_format="csv",
            )
            self.assertTrue(csv_result.csv_path is not None)
            self.assertTrue(csv_result.csv_path.exists())
            self.assertTrue(parquet_result.parquet_path.exists())
            self.assertEqual(csv_result.json_path.read_bytes(), raw_before)

            skipped = run_tick_hour(
                "EUR-USD",
                REQUESTED_DATE,
                1,
                output_root=root,
                fetcher=lambda _: self.fail("existing CSV tick output must be skipped"),
                output_format="csv",
            )
            self.assertTrue(skipped.parquet_skipped)
            self.assertIsNone(skipped.parquet_path)

    def test_tick_cache_reuse_empty_hours_and_existing_parquet_skip(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            calls: list[str] = []

            def fetch(url: str) -> bytes:
                calls.append(url)
                return tick_bytes(REQUESTED_DATE, 1)

            first = run_tick_hour(
                "EUR-USD", REQUESTED_DATE, 1, output_root=root, fetcher=fetch
            )
            raw_before = first.json_path.read_bytes()

            def fail_fetch(_: str) -> bytes:
                self.fail("a cached tick JSON must be reused")

            second = run_tick_hour(
                "EUR-USD", REQUESTED_DATE, 1, output_root=root, fetcher=fail_fetch
            )
            self.assertTrue(second.parquet_skipped)
            self.assertEqual(calls, [f"{TICKS_BASE_URL}/EUR-USD/2026/9/1/1"])
            self.assertEqual(second.json_path.read_bytes(), raw_before)

            empty = run_tick_hour(
                "EUR-USD",
                REQUESTED_DATE,
                2,
                output_root=root,
                fetcher=lambda _: empty_tick_bytes(REQUESTED_DATE, 2),
            )
            self.assertTrue(empty.is_empty)
            self.assertIsNone(empty.parquet_path)
            self.assertTrue(empty.json_path.exists())
            self.assertFalse(tick_parquet_path(root, "EUR-USD", REQUESTED_DATE, 2).exists())

    def test_invalid_cached_tick_json_does_not_redownload(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            cached = tick_json_path(root, "EUR-USD", REQUESTED_DATE, 1)
            cached.parent.mkdir(parents=True)
            cached.write_bytes(b"invalid")

            with self.assertRaises(DataValidationError):
                run_tick_hour(
                    "EUR-USD",
                    REQUESTED_DATE,
                    1,
                    output_root=root,
                    fetcher=lambda _: self.fail("invalid cache must not redownload"),
                )
            self.assertEqual(cached.read_bytes(), b"invalid")

    def test_tick_date_processes_all_hours_and_continues_after_failure(self) -> None:
        first_date = REQUESTED_DATE
        second_date = date(2026, 9, 2)
        calls: list[str] = []

        def fetch(url: str) -> bytes:
            calls.append(url)
            hour = int(url.rsplit("/", 1)[1])
            year, month, day = (int(value) for value in url.split("/")[-4:-1])
            requested_date = date(year, month, day)
            if requested_date == first_date and hour == 2:
                raise DownloadError("simulated tick failure")
            return tick_bytes(requested_date, hour)

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            first = run_tick_date(
                "EUR-USD", first_date, None, output_root=root, fetcher=fetch
            )
            second = run_tick_date(
                "EUR-USD", second_date, (0,), output_root=root, fetcher=fetch
            )
            self.assertEqual(len(first.hour_outcomes), 24)
            self.assertEqual(first.failed_hours[0].hour, 2)
            self.assertEqual(len(first.created_hours), 23)
            self.assertEqual(second.created_hours, (0,))
            self.assertTrue(any(url.endswith("/2026/9/1/2") for url in calls))
            self.assertTrue(
                tick_parquet_path(root, "EUR-USD", second_date, 0).exists()
            )

    def test_cli_modes_and_include_ticks_do_not_request_comb_endpoint(self) -> None:
        parser = build_argument_parser()
        only_ticks = parser.parse_args(
            [
                "download",
                "--instrument",
                "EUR-USD",
                "--date",
                "2026-09-01",
                "--only-ticks",
            ]
        )
        self.assertTrue(only_ticks.only_ticks)
        self.assertIsNone(only_ticks.aggregation)
        csv_arguments = parser.parse_args(
            [
                "download",
                "--instrument",
                "EUR-USD",
                "--date",
                "2026-09-01",
                "--aggregation",
                "1",
                "--csv",
                "-o",
                "nested/output",
            ]
        )
        self.assertTrue(csv_arguments.csv)
        self.assertEqual(csv_arguments.output, "nested/output")

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            calls: list[str] = []

            def fetch(url: str) -> bytes:
                calls.append(url)
                if "/ticks/" in url:
                    hour = int(url.rsplit("/", 1)[1])
                    return tick_bytes(REQUESTED_DATE, hour)
                if "/ASK/" in url:
                    return json.dumps(
                        {
                            "timestamp": 1_789_257_600_000,
                            "multiplier": 0.01,
                            "open": 1.1,
                            "high": 1.1,
                            "low": 1.1,
                            "close": 1.1,
                            "shift": 60_000,
                            "times": [0],
                            "opens": [0],
                            "highs": [0],
                            "lows": [0],
                            "closes": [0],
                            "volumes": [1.0],
                        }
                    ).encode()
                return json.dumps(
                    {
                        "timestamp": 1_789_257_600_000,
                        "multiplier": 0.01,
                        "open": 1.0,
                        "high": 1.0,
                        "low": 1.0,
                        "close": 1.0,
                        "shift": 60_000,
                        "times": [0],
                        "opens": [0],
                        "highs": [0],
                        "lows": [0],
                        "closes": [0],
                        "volumes": [1.0],
                    }
                ).encode()

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main(
                    [
                        "download",
                        "--instrument",
                        "EUR-USD",
                        "--date",
                        "2026-09-01",
                        "--aggregation",
                        "1",
                        "--include-ticks",
                        "--csv",
                    ],
                    output_root=root,
                    fetcher=fetch,
                )
            self.assertEqual(exit_code, 0)
            self.assertEqual(sum("/ticks/" in url for url in calls), 24)
            self.assertFalse(any("/COMB/" in url for url in calls))
            self.assertIn("created_tick_hours=24", stdout.getvalue())
            self.assertTrue(tick_csv_path(root, "EUR-USD", REQUESTED_DATE, 0).exists())
            self.assertTrue(
                (
                    root
                    / "instrument=EUR-USD"
                    / "tf=1m"
                    / "year=2026"
                    / "month=09"
                    / "day=13"
                    / "EUR-USD-2026-09-01-COMB.csv"
                ).exists()
            )

    def test_cli_only_ticks_single_hour_and_failure_summary_are_path_free(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            calls: list[str] = []

            def fetch(url: str) -> bytes:
                calls.append(url)
                return tick_bytes(REQUESTED_DATE, 1)

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main(
                    [
                        "download",
                        "--instrument",
                        "EUR-USD",
                        "--date",
                        "2026-09-01",
                        "--only-ticks",
                        "--hour",
                        "1",
                    ],
                    output_root=root,
                    fetcher=fetch,
                )
            self.assertEqual(exit_code, 0)
            self.assertEqual(len(calls), 1)
            self.assertIn("created_tick_hours=1", stdout.getvalue())

            def fail_fetch(_: str) -> bytes:
                raise DownloadError("tick network failure")

            stderr = io.StringIO()
            with redirect_stderr(stderr):
                exit_code = main(
                    [
                        "download",
                        "--instrument",
                        "EUR-USD",
                        "--date",
                        "2026-09-02",
                        "--only-ticks",
                        "--hour",
                        "1",
                    ],
                    output_root=root,
                    fetcher=fail_fetch,
                )
            self.assertEqual(exit_code, 1)
            self.assertIn("ticks hour 01: tick network failure", stderr.getvalue())
            self.assertNotIn(str(root), stderr.getvalue())

    def test_cli_rejects_invalid_tick_flag_combinations(self) -> None:
        common = ["download", "--instrument", "EUR-USD", "--date", "2026-09-01"]
        with self.assertRaises(SystemExit):
            main(common + ["--include-ticks"])
        with self.assertRaises(SystemExit):
            main(common + ["--aggregation", "1", "--only-ticks"])
        with self.assertRaises(SystemExit):
            main(common + ["--aggregation", "1", "--include-ticks", "--hour", "1"])


if __name__ == "__main__":
    unittest.main()
