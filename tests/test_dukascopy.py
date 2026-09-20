import json
import io
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow.dataset as ds
import pyarrow.parquet as pq

from dukascopy_market_data.candles import (
    DataValidationError,
    DownloadError,
    aggregate_candles,
    build_endpoint_url,
    decode_json_bytes,
    decode_payload,
    expand_date_range,
    parquet_output_paths,
    raw_json_path,
    run_download,
    run_combined_downloads,
    run_date_range,
    run_downloads,
    resolve_requested_dates,
    validate_aggregation,
    validate_aggregations,
    validate_download_side,
    validate_instrument,
    validate_side,
    write_parquet,
)
from dukascopy_market_data.cli import build_argument_parser, main
from dukascopy_market_data.instruments import INSTRUMENTS_URL, decode_instrument_codes


BASE_TIMESTAMP = 1_789_257_600_000  # 2026-09-13 00:00:00 UTC
REQUESTED_DATE = date(2026, 9, 13)


def sample_payload(timestamp: int = BASE_TIMESTAMP) -> dict:
    return {
        "timestamp": timestamp,
        "multiplier": 0.01,
        "open": 1.00,
        "high": 1.02,
        "low": 0.99,
        "close": 1.01,
        "shift": 60_000,
        "times": [0, 1, 2, 12, 1],
        "opens": [0, 1, -2, 3, -1],
        "highs": [0, 2, -1, 1, 0],
        "lows": [0, -1, 1, -2, 1],
        "closes": [0, 1, -2, 4, -1],
        "volumes": [1.5, 0.5, 1.25, 2.0, 0.25],
    }


def sample_bytes(timestamp: int = BASE_TIMESTAMP) -> bytes:
    return json.dumps(sample_payload(timestamp), separators=(",", ":")).encode("utf-8")


def ask_payload(timestamp: int = BASE_TIMESTAMP) -> dict:
    return {
        "timestamp": timestamp,
        "multiplier": 0.01,
        "open": 1.10,
        "high": 1.12,
        "low": 1.09,
        "close": 1.11,
        "shift": 60_000,
        "times": [0, 1, 2, 12, 1],
        "opens": [0, -1, 2, 1, -2],
        "highs": [0, 1, -1, 2, 0],
        "lows": [0, -1, 1, -2, 1],
        "closes": [0, 2, -1, 2, -2],
        "volumes": [2.0, 1.0, 1.5, 0.5, 0.75],
    }


def ask_bytes(timestamp: int = BASE_TIMESTAMP) -> bytes:
    return json.dumps(ask_payload(timestamp), separators=(",", ":")).encode("utf-8")


def empty_payload(timestamp: int = BASE_TIMESTAMP) -> dict:
    return {
        "timestamp": timestamp,
        "multiplier": 0.00001,
        "open": None,
        "high": None,
        "low": None,
        "close": None,
        "shift": 60_000,
        "times": [],
        "opens": [],
        "highs": [],
        "lows": [],
        "closes": [],
        "volumes": [],
    }


def empty_bytes(timestamp: int = BASE_TIMESTAMP) -> bytes:
    return json.dumps(empty_payload(timestamp), separators=(",", ":")).encode("utf-8")


def timestamp_for_day(value: date) -> int:
    return int(datetime.combine(value, datetime.min.time(), tzinfo=timezone.utc).timestamp() * 1000)


class DukascopyCandleTests(unittest.TestCase):
    def test_cli_validation_rejects_invalid_values(self) -> None:
        self.assertEqual(validate_instrument("eur-usd"), "eur-usd")
        self.assertEqual(validate_instrument("A.US-USD"), "A.US-USD")
        self.assertEqual(validate_instrument("0005.HK-HKD"), "0005.HK-HKD")
        self.assertEqual(validate_instrument("DUKplus-EUR"), "DUKplus-EUR")
        self.assertEqual(validate_side("bid"), "BID")
        self.assertEqual(validate_download_side("comb"), "COMB")
        self.assertEqual(validate_aggregation("15"), 15)
        self.assertEqual(validate_aggregations("1, 5, 15, 5"), (1, 5, 15))
        self.assertEqual(
            expand_date_range(date(2026, 9, 1), date(2026, 9, 3)),
            (date(2026, 9, 1), date(2026, 9, 2), date(2026, 9, 3)),
        )
        self.assertEqual(resolve_requested_dates(REQUESTED_DATE, None, None), (REQUESTED_DATE,))
        self.assertEqual(
            resolve_requested_dates(None, date(2026, 9, 1), date(2026, 9, 3)),
            (date(2026, 9, 1), date(2026, 9, 2), date(2026, 9, 3)),
        )

        with self.assertRaises(ValueError):
            validate_instrument("EURUSD")
        with self.assertRaises(ValueError):
            validate_side("MID")
        with self.assertRaises(ValueError):
            validate_download_side("MID")
        with self.assertRaises(ValueError):
            validate_aggregation(0)
        with self.assertRaises(ValueError):
            validate_aggregations("1,,5")
        with self.assertRaises(ValueError):
            expand_date_range(date(2026, 9, 3), date(2026, 9, 1))
        with self.assertRaises(ValueError):
            resolve_requested_dates(REQUESTED_DATE, date(2026, 9, 1), date(2026, 9, 3))

        parser = build_argument_parser()
        default_combined = parser.parse_args(
            [
                "download",
                "--instrument",
                "EUR-USD",
                "--date",
                "2026-09-13",
                "--aggregation",
                "15",
            ]
        )
        self.assertEqual(default_combined.side, "COMB")
        explicit_combined = parser.parse_args(
            [
                "download",
                "--instrument",
                "EUR-USD",
                "--side",
                "COMB",
                "--date",
                "2026-09-13",
                "--aggregation",
                "15",
            ]
        )
        self.assertEqual(explicit_combined.side, "COMB")
        with self.assertRaises(ValueError):
            resolve_requested_dates(None, date(2026, 9, 1), None)

        parser = build_argument_parser()
        with self.assertRaises(SystemExit) as raised:
            parser.parse_args(
                [
                    "download",
                    "--instrument",
                    "EURUSD",
                    "--side",
                    "BID",
                    "--date",
                    "2026-09-13",
                    "--aggregation",
                    "15",
                ]
            )
        self.assertEqual(raised.exception.code, 2)

        range_arguments = parser.parse_args(
            [
                "download",
                "--instrument",
                "EUR-USD",
                "--side",
                "BID",
                "--start-date",
                "2026-09-01",
                "--end-date",
                "2026-09-03",
                "--aggregation",
                "1,5,15",
            ]
        )
        self.assertEqual(
            resolve_requested_dates(
                range_arguments.requested_date,
                range_arguments.start_date,
                range_arguments.end_date,
            ),
            (date(2026, 9, 1), date(2026, 9, 2), date(2026, 9, 3)),
        )

    def test_endpoint_uses_unpadded_path_components(self) -> None:
        self.assertEqual(
            build_endpoint_url("eur-usd", "bid", REQUESTED_DATE),
            "https://jetta.dukascopy.com/v1/candles/minute/eur-usd/BID/2026/9/13",
        )
        self.assertEqual(
            build_endpoint_url("DUKplus-EUR", "BID", REQUESTED_DATE),
            "https://jetta.dukascopy.com/v1/candles/minute/DUKplus-EUR/BID/2026/9/13",
        )
        self.assertEqual(
            raw_json_path(Path("output"), "EUR-USD", REQUESTED_DATE, "BID").as_posix(),
            "output/artifacts/instrument=EUR-USD/json/minute/year=2026/month=09/day=13/EUR-USD-2026-09-13-BID.json",
        )

    def test_legacy_candles_files_are_not_reused_by_artifacts_layout(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            output_root = Path(temporary_directory)
            legacy_raw = (
                output_root
                / "candles"
                / "minute"
                / "json"
                / "EUR-USD-2026-09-13-BID.json"
            )
            legacy_raw.parent.mkdir(parents=True)
            legacy_raw.write_bytes(b"legacy raw")
            legacy_parquet = (
                output_root
                / "candles"
                / "15m"
                / "year=2026"
                / "month=09"
                / "day=13"
                / "EUR-USD-2026-09-13-BID.parquet"
            )
            legacy_parquet.parent.mkdir(parents=True)
            legacy_parquet.write_bytes(b"legacy parquet")

            calls: list[str] = []
            json_path, parquet_paths, minute_count = run_download(
                "EUR-USD",
                "BID",
                REQUESTED_DATE,
                15,
                output_root=output_root,
                fetcher=lambda url: calls.append(url) or sample_bytes(),
            )

            self.assertEqual(len(calls), 1)
            self.assertTrue(json_path.is_relative_to(output_root / "artifacts"))
            self.assertTrue(parquet_paths[0].is_relative_to(output_root / "artifacts"))
            self.assertEqual(minute_count, 5)
            self.assertEqual(legacy_raw.read_bytes(), b"legacy raw")
            self.assertEqual(legacy_parquet.read_bytes(), b"legacy parquet")

    def test_instrument_codes_are_sorted_deduplicated_and_exact(self) -> None:
        payload = {
            "groups": [],
            "instruments": [
                {"code": "EUR-USD"},
                {"code": "DUKplus-EUR"},
                {"code": "0005.HK-HKD"},
                {"code": "EUR-USD"},
            ],
        }
        raw_bytes = json.dumps(payload).encode("utf-8")
        self.assertEqual(
            decode_instrument_codes(raw_bytes),
            ("0005.HK-HKD", "DUKplus-EUR", "EUR-USD"),
        )

    def test_instrument_code_payload_validation(self) -> None:
        invalid_payloads = [
            b"not json",
            json.dumps({}).encode("utf-8"),
            json.dumps({"instruments": {}}).encode("utf-8"),
            json.dumps({"instruments": [{"name": "EUR/USD"}]}).encode("utf-8"),
            json.dumps({"instruments": [{"code": ""}]}).encode("utf-8"),
            json.dumps({"instruments": [{"code": "EUR/USD"}]}).encode("utf-8"),
        ]
        for raw_bytes in invalid_payloads:
            with self.subTest(raw_bytes=raw_bytes), self.assertRaises(DataValidationError):
                decode_instrument_codes(raw_bytes)

    def test_no_subcommand_prints_help(self) -> None:
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            exit_code = main([])
        self.assertEqual(exit_code, 0)
        self.assertIn("download", stdout.getvalue())
        self.assertIn("instruments", stdout.getvalue())

    def test_instruments_command_prints_only_codes(self) -> None:
        payload = json.dumps(
            {
                "instruments": [
                    {"code": "EUR-USD", "name": "Euro vs US Dollar"},
                    {"code": "0005.HK-HKD", "name": "HSBC"},
                    {"code": "EUR-USD", "name": "Duplicate"},
                ]
            }
        ).encode("utf-8")
        requested_urls: list[str] = []

        def fetch_instruments(url: str) -> bytes:
            requested_urls.append(url)
            return payload

        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            exit_code = main(["instruments"], fetcher=fetch_instruments)

        self.assertEqual(exit_code, 0)
        self.assertEqual(requested_urls, [INSTRUMENTS_URL])
        self.assertEqual(stdout.getvalue().splitlines(), ["0005.HK-HKD", "EUR-USD"])
        self.assertEqual(stderr.getvalue(), "")

    def test_instruments_command_reports_fetch_failure(self) -> None:
        stdout = io.StringIO()
        stderr = io.StringIO()

        def fail_fetch(_: str) -> bytes:
            raise DownloadError("network failure")

        with redirect_stdout(stdout), redirect_stderr(stderr):
            exit_code = main(["instruments"], fetcher=fail_fetch)

        self.assertEqual(exit_code, 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn("ERROR: network failure", stderr.getvalue())

    def test_decode_accumulates_times_prices_and_volume(self) -> None:
        candles = decode_json_bytes(sample_bytes())

        self.assertEqual(len(candles), 5)
        self.assertEqual(
            [candle.timestamp_ms for candle in candles],
            [
                BASE_TIMESTAMP,
                BASE_TIMESTAMP + 60_000,
                BASE_TIMESTAMP + 3 * 60_000,
                BASE_TIMESTAMP + 15 * 60_000,
                BASE_TIMESTAMP + 16 * 60_000,
            ],
        )
        self.assertEqual(candles[0].open, Decimal("1.00"))
        self.assertEqual(candles[1].open, Decimal("1.01"))
        self.assertEqual(candles[2].close, Decimal("1.00"))
        self.assertEqual(candles[0].volume, 1_500_000)
        self.assertEqual(candles[4].volume, 250_000)

    def test_decode_accepts_valid_empty_endpoint_payload(self) -> None:
        self.assertEqual(decode_json_bytes(empty_bytes()), [])

    def test_decode_rejects_bad_first_delta(self) -> None:
        payload = sample_payload()
        payload["opens"][0] = 1
        with self.assertRaises(DataValidationError):
            decode_payload(payload)

    def test_decode_rejects_mismatched_arrays_and_duplicate_timestamps(self) -> None:
        payload = sample_payload()
        payload["highs"] = payload["highs"][:-1]
        with self.assertRaises(DataValidationError):
            decode_payload(payload)

        payload = sample_payload()
        payload["times"] = [0, 1, 0, 12, 1]
        with self.assertRaises(DataValidationError):
            decode_payload(payload)

    def test_aggregate_aligns_calendar_buckets_and_keeps_sparse_data(self) -> None:
        minute_candles = decode_json_bytes(sample_bytes())
        aggregated = aggregate_candles(minute_candles, 15)

        self.assertEqual(
            [candle.timestamp_ms for candle in aggregated],
            [BASE_TIMESTAMP, BASE_TIMESTAMP + 15 * 60_000],
        )
        self.assertEqual(aggregated[0].open, Decimal("1.00"))
        self.assertEqual(aggregated[0].high, Decimal("1.04"))
        self.assertEqual(aggregated[0].low, Decimal("0.98"))
        self.assertEqual(aggregated[0].close, Decimal("1.00"))
        self.assertEqual(aggregated[0].volume, 3_250_000)
        self.assertEqual(aggregated[1].open, Decimal("1.02"))
        self.assertEqual(aggregated[1].close, Decimal("1.03"))
        self.assertEqual(aggregated[1].volume, 2_250_000)

    def test_write_parquet_has_expected_schema_and_hive_partitions(self) -> None:
        aggregated = aggregate_candles(decode_json_bytes(sample_bytes()), 15)
        with tempfile.TemporaryDirectory() as temporary_directory:
            output_root = Path(temporary_directory)
            paths = write_parquet(
                aggregated,
                output_root,
                "EUR-USD",
                REQUESTED_DATE,
                "BID",
                15,
            )

            self.assertEqual(len(paths), 1)
            output_path = paths[0]
            self.assertTrue(output_path.exists())
            self.assertIn("instrument=EUR-USD", str(output_path))
            self.assertIn("tf=15m", str(output_path))
            self.assertIn("year=2026", str(output_path))
            self.assertIn("month=09", str(output_path))
            self.assertIn("day=13", str(output_path))

            table = pq.read_table(output_path)
            self.assertEqual(
                [field.name for field in table.schema],
                ["timestamp", "open", "high", "low", "close", "volume"],
            )
            self.assertEqual(str(table.schema.field("timestamp").type), "timestamp[ms, tz=UTC]")
            self.assertEqual(str(table.schema.field("volume").type), "int64")
            self.assertEqual(table.num_rows, 2)

            dataset = ds.dataset(
                output_root / "artifacts" / "instrument=EUR-USD" / "tf=15m",
                format="parquet",
                partitioning="hive",
            )
            self.assertEqual(dataset.to_table().num_rows, 2)
            self.assertEqual(
                set(dataset.schema.names),
                {"timestamp", "open", "high", "low", "close", "volume", "year", "month", "day"},
            )

    def test_combined_download_fetches_both_sides_and_writes_combined_schema(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            output_root = Path(temporary_directory)
            calls: list[str] = []

            def fetch(url: str) -> bytes:
                calls.append(url)
                self.assertNotIn("/COMB/", url)
                if "/BID/" in url:
                    return sample_bytes()
                if "/ASK/" in url:
                    return ask_bytes()
                self.fail(f"unexpected URL: {url}")

            first = run_combined_downloads(
                "EUR-USD",
                REQUESTED_DATE,
                15,
                output_root=output_root,
                fetcher=fetch,
            )

            self.assertEqual(
                calls,
                [
                    "https://jetta.dukascopy.com/v1/candles/minute/EUR-USD/BID/2026/9/13",
                    "https://jetta.dukascopy.com/v1/candles/minute/EUR-USD/ASK/2026/9/13",
                ],
            )
            self.assertEqual(first.minute_count, 5)
            self.assertEqual(first.raw_downloaded_sides, ("BID", "ASK"))
            self.assertEqual(first.raw_cached_sides, ())
            self.assertEqual(first.created_aggregations, (15,))
            self.assertEqual(len(first.parquet_paths), 1)
            output_path = first.parquet_paths[0]
            self.assertEqual(output_path.name, "EUR-USD-2026-09-13-COMB.parquet")

            table = pq.read_table(output_path)
            self.assertEqual(
                [field.name for field in table.schema],
                [
                    "timestamp",
                    "bidOpen",
                    "bidHigh",
                    "bidLow",
                    "bidClose",
                    "askOpen",
                    "askHigh",
                    "askLow",
                    "askClose",
                    "bidVolume",
                    "askVolume",
                ],
            )
            self.assertEqual(str(table.schema.field("timestamp").type), "timestamp[ms, tz=UTC]")
            for field_name in (
                "bidOpen",
                "bidHigh",
                "bidLow",
                "bidClose",
                "askOpen",
                "askHigh",
                "askLow",
                "askClose",
            ):
                self.assertEqual(str(table.schema.field(field_name).type), "double")
            self.assertEqual(str(table.schema.field("bidVolume").type), "int64")
            self.assertEqual(str(table.schema.field("askVolume").type), "int64")
            self.assertEqual(table.column("bidOpen")[0].as_py(), 1.0)
            self.assertEqual(table.column("askOpen")[0].as_py(), 1.1)
            self.assertEqual(table.column("bidVolume")[0].as_py(), 3_250_000)
            self.assertEqual(table.column("askVolume")[0].as_py(), 4_500_000)

            bid_raw_before = raw_json_path(output_root, "EUR-USD", REQUESTED_DATE, "BID").read_bytes()
            ask_raw_before = raw_json_path(output_root, "EUR-USD", REQUESTED_DATE, "ASK").read_bytes()

            def fail_on_cached_download(_: str) -> bytes:
                self.fail("combined cached JSON must be reused")

            second = run_combined_downloads(
                "EUR-USD",
                REQUESTED_DATE,
                5,
                output_root=output_root,
                fetcher=fail_on_cached_download,
            )
            self.assertEqual(second.raw_downloaded_sides, ())
            self.assertEqual(second.raw_cached_sides, ("BID", "ASK"))
            self.assertEqual(second.created_aggregations, (5,))
            self.assertEqual(
                raw_json_path(output_root, "EUR-USD", REQUESTED_DATE, "BID").read_bytes(),
                bid_raw_before,
            )
            self.assertEqual(
                raw_json_path(output_root, "EUR-USD", REQUESTED_DATE, "ASK").read_bytes(),
                ask_raw_before,
            )

            third = run_combined_downloads(
                "EUR-USD",
                REQUESTED_DATE,
                15,
                output_root=output_root,
                fetcher=fail_on_cached_download,
            )
            self.assertEqual(third.created_aggregations, ())
            self.assertEqual(third.skipped_aggregations, (15,))
            self.assertEqual(third.parquet_paths, ())

    def test_main_defaults_to_combined_mode(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            output_root = Path(temporary_directory)
            calls: list[str] = []

            def fetch(url: str) -> bytes:
                calls.append(url)
                return ask_bytes() if "/ASK/" in url else sample_bytes()

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main(
                    [
                        "download",
                        "--instrument",
                        "EUR-USD",
                        "--date",
                        "2026-09-13",
                        "--aggregation",
                        "15",
                    ],
                    output_root=output_root,
                    fetcher=fetch,
                )

            self.assertEqual(exit_code, 0)
            self.assertEqual(len(calls), 2)
            self.assertTrue(
                (
                    output_root
                    / "artifacts"
                    / "instrument=EUR-USD"
                    / "tf=15m"
                    / "year=2026"
                    / "month=09"
                    / "day=13"
                    / "EUR-USD-2026-09-13-COMB.parquet"
                ).exists()
            )

    def test_combined_download_rejects_count_and_timestamp_mismatches(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            output_root = Path(temporary_directory)
            mismatched = ask_payload()
            for field in ("times", "opens", "highs", "lows", "closes", "volumes"):
                mismatched[field] = mismatched[field][:-1]

            def fetch_mismatched_count(url: str) -> bytes:
                return sample_bytes() if "/BID/" in url else json.dumps(mismatched).encode("utf-8")

            with self.assertRaises(DataValidationError):
                run_combined_downloads(
                    "EUR-USD",
                    REQUESTED_DATE,
                    15,
                    output_root=output_root,
                    fetcher=fetch_mismatched_count,
                )
            self.assertFalse(raw_json_path(output_root, "EUR-USD", REQUESTED_DATE, "BID").exists())
            self.assertFalse(raw_json_path(output_root, "EUR-USD", REQUESTED_DATE, "ASK").exists())

        with tempfile.TemporaryDirectory() as temporary_directory:
            output_root = Path(temporary_directory)

            def fetch_mismatched_timestamp(url: str) -> bytes:
                return sample_bytes() if "/BID/" in url else ask_bytes(BASE_TIMESTAMP + 60_000)

            with self.assertRaises(DataValidationError):
                run_combined_downloads(
                    "EUR-USD",
                    REQUESTED_DATE,
                    15,
                    output_root=output_root,
                    fetcher=fetch_mismatched_timestamp,
                )

    def test_combined_empty_day_caches_both_sides_and_requires_both_empty(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            output_root = Path(temporary_directory)
            result = run_combined_downloads(
                "EUR-USD",
                REQUESTED_DATE,
                "1,5,15",
                output_root=output_root,
                fetcher=lambda _: empty_bytes(),
            )
            self.assertEqual(result.minute_count, 0)
            self.assertEqual(result.parquet_paths, ())
            self.assertTrue(raw_json_path(output_root, "EUR-USD", REQUESTED_DATE, "BID").exists())
            self.assertTrue(raw_json_path(output_root, "EUR-USD", REQUESTED_DATE, "ASK").exists())

        with tempfile.TemporaryDirectory() as temporary_directory:
            output_root = Path(temporary_directory)

            def fetch_one_empty(url: str) -> bytes:
                return empty_bytes() if "/BID/" in url else ask_bytes()

            with self.assertRaises(DataValidationError):
                run_combined_downloads(
                    "EUR-USD",
                    REQUESTED_DATE,
                    15,
                    output_root=output_root,
                    fetcher=fetch_one_empty,
                )

    def test_combined_date_range_keeps_daily_outputs_and_partitions(self) -> None:
        first_date = date(2026, 9, 13)
        second_date = date(2026, 9, 14)

        def fetch_by_date(url: str) -> bytes:
            timestamp = timestamp_for_day(second_date) if url.endswith("/2026/9/14") else timestamp_for_day(first_date)
            return ask_bytes(timestamp) if "/ASK/" in url else sample_bytes(timestamp)

        with tempfile.TemporaryDirectory() as temporary_directory:
            output_root = Path(temporary_directory)
            outcomes = run_date_range(
                "EUR-USD",
                "COMB",
                (first_date, second_date),
                15,
                output_root=output_root,
                fetcher=fetch_by_date,
            )

            self.assertTrue(all(outcome.error is None for outcome in outcomes))
            for requested_date in (first_date, second_date):
                parquet_path = (
                    output_root
                    / "artifacts"
                    / "instrument=EUR-USD"
                    / "tf=15m"
                    / "year=2026"
                    / "month=09"
                    / f"day={requested_date.day:02d}"
                    / f"EUR-USD-{requested_date.isoformat()}-COMB.parquet"
                )
                self.assertTrue(parquet_path.exists())
                self.assertTrue(raw_json_path(output_root, "EUR-USD", requested_date, "BID").exists())
                self.assertTrue(raw_json_path(output_root, "EUR-USD", requested_date, "ASK").exists())

    def test_existing_raw_json_is_reused_for_new_aggregation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            output_root = Path(temporary_directory)
            raw_path = raw_json_path(output_root, "EUR-USD", REQUESTED_DATE, "BID")
            raw_path.parent.mkdir(parents=True)
            raw_path.write_bytes(sample_bytes())

            def fail_fetch(_: str) -> bytes:
                self.fail("an existing raw JSON must be reused without downloading")

            json_path, parquet_paths, minute_count = run_download(
                "EUR-USD",
                "BID",
                REQUESTED_DATE,
                15,
                output_root=output_root,
                fetcher=fail_fetch,
            )

            self.assertEqual(json_path, raw_path)
            self.assertEqual(len(parquet_paths), 1)
            self.assertEqual(minute_count, 5)
            self.assertEqual(raw_path.read_bytes(), sample_bytes())

    def test_second_aggregation_reuses_first_download(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            output_root = Path(temporary_directory)
            fetch_calls: list[str] = []

            def fetch_once(url: str) -> bytes:
                fetch_calls.append(url)
                return sample_bytes()

            first_json, first_parquet, _ = run_download(
                "EUR-USD",
                "BID",
                REQUESTED_DATE,
                15,
                output_root=output_root,
                fetcher=fetch_once,
            )
            raw_before_second_run = first_json.read_bytes()

            def fail_on_second_download(_: str) -> bytes:
                self.fail("the second aggregation must use the cached raw JSON")

            second_json, second_parquet, _ = run_download(
                "EUR-USD",
                "BID",
                REQUESTED_DATE,
                5,
                output_root=output_root,
                fetcher=fail_on_second_download,
            )

            self.assertEqual(fetch_calls, ["https://jetta.dukascopy.com/v1/candles/minute/EUR-USD/BID/2026/9/13"])
            self.assertEqual(first_json, second_json)
            self.assertEqual(raw_before_second_run, second_json.read_bytes())
            self.assertTrue(first_parquet[0].exists())
            self.assertTrue(second_parquet[0].exists())
            self.assertNotEqual(first_parquet[0], second_parquet[0])

    def test_empty_day_is_cached_without_parquet_output(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            output_root = Path(temporary_directory)
            calls: list[str] = []

            def fetch_empty(url: str) -> bytes:
                calls.append(url)
                return empty_bytes(timestamp_for_day(REQUESTED_DATE))

            first = run_downloads(
                "EUR-USD",
                "BID",
                REQUESTED_DATE,
                "1,5,15",
                output_root=output_root,
                fetcher=fetch_empty,
            )
            raw_path = raw_json_path(output_root, "EUR-USD", REQUESTED_DATE, "BID")
            self.assertEqual(first.minute_count, 0)
            self.assertEqual(first.parquet_paths, ())
            self.assertTrue(first.raw_was_downloaded)
            self.assertTrue(raw_path.exists())

            def fail_on_reused_download(_: str) -> bytes:
                self.fail("a cached empty-day response must be reused")

            second = run_downloads(
                "EUR-USD",
                "BID",
                REQUESTED_DATE,
                15,
                output_root=output_root,
                fetcher=fail_on_reused_download,
            )
            self.assertEqual(calls, ["https://jetta.dukascopy.com/v1/candles/minute/EUR-USD/BID/2026/9/13"])
            self.assertEqual(second.minute_count, 0)
            self.assertFalse(second.raw_was_downloaded)
            self.assertEqual(second.parquet_paths, ())

    def test_date_range_continues_after_failure_and_keeps_daily_outputs(self) -> None:
        first_date = date(2026, 9, 11)
        empty_date = date(2026, 9, 12)
        failed_date = date(2026, 9, 13)

        def fetch_by_date(url: str) -> bytes:
            if url.endswith("/2026/9/12"):
                return empty_bytes(timestamp_for_day(empty_date))
            if url.endswith("/2026/9/13"):
                raise DownloadError("simulated network failure")
            return sample_bytes(timestamp_for_day(first_date))

        with tempfile.TemporaryDirectory() as temporary_directory:
            output_root = Path(temporary_directory)
            outcomes = run_date_range(
                "EUR-USD",
                "BID",
                (first_date, empty_date, failed_date),
                15,
                output_root=output_root,
                fetcher=fetch_by_date,
            )

            self.assertEqual(len(outcomes), 3)
            self.assertIsNotNone(outcomes[0].result)
            self.assertEqual(outcomes[0].result.minute_count, 5)
            self.assertIsNotNone(outcomes[1].result)
            self.assertEqual(outcomes[1].result.minute_count, 0)
            self.assertIsNone(outcomes[2].result)
            self.assertEqual(outcomes[2].error, "simulated network failure")
            self.assertTrue(
                (
                    output_root
                    / "artifacts"
                    / "instrument=EUR-USD"
                    / "tf=15m"
                    / "year=2026"
                    / "month=09"
                    / "day=11"
                ).exists()
            )
            self.assertTrue(raw_json_path(output_root, "EUR-USD", first_date, "BID").exists())
            self.assertTrue(raw_json_path(output_root, "EUR-USD", empty_date, "BID").exists())
            self.assertFalse(raw_json_path(output_root, "EUR-USD", failed_date, "BID").exists())

    def test_main_reports_empty_and_failed_dates_and_returns_nonzero(self) -> None:
        first_date = date(2026, 9, 11)
        empty_date = date(2026, 9, 12)
        failed_date = date(2026, 9, 13)

        def fetch_by_date(url: str) -> bytes:
            if url.endswith("/2026/9/12"):
                return empty_bytes(timestamp_for_day(empty_date))
            if url.endswith("/2026/9/13"):
                raise DownloadError("simulated network failure")
            return sample_bytes(timestamp_for_day(first_date))

        with tempfile.TemporaryDirectory() as temporary_directory:
            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                exit_code = main(
                    [
                        "download",
                        "--instrument",
                        "EUR-USD",
                        "--side",
                        "BID",
                        "--start-date",
                        "2026-09-11",
                        "--end-date",
                        "2026-09-13",
                        "--aggregation",
                        "15",
                    ],
                    output_root=Path(temporary_directory),
                    fetcher=fetch_by_date,
                )

            self.assertEqual(exit_code, 1)
            self.assertIn("[2026-09-12] Downloaded; no candles (empty date)", stdout.getvalue())
            self.assertIn("Empty days: 2026-09-12", stdout.getvalue())
            self.assertIn("Failed days: 2026-09-13", stdout.getvalue())
            self.assertIn("Skipped days: none", stdout.getvalue())
            self.assertIn("Summary: processed=3 successful=1 empty=1 failed=1", stdout.getvalue())
            self.assertIn("[2026-09-13] ERROR: simulated network failure", stderr.getvalue())
            self.assertNotIn("Raw JSON:", stdout.getvalue())
            self.assertNotIn("Parquet:", stdout.getvalue())

    def test_main_reports_skipped_days_without_filesystem_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            output_root = Path(temporary_directory)
            arguments = [
                "download",
                "--instrument",
                "EUR-USD",
                "--side",
                "BID",
                "--date",
                "2026-09-13",
                "--aggregation",
                "15",
            ]
            main(arguments, output_root=output_root, fetcher=lambda _: sample_bytes())

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main(
                    arguments,
                    output_root=output_root,
                    fetcher=lambda _: self.fail("cached JSON should be reused"),
                )

            self.assertEqual(exit_code, 0)
            self.assertIn("Skipped days: 2026-09-13", stdout.getvalue())
            self.assertIn("Summary: processed=1 successful=0 empty=0 failed=0 skipped=1", stdout.getvalue())
            self.assertNotIn("Raw JSON:", stdout.getvalue())
            self.assertNotIn("Parquet:", stdout.getvalue())

    def test_batch_aggregations_create_missing_and_skip_existing(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            output_root = Path(temporary_directory)
            first = run_downloads(
                "EUR-USD",
                "BID",
                REQUESTED_DATE,
                "15, 5",
                output_root=output_root,
                fetcher=lambda _: sample_bytes(),
            )
            self.assertEqual(first.created_aggregations, (15, 5))
            self.assertEqual(first.skipped_aggregations, ())
            self.assertTrue(first.raw_was_downloaded)

            def fail_on_cached_download(_: str) -> bytes:
                self.fail("all batch aggregations must use the cached raw JSON")

            second = run_downloads(
                "EUR-USD",
                "BID",
                REQUESTED_DATE,
                "1,5,15,5",
                output_root=output_root,
                fetcher=fail_on_cached_download,
            )
            self.assertEqual(second.created_aggregations, (1,))
            self.assertEqual(second.skipped_aggregations, (5, 15))
            self.assertFalse(second.raw_was_downloaded)
            self.assertEqual(len(second.parquet_paths), 1)

    def test_invalid_cached_json_fails_without_downloading(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            output_root = Path(temporary_directory)
            raw_path = raw_json_path(output_root, "EUR-USD", REQUESTED_DATE, "BID")
            raw_path.parent.mkdir(parents=True)
            raw_path.write_bytes(b"not valid json")

            def fail_fetch(_: str) -> bytes:
                self.fail("invalid cached JSON must not trigger a replacement download")

            with self.assertRaises(DataValidationError):
                run_download(
                    "EUR-USD",
                    "BID",
                    REQUESTED_DATE,
                    15,
                    output_root=output_root,
                    fetcher=fail_fetch,
                )
            self.assertEqual(raw_path.read_bytes(), b"not valid json")

        with tempfile.TemporaryDirectory() as temporary_directory:
            output_root = Path(temporary_directory)
            aggregated = aggregate_candles(decode_json_bytes(sample_bytes()), 15)
            output_path = next(
                iter(
                    parquet_output_paths(
                        aggregated,
                        output_root,
                        "EUR-USD",
                        REQUESTED_DATE,
                        "BID",
                        15,
                    )
                )
            )
            output_path.parent.mkdir(parents=True)
            output_path.write_bytes(b"existing parquet")

            json_path, parquet_paths, minute_count = run_download(
                "EUR-USD",
                "BID",
                REQUESTED_DATE,
                15,
                output_root=output_root,
                fetcher=lambda _: sample_bytes(),
            )
            self.assertEqual(parquet_paths, [])
            self.assertEqual(minute_count, 5)
            self.assertTrue(json_path.exists())
            self.assertEqual(output_path.read_bytes(), b"existing parquet")


if __name__ == "__main__":
    unittest.main()
