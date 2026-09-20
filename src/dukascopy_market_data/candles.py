"""Download and decode compressed Dukascopy minute candles.

The Dukascopy endpoint stores a sparse series as a base timestamp, cumulative
time deltas, and cumulative price deltas.  This module expands that payload,
aggregates the resulting candles on UTC calendar boundaries, and writes a
Hive-partitioned Parquet or CSV datasets.
"""

from __future__ import annotations

import csv
import json
import os
import re
import tempfile
import time
import urllib.error
import urllib.request
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

import pyarrow as pa
import pyarrow.parquet as pq


BASE_URL = "https://jetta.dukascopy.com/v1/candles/minute"
HTTP_TIMEOUT_SECONDS = 30.0
MAX_DOWNLOAD_ATTEMPTS = 3
RETRYABLE_HTTP_CODES = frozenset({408, 429, 500, 502, 503, 504})
RETRY_DELAYS_SECONDS = (1.0, 2.0)
MILLISECONDS_PER_MINUTE = 60_000
MILLION = Decimal("1000000")
INT64_MAX = 2**63 - 1
OUTPUT_FORMATS = frozenset({"parquet", "csv"})
INSTRUMENT_PATTERN = re.compile(
    r"^[A-Za-z0-9]+(?:\.[A-Za-z0-9]+)*-[A-Za-z0-9]+(?:\.[A-Za-z0-9]+)*$"
)
DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class DataValidationError(ValueError):
    """Raised when the endpoint response does not satisfy its data contract."""


class DownloadError(RuntimeError):
    """Raised when the endpoint cannot be downloaded successfully."""


@dataclass(frozen=True)
class MinuteCandle:
    """One expanded one-minute candle."""

    timestamp_ms: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int


@dataclass(frozen=True)
class AggregatedCandle:
    """One calendar-aligned aggregated candle."""

    timestamp_ms: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int


@dataclass(frozen=True)
class CombinedAggregatedCandle:
    """One aggregated candle containing aligned BID and ASK values."""

    timestamp_ms: int
    bid_open: Decimal
    bid_high: Decimal
    bid_low: Decimal
    bid_close: Decimal
    ask_open: Decimal
    ask_high: Decimal
    ask_low: Decimal
    ask_close: Decimal
    bid_volume: int
    ask_volume: int


@dataclass(frozen=True)
class DownloadBatchResult:
    """Result of processing one or more requested aggregations."""

    json_path: Path
    output_paths: tuple[Path, ...]
    minute_count: int
    raw_was_downloaded: bool
    created_aggregations: tuple[int, ...]
    skipped_aggregations: tuple[int, ...]
    output_format: str = "parquet"

    @property
    def parquet_paths(self) -> tuple[Path, ...]:
        """Backward-compatible Parquet path view."""

        return self.output_paths if self.output_format == "parquet" else ()

    @property
    def csv_paths(self) -> tuple[Path, ...]:
        """Return generated CSV paths for CSV batches."""

        return self.output_paths if self.output_format == "csv" else ()

    @property
    def source_message(self) -> str:
        return "Downloaded" if self.raw_was_downloaded else "Reused cached JSON"


@dataclass(frozen=True)
class CombinedDownloadBatchResult:
    """Result of processing aligned BID and ASK data."""

    json_paths: tuple[Path, Path]
    output_paths: tuple[Path, ...]
    minute_count: int
    raw_downloaded_sides: tuple[str, ...]
    raw_cached_sides: tuple[str, ...]
    created_aggregations: tuple[int, ...]
    skipped_aggregations: tuple[int, ...]
    output_format: str = "parquet"

    @property
    def parquet_paths(self) -> tuple[Path, ...]:
        """Backward-compatible Parquet path view."""

        return self.output_paths if self.output_format == "parquet" else ()

    @property
    def csv_paths(self) -> tuple[Path, ...]:
        """Return generated CSV paths for CSV batches."""

        return self.output_paths if self.output_format == "csv" else ()

    @property
    def raw_was_downloaded(self) -> bool:
        return bool(self.raw_downloaded_sides)

    @property
    def source_message(self) -> str:
        downloaded = self.raw_downloaded_sides
        cached = self.raw_cached_sides
        if downloaded and cached:
            return (
                f"Downloaded {', '.join(downloaded)} JSON and reused cached "
                f"{', '.join(cached)} JSON"
            )
        if downloaded:
            return f"Downloaded {' and '.join(downloaded)} JSON"
        return f"Reused cached {' and '.join(cached)} JSON"


@dataclass(frozen=True)
class DateRunOutcome:
    """Outcome for one requested endpoint date."""

    requested_date: date
    result: DownloadBatchResult | CombinedDownloadBatchResult | None
    error: str | None


def validate_instrument(value: str) -> str:
    """Validate and preserve an exact instrument code accepted by the endpoint."""

    if not isinstance(value, str):
        raise ValueError(f"instrument must be a string; received {value!r}")
    instrument = value.strip()
    if not INSTRUMENT_PATTERN.fullmatch(instrument):
        raise ValueError(
            "instrument must contain two non-empty alphanumeric/dot components separated "
            f"by one hyphen, for example EUR-USD; received {value!r}"
        )
    return instrument


def validate_side(value: str) -> str:
    """Normalize and validate the quote side."""

    side = value.strip().upper()
    if side not in {"BID", "ASK"}:
        raise ValueError(f"side must be BID or ASK; received {value!r}")
    return side


def validate_download_side(value: str) -> str:
    """Normalize a requested output mode, including the combined mode."""

    side = value.strip().upper()
    if side not in {"BID", "ASK", "COMB"}:
        raise ValueError(f"side must be BID, ASK, or COMB; received {value!r}")
    return side


def validate_output_format(value: str) -> str:
    """Validate the derived-data serialization format."""

    if not isinstance(value, str):
        raise ValueError(f"output format must be parquet or csv; received {value!r}")
    output_format = value.strip().lower()
    if output_format not in OUTPUT_FORMATS:
        raise ValueError(
            f"output format must be parquet or csv; received {value!r}"
        )
    return output_format


def format_timestamp_utc(timestamp_ms: int) -> str:
    """Serialize an epoch-millisecond timestamp as ISO-8601 UTC."""

    seconds, milliseconds = divmod(timestamp_ms, 1000)
    timestamp = datetime.fromtimestamp(seconds, tz=timezone.utc).replace(
        microsecond=milliseconds * 1000
    )
    return timestamp.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_date(value: str) -> date:
    """Parse an ISO calendar date without accepting ambiguous formats."""

    if not DATE_PATTERN.fullmatch(value):
        raise ValueError(f"date must use YYYY-MM-DD format; received {value!r}")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"date is not a valid calendar date: {value!r}") from exc


def expand_date_range(start_date: date, end_date: date) -> tuple[date, ...]:
    """Return every calendar date from start_date through end_date."""

    if end_date < start_date:
        raise ValueError(
            f"end date {end_date.isoformat()} cannot be before start date {start_date.isoformat()}"
        )
    return tuple(
        start_date + timedelta(days=offset)
        for offset in range((end_date - start_date).days + 1)
    )


def resolve_requested_dates(
    requested_date: date | None,
    start_date: date | None,
    end_date: date | None,
) -> tuple[date, ...]:
    """Resolve mutually exclusive single-date or inclusive range arguments."""

    if requested_date is not None:
        if start_date is not None or end_date is not None:
            raise ValueError("--date cannot be combined with --start-date or --end-date")
        return (requested_date,)

    if start_date is None and end_date is None:
        raise ValueError("provide --date or both --start-date and --end-date")
    if start_date is None or end_date is None:
        raise ValueError("--start-date and --end-date must be provided together")
    return expand_date_range(start_date, end_date)


def validate_aggregation(value: int | str) -> int:
    """Return a positive aggregation size in minutes."""

    try:
        aggregation = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"aggregation must be a positive integer; received {value!r}") from exc
    if aggregation <= 0:
        raise ValueError(f"aggregation must be a positive integer; received {value!r}")
    return aggregation


def validate_aggregations(value: int | str | Sequence[int | str]) -> tuple[int, ...]:
    """Normalize a comma-separated or sequence of aggregation sizes."""

    if isinstance(value, str):
        values: list[int | str] = value.split(",")
    elif isinstance(value, int):
        values = [value]
    else:
        try:
            values = list(value)
        except TypeError as exc:
            raise ValueError("aggregations must be a comma-separated list of positive integers") from exc

    if not values:
        raise ValueError("at least one aggregation is required")

    normalized: list[int] = []
    seen: set[int] = set()
    for item in values:
        if isinstance(item, str) and not item.strip():
            raise ValueError("aggregation list contains an empty value")
        aggregation = validate_aggregation(item)
        if aggregation not in seen:
            normalized.append(aggregation)
            seen.add(aggregation)
    return tuple(normalized)


def build_endpoint_url(instrument: str, side: str, requested_date: date) -> str:
    """Build the Dukascopy endpoint URL for one UTC calendar date."""

    normalized_instrument = validate_instrument(instrument)
    normalized_side = validate_side(side)
    if not isinstance(requested_date, date):
        raise TypeError("requested_date must be a datetime.date")
    return (
        f"{BASE_URL}/{normalized_instrument}/{normalized_side}/"
        f"{requested_date.year}/{requested_date.month}/{requested_date.day}"
    )


def _as_decimal(value: Any, field: str) -> Decimal:
    if isinstance(value, bool):
        raise DataValidationError(f"{field} must be numeric")
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise DataValidationError(f"{field} must be numeric; received {value!r}") from exc
    if not result.is_finite():
        raise DataValidationError(f"{field} must be finite; received {value!r}")
    return result


def _as_int(value: Any, field: str) -> int:
    if isinstance(value, bool):
        raise DataValidationError(f"{field} must be an integer")
    if isinstance(value, int):
        return value
    decimal_value = _as_decimal(value, field)
    if decimal_value != decimal_value.to_integral_value():
        raise DataValidationError(f"{field} must be an integer; received {value!r}")
    return int(decimal_value)


def _require_mapping(payload: Mapping[str, Any], field: str) -> Any:
    if field not in payload:
        raise DataValidationError(f"response is missing required field {field!r}")
    return payload[field]


def _require_sequence(payload: Mapping[str, Any], field: str) -> list[Any]:
    value = _require_mapping(payload, field)
    if not isinstance(value, list):
        raise DataValidationError(f"{field} must be a JSON array")
    return value


def _expand_price_series(
    initial_value: Decimal,
    deltas: Sequence[Any],
    multiplier: Decimal,
    field: str,
) -> list[Decimal]:
    if not deltas:
        raise DataValidationError(f"{field} must contain at least one value")

    delta_values = [_as_int(value, f"{field}[{index}]") for index, value in enumerate(deltas)]
    if delta_values[0] != 0:
        raise DataValidationError(f"{field}[0] must be zero")

    values: list[Decimal] = []
    current = initial_value
    for index, delta in enumerate(delta_values):
        if index > 0:
            current += Decimal(delta) * multiplier
        values.append(current)
    return values


def decode_payload(payload: Mapping[str, Any]) -> list[MinuteCandle]:
    """Expand one decoded Dukascopy response into minute candles."""

    if not isinstance(payload, Mapping):
        raise DataValidationError("response must be a JSON object")

    base_timestamp = _as_int(_require_mapping(payload, "timestamp"), "timestamp")
    if base_timestamp <= 0:
        raise DataValidationError("timestamp must be positive")

    multiplier = _as_decimal(_require_mapping(payload, "multiplier"), "multiplier")
    if multiplier <= 0:
        raise DataValidationError("multiplier must be positive")

    shift = _as_int(_require_mapping(payload, "shift"), "shift")
    if shift <= 0:
        raise DataValidationError("shift must be positive")

    times = _require_sequence(payload, "times")
    opens = _require_sequence(payload, "opens")
    highs = _require_sequence(payload, "highs")
    lows = _require_sequence(payload, "lows")
    closes = _require_sequence(payload, "closes")
    volumes = _require_sequence(payload, "volumes")

    series = {
        "times": times,
        "opens": opens,
        "highs": highs,
        "lows": lows,
        "closes": closes,
        "volumes": volumes,
    }
    length = len(times)
    for field, values in series.items():
        if len(values) != length:
            raise DataValidationError(
                "compressed arrays must have equal lengths; "
                f"times has {length}, {field} has {len(values)}"
            )
    if length == 0:
        return []

    time_deltas = [_as_int(value, f"times[{index}]") for index, value in enumerate(times)]
    if any(value < 0 for value in time_deltas):
        raise DataValidationError("times deltas must be non-negative")

    initial_prices = {
        "opens": _as_decimal(_require_mapping(payload, "open"), "open"),
        "highs": _as_decimal(_require_mapping(payload, "high"), "high"),
        "lows": _as_decimal(_require_mapping(payload, "low"), "low"),
        "closes": _as_decimal(_require_mapping(payload, "close"), "close"),
    }
    expanded_prices = {
        field: _expand_price_series(initial_prices[field], series[field], multiplier, field)
        for field in ("opens", "highs", "lows", "closes")
    }

    volume_values: list[int] = []
    for index, value in enumerate(volumes):
        volume_decimal = _as_decimal(value, f"volumes[{index}]")
        if volume_decimal < 0:
            raise DataValidationError(f"volumes[{index}] must be non-negative")
        scaled_volume = volume_decimal * MILLION
        if scaled_volume != scaled_volume.to_integral_value():
            raise DataValidationError(
                f"volumes[{index}] does not represent an exact integer after scaling: {value!r}"
            )
        volume = int(scaled_volume)
        if volume > INT64_MAX:
            raise DataValidationError(f"volumes[{index}] exceeds int64 range")
        volume_values.append(volume)

    candles: list[MinuteCandle] = []
    elapsed_units = 0
    previous_timestamp: int | None = None
    for index, time_delta in enumerate(time_deltas):
        elapsed_units += time_delta
        timestamp_ms = base_timestamp + elapsed_units * shift
        if timestamp_ms <= 0:
            raise DataValidationError(f"decoded timestamp at index {index} must be positive")
        if previous_timestamp is not None and timestamp_ms <= previous_timestamp:
            raise DataValidationError(
                "decoded timestamps must be strictly increasing; "
                f"index {index} is {timestamp_ms} after {previous_timestamp}"
            )
        previous_timestamp = timestamp_ms
        candles.append(
            MinuteCandle(
                timestamp_ms=timestamp_ms,
                open=expanded_prices["opens"][index],
                high=expanded_prices["highs"][index],
                low=expanded_prices["lows"][index],
                close=expanded_prices["closes"][index],
                volume=volume_values[index],
            )
        )
    return candles


def decode_json_bytes(raw_bytes: bytes) -> list[MinuteCandle]:
    """Parse and validate a raw JSON response."""

    try:
        text = raw_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise DataValidationError("response is not valid UTF-8 JSON") from exc
    try:
        payload = json.loads(text, parse_float=Decimal, parse_int=int)
    except json.JSONDecodeError as exc:
        raise DataValidationError(f"response is not valid JSON: {exc.msg}") from exc
    return decode_payload(payload)


def aggregate_candles(
    candles: Sequence[MinuteCandle], aggregation_minutes: int | str
) -> list[AggregatedCandle]:
    """Aggregate minute candles on UTC epoch-aligned boundaries."""

    aggregation = validate_aggregation(aggregation_minutes)
    if not candles:
        raise ValueError("cannot aggregate an empty candle sequence")

    ordered = sorted(candles, key=lambda candle: candle.timestamp_ms)
    for previous, current in zip(ordered, ordered[1:]):
        if current.timestamp_ms <= previous.timestamp_ms:
            raise DataValidationError("candles must have strictly increasing timestamps")

    period_ms = aggregation * MILLISECONDS_PER_MINUTE
    grouped: dict[int, AggregatedCandle] = {}
    for candle in ordered:
        bucket_timestamp = (candle.timestamp_ms // period_ms) * period_ms
        existing = grouped.get(bucket_timestamp)
        if existing is None:
            grouped[bucket_timestamp] = AggregatedCandle(
                timestamp_ms=bucket_timestamp,
                open=candle.open,
                high=candle.high,
                low=candle.low,
                close=candle.close,
                volume=candle.volume,
            )
            continue

        new_volume = existing.volume + candle.volume
        if new_volume > INT64_MAX:
            raise DataValidationError(
                f"aggregated volume for bucket {bucket_timestamp} exceeds int64 range"
            )
        grouped[bucket_timestamp] = AggregatedCandle(
            timestamp_ms=existing.timestamp_ms,
            open=existing.open,
            high=max(existing.high, candle.high),
            low=min(existing.low, candle.low),
            close=candle.close,
            volume=new_volume,
        )
    return [grouped[timestamp] for timestamp in sorted(grouped)]


def combine_candles(
    bid_candles: Sequence[MinuteCandle], ask_candles: Sequence[MinuteCandle]
) -> list[tuple[MinuteCandle, MinuteCandle]]:
    """Pair BID and ASK candles only when their decoded timelines match."""

    if len(bid_candles) != len(ask_candles):
        raise DataValidationError(
            "BID and ASK candle counts must match; "
            f"BID has {len(bid_candles)}, ASK has {len(ask_candles)}"
        )
    for index, (bid_candle, ask_candle) in enumerate(zip(bid_candles, ask_candles)):
        if bid_candle.timestamp_ms != ask_candle.timestamp_ms:
            raise DataValidationError(
                "BID and ASK timestamps must match at each position; "
                f"index {index} has {bid_candle.timestamp_ms} and {ask_candle.timestamp_ms}"
            )
    return list(zip(bid_candles, ask_candles))


def aggregate_combined_candles(
    bid_candles: Sequence[MinuteCandle],
    ask_candles: Sequence[MinuteCandle],
    aggregation_minutes: int | str,
) -> list[CombinedAggregatedCandle]:
    """Aggregate aligned BID and ASK candles into combined rows."""

    paired = combine_candles(bid_candles, ask_candles)
    if not paired:
        return []

    bid_aggregated = aggregate_candles(bid_candles, aggregation_minutes)
    ask_aggregated = aggregate_candles(ask_candles, aggregation_minutes)
    if len(bid_aggregated) != len(ask_aggregated):
        raise DataValidationError("BID and ASK aggregation bucket counts must match")

    combined: list[CombinedAggregatedCandle] = []
    for index, (bid_candle, ask_candle) in enumerate(zip(bid_aggregated, ask_aggregated)):
        if bid_candle.timestamp_ms != ask_candle.timestamp_ms:
            raise DataValidationError(
                "BID and ASK aggregation buckets must match; "
                f"index {index} has {bid_candle.timestamp_ms} and {ask_candle.timestamp_ms}"
            )
        combined.append(
            CombinedAggregatedCandle(
                timestamp_ms=bid_candle.timestamp_ms,
                bid_open=bid_candle.open,
                bid_high=bid_candle.high,
                bid_low=bid_candle.low,
                bid_close=bid_candle.close,
                ask_open=ask_candle.open,
                ask_high=ask_candle.high,
                ask_low=ask_candle.low,
                ask_close=ask_candle.close,
                bid_volume=bid_candle.volume,
                ask_volume=ask_candle.volume,
            )
        )
    return combined


def raw_json_path(output_root: Path, instrument: str, requested_date: date, side: str) -> Path:
    """Return the deterministic raw JSON destination."""

    return (
        output_root
        / f"instrument={instrument}"
        / "json"
        / "minute"
        / f"year={requested_date.year:04d}"
        / f"month={requested_date.month:02d}"
        / f"day={requested_date.day:02d}"
        / f"{instrument}-{requested_date.isoformat()}-{side}.json"
    )


def _partition_date(timestamp_ms: int) -> date:
    return datetime.fromtimestamp(timestamp_ms // 1000, tz=timezone.utc).date()


def candle_output_paths(
    candles: Sequence[AggregatedCandle],
    output_root: Path,
    instrument: str,
    requested_date: date,
    side: str,
    aggregation_minutes: int,
    output_format: str = "parquet",
) -> dict[Path, list[AggregatedCandle]]:
    """Group candle rows by UTC Hive partition and return final paths."""

    normalized_format = validate_output_format(output_format)
    partitions: dict[Path, list[AggregatedCandle]] = defaultdict(list)
    extension = ".csv" if normalized_format == "csv" else ".parquet"
    filename = f"{instrument}-{requested_date.isoformat()}-{side}{extension}"
    for candle in candles:
        partition_date = _partition_date(candle.timestamp_ms)
        partition_dir = (
            output_root
            / f"instrument={instrument}"
            / f"tf={aggregation_minutes}m"
            / f"year={partition_date.year:04d}"
            / f"month={partition_date.month:02d}"
            / f"day={partition_date.day:02d}"
        )
        partitions[partition_dir / filename].append(candle)
    return dict(sorted(partitions.items(), key=lambda item: str(item[0])))


def parquet_output_paths(
    candles: Sequence[AggregatedCandle],
    output_root: Path,
    instrument: str,
    requested_date: date,
    side: str,
    aggregation_minutes: int,
) -> dict[Path, list[AggregatedCandle]]:
    """Return Parquet candle paths for backward-compatible callers."""

    return candle_output_paths(
        candles,
        output_root,
        instrument,
        requested_date,
        side,
        aggregation_minutes,
        "parquet",
    )


def csv_output_paths(
    candles: Sequence[AggregatedCandle],
    output_root: Path,
    instrument: str,
    requested_date: date,
    side: str,
    aggregation_minutes: int,
) -> dict[Path, list[AggregatedCandle]]:
    """Return CSV candle paths grouped by UTC Hive partition."""

    return candle_output_paths(
        candles,
        output_root,
        instrument,
        requested_date,
        side,
        aggregation_minutes,
        "csv",
    )


def combined_candle_output_paths(
    candles: Sequence[CombinedAggregatedCandle],
    output_root: Path,
    instrument: str,
    requested_date: date,
    aggregation_minutes: int,
    output_format: str = "parquet",
) -> dict[Path, list[CombinedAggregatedCandle]]:
    """Return combined paths grouped by their UTC Hive partition."""

    normalized_format = validate_output_format(output_format)
    partitions: dict[Path, list[CombinedAggregatedCandle]] = defaultdict(list)
    extension = ".csv" if normalized_format == "csv" else ".parquet"
    filename = f"{instrument}-{requested_date.isoformat()}-COMB{extension}"
    for candle in candles:
        partition_date = _partition_date(candle.timestamp_ms)
        partition_dir = (
            output_root
            / f"instrument={instrument}"
            / f"tf={aggregation_minutes}m"
            / f"year={partition_date.year:04d}"
            / f"month={partition_date.month:02d}"
            / f"day={partition_date.day:02d}"
        )
        partitions[partition_dir / filename].append(candle)
    return dict(sorted(partitions.items(), key=lambda item: str(item[0])))


def combined_parquet_output_paths(
    candles: Sequence[CombinedAggregatedCandle],
    output_root: Path,
    instrument: str,
    requested_date: date,
    aggregation_minutes: int,
) -> dict[Path, list[CombinedAggregatedCandle]]:
    """Return combined Parquet paths for backward-compatible callers."""

    return combined_candle_output_paths(
        candles,
        output_root,
        instrument,
        requested_date,
        aggregation_minutes,
        "parquet",
    )


def combined_csv_output_paths(
    candles: Sequence[CombinedAggregatedCandle],
    output_root: Path,
    instrument: str,
    requested_date: date,
    aggregation_minutes: int,
) -> dict[Path, list[CombinedAggregatedCandle]]:
    """Return combined CSV paths grouped by UTC Hive partition."""

    return combined_candle_output_paths(
        candles,
        output_root,
        instrument,
        requested_date,
        aggregation_minutes,
        "csv",
    )


def _utc_timestamp_array(candles: Sequence[AggregatedCandle]) -> pa.Array:
    return pa.array(
        [candle.timestamp_ms for candle in candles],
        type=pa.timestamp("ms", tz="UTC"),
    )


def _table_for_candles(
    candles: Sequence[AggregatedCandle],
    instrument: str,
    side: str,
    requested_date: date,
    aggregation_minutes: int,
) -> pa.Table:
    table = pa.table(
        {
            "timestamp": _utc_timestamp_array(candles),
            "open": pa.array([float(candle.open) for candle in candles], type=pa.float64()),
            "high": pa.array([float(candle.high) for candle in candles], type=pa.float64()),
            "low": pa.array([float(candle.low) for candle in candles], type=pa.float64()),
            "close": pa.array([float(candle.close) for candle in candles], type=pa.float64()),
            "volume": pa.array([candle.volume for candle in candles], type=pa.int64()),
        }
    )
    metadata = {
        b"instrument": instrument.encode("utf-8"),
        b"side": side.encode("utf-8"),
        b"requested_date": requested_date.isoformat().encode("ascii"),
        b"aggregation_minutes": str(aggregation_minutes).encode("ascii"),
        b"source_url": build_endpoint_url(instrument, side, requested_date).encode("utf-8"),
    }
    return table.replace_schema_metadata(metadata)


def _table_for_combined_candles(
    candles: Sequence[CombinedAggregatedCandle],
    instrument: str,
    requested_date: date,
    aggregation_minutes: int,
) -> pa.Table:
    table = pa.table(
        {
            "timestamp": pa.array(
                [candle.timestamp_ms for candle in candles],
                type=pa.timestamp("ms", tz="UTC"),
            ),
            "bidOpen": pa.array([float(candle.bid_open) for candle in candles], type=pa.float64()),
            "bidHigh": pa.array([float(candle.bid_high) for candle in candles], type=pa.float64()),
            "bidLow": pa.array([float(candle.bid_low) for candle in candles], type=pa.float64()),
            "bidClose": pa.array([float(candle.bid_close) for candle in candles], type=pa.float64()),
            "askOpen": pa.array([float(candle.ask_open) for candle in candles], type=pa.float64()),
            "askHigh": pa.array([float(candle.ask_high) for candle in candles], type=pa.float64()),
            "askLow": pa.array([float(candle.ask_low) for candle in candles], type=pa.float64()),
            "askClose": pa.array([float(candle.ask_close) for candle in candles], type=pa.float64()),
            "bidVolume": pa.array([candle.bid_volume for candle in candles], type=pa.int64()),
            "askVolume": pa.array([candle.ask_volume for candle in candles], type=pa.int64()),
        }
    )
    metadata = {
        b"instrument": instrument.encode("utf-8"),
        b"side": b"COMB",
        b"requested_date": requested_date.isoformat().encode("ascii"),
        b"aggregation_minutes": str(aggregation_minutes).encode("ascii"),
        b"bid_source_url": build_endpoint_url(instrument, "BID", requested_date).encode("utf-8"),
        b"ask_source_url": build_endpoint_url(instrument, "ASK", requested_date).encode("utf-8"),
    }
    return table.replace_schema_metadata(metadata)


def _write_csv_partitions(
    paths_to_rows: Mapping[Path, Sequence[Any]],
    headers: Sequence[str],
    row_builder: Callable[[Any], Sequence[Any]],
    *,
    kind: str,
) -> list[Path]:
    """Write partitioned CSV files atomically without overwriting."""

    final_paths = list(paths_to_rows)
    existing = next((path for path in final_paths if path.exists()), None)
    if existing is not None:
        raise FileExistsError(f"refusing to overwrite existing CSV file: {existing}")

    published_paths: list[Path] = []
    temporary_paths: list[Path] = []
    try:
        for final_path, partition_rows in paths_to_rows.items():
            final_path.parent.mkdir(parents=True, exist_ok=True)
            fd, temporary_name = tempfile.mkstemp(
                prefix=f".{kind}-csv-",
                suffix=".tmp",
                dir=final_path.parent,
            )
            os.close(fd)
            temporary_path = Path(temporary_name)
            temporary_paths.append(temporary_path)
            with temporary_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle, lineterminator="\n")
                writer.writerow(headers)
                for row in partition_rows:
                    writer.writerow(row_builder(row))

        for temporary_path, final_path in zip(temporary_paths, final_paths):
            if final_path.exists():
                raise FileExistsError(f"refusing to overwrite existing CSV file: {final_path}")
            os.replace(temporary_path, final_path)
            published_paths.append(final_path)
        temporary_paths.clear()
        return final_paths
    except Exception:
        for temporary_path in temporary_paths:
            temporary_path.unlink(missing_ok=True)
        for published_path in published_paths:
            published_path.unlink(missing_ok=True)
        raise


def write_parquet(
    candles: Sequence[AggregatedCandle],
    output_root: Path,
    instrument: str,
    requested_date: date,
    side: str,
    aggregation_minutes: int | str,
) -> list[Path]:
    """Write aggregated candles into non-overwriting Hive-partitioned Parquet files."""

    if not candles:
        raise ValueError("cannot write an empty candle sequence")
    normalized_instrument = validate_instrument(instrument)
    normalized_side = validate_side(side)
    normalized_aggregation = validate_aggregation(aggregation_minutes)
    paths_to_rows = parquet_output_paths(
        candles,
        Path(output_root),
        normalized_instrument,
        requested_date,
        normalized_side,
        normalized_aggregation,
    )
    final_paths = list(paths_to_rows)
    existing = next((path for path in final_paths if path.exists()), None)
    if existing is not None:
        raise FileExistsError(f"refusing to overwrite existing Parquet file: {existing}")

    published_paths: list[Path] = []
    temporary_paths: list[Path] = []
    try:
        for final_path, partition_candles in paths_to_rows.items():
            final_path.parent.mkdir(parents=True, exist_ok=True)
            fd, temporary_name = tempfile.mkstemp(
                prefix=".parquet-",
                suffix=".tmp",
                dir=final_path.parent,
            )
            os.close(fd)
            temporary_path = Path(temporary_name)
            temporary_paths.append(temporary_path)
            table = _table_for_candles(
                partition_candles,
                normalized_instrument,
                normalized_side,
                requested_date,
                normalized_aggregation,
            )
            pq.write_table(table, temporary_path, compression="zstd")

        for temporary_path, final_path in zip(temporary_paths, final_paths):
            if final_path.exists():
                raise FileExistsError(f"refusing to overwrite existing Parquet file: {final_path}")
            os.replace(temporary_path, final_path)
            published_paths.append(final_path)
        temporary_paths.clear()
        return final_paths
    except Exception:
        for temporary_path in temporary_paths:
            temporary_path.unlink(missing_ok=True)
        for published_path in published_paths:
            published_path.unlink(missing_ok=True)
        raise


def write_csv(
    candles: Sequence[AggregatedCandle],
    output_root: Path,
    instrument: str,
    requested_date: date,
    side: str,
    aggregation_minutes: int | str,
) -> list[Path]:
    """Write aggregated candles into non-overwriting Hive-partitioned CSV files."""

    if not candles:
        raise ValueError("cannot write an empty candle sequence")
    normalized_instrument = validate_instrument(instrument)
    normalized_side = validate_side(side)
    normalized_aggregation = validate_aggregation(aggregation_minutes)
    paths_to_rows = csv_output_paths(
        candles,
        Path(output_root),
        normalized_instrument,
        requested_date,
        normalized_side,
        normalized_aggregation,
    )
    return _write_csv_partitions(
        paths_to_rows,
        ("timestamp", "open", "high", "low", "close", "volume"),
        lambda candle: (
            format_timestamp_utc(candle.timestamp_ms),
            str(candle.open),
            str(candle.high),
            str(candle.low),
            str(candle.close),
            str(candle.volume),
        ),
        kind="candle",
    )


def write_combined_parquet(
    candles: Sequence[CombinedAggregatedCandle],
    output_root: Path,
    instrument: str,
    requested_date: date,
    aggregation_minutes: int | str,
) -> list[Path]:
    """Write combined BID/ASK candles into non-overwriting Parquet files."""

    if not candles:
        raise ValueError("cannot write an empty candle sequence")
    normalized_instrument = validate_instrument(instrument)
    normalized_aggregation = validate_aggregation(aggregation_minutes)
    paths_to_rows = combined_parquet_output_paths(
        candles,
        Path(output_root),
        normalized_instrument,
        requested_date,
        normalized_aggregation,
    )
    final_paths = list(paths_to_rows)
    existing = next((path for path in final_paths if path.exists()), None)
    if existing is not None:
        raise FileExistsError(f"refusing to overwrite existing Parquet file: {existing}")

    published_paths: list[Path] = []
    temporary_paths: list[Path] = []
    try:
        for final_path, partition_candles in paths_to_rows.items():
            final_path.parent.mkdir(parents=True, exist_ok=True)
            fd, temporary_name = tempfile.mkstemp(
                prefix=".parquet-",
                suffix=".tmp",
                dir=final_path.parent,
            )
            os.close(fd)
            temporary_path = Path(temporary_name)
            temporary_paths.append(temporary_path)
            table = _table_for_combined_candles(
                partition_candles,
                normalized_instrument,
                requested_date,
                normalized_aggregation,
            )
            pq.write_table(table, temporary_path, compression="zstd")

        for temporary_path, final_path in zip(temporary_paths, final_paths):
            if final_path.exists():
                raise FileExistsError(f"refusing to overwrite existing Parquet file: {final_path}")
            os.replace(temporary_path, final_path)
            published_paths.append(final_path)
        temporary_paths.clear()
        return final_paths
    except Exception:
        for temporary_path in temporary_paths:
            temporary_path.unlink(missing_ok=True)
        for published_path in published_paths:
            published_path.unlink(missing_ok=True)
        raise


def write_combined_csv(
    candles: Sequence[CombinedAggregatedCandle],
    output_root: Path,
    instrument: str,
    requested_date: date,
    aggregation_minutes: int | str,
) -> list[Path]:
    """Write combined BID/ASK candles into non-overwriting CSV files."""

    if not candles:
        raise ValueError("cannot write an empty candle sequence")
    normalized_instrument = validate_instrument(instrument)
    normalized_aggregation = validate_aggregation(aggregation_minutes)
    paths_to_rows = combined_csv_output_paths(
        candles,
        Path(output_root),
        normalized_instrument,
        requested_date,
        normalized_aggregation,
    )
    return _write_csv_partitions(
        paths_to_rows,
        (
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
        ),
        lambda candle: (
            format_timestamp_utc(candle.timestamp_ms),
            str(candle.bid_open),
            str(candle.bid_high),
            str(candle.bid_low),
            str(candle.bid_close),
            str(candle.ask_open),
            str(candle.ask_high),
            str(candle.ask_low),
            str(candle.ask_close),
            str(candle.bid_volume),
            str(candle.ask_volume),
        ),
        kind="combined-candle",
    )


def _write_raw_json(raw_bytes: bytes, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        raise FileExistsError(f"refusing to overwrite existing raw JSON: {destination}")
    fd, temporary_name = tempfile.mkstemp(
        prefix=".json-",
        suffix=".tmp",
        dir=destination.parent,
    )
    os.close(fd)
    temporary_path = Path(temporary_name)
    try:
        temporary_path.write_bytes(raw_bytes)
        if destination.exists():
            raise FileExistsError(f"refusing to overwrite existing raw JSON: {destination}")
        os.replace(temporary_path, destination)
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise


def download_json_bytes(
    url: str,
    *,
    timeout: float = HTTP_TIMEOUT_SECONDS,
    opener: Callable[..., Any] = urllib.request.urlopen,
    sleeper: Callable[[float], None] = time.sleep,
) -> bytes:
    """Download one response, retrying only transient failures."""

    request = urllib.request.Request(
        url,
        headers={"User-Agent": "DukascopyCandleDownloader/1.0"},
        method="GET",
    )
    last_error: Exception | None = None
    for attempt in range(MAX_DOWNLOAD_ATTEMPTS):
        try:
            with opener(request, timeout=timeout) as response:
                status = getattr(response, "status", 200)
                if status >= 400:
                    status_error = DownloadError(f"HTTP {status} from {url}")
                    last_error = status_error
                    if status not in RETRYABLE_HTTP_CODES or attempt >= MAX_DOWNLOAD_ATTEMPTS - 1:
                        raise status_error
                else:
                    return response.read()
        except urllib.error.HTTPError as exc:
            last_error = exc
            if exc.code not in RETRYABLE_HTTP_CODES or attempt >= MAX_DOWNLOAD_ATTEMPTS - 1:
                raise DownloadError(f"HTTP {exc.code} while downloading {url}") from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            last_error = exc
            if attempt >= MAX_DOWNLOAD_ATTEMPTS - 1:
                raise DownloadError(f"could not download {url}: {exc}") from exc
        if attempt < len(RETRY_DELAYS_SECONDS):
            sleeper(RETRY_DELAYS_SECONDS[attempt])
    raise DownloadError(f"could not download {url}: {last_error}") from last_error


def _load_or_download_raw(
    instrument: str,
    side: str,
    requested_date: date,
    *,
    output_root: Path,
    fetcher: Callable[[str], bytes],
) -> tuple[bytes, Path, bool]:
    """Load one validated-cache candidate or download its raw response."""

    normalized_instrument = validate_instrument(instrument)
    normalized_side = validate_side(side)
    normalized_root = Path(output_root)
    destination = raw_json_path(
        normalized_root,
        normalized_instrument,
        requested_date,
        normalized_side,
    )
    was_downloaded = not destination.exists()
    if was_downloaded:
        return (
            fetcher(build_endpoint_url(normalized_instrument, normalized_side, requested_date)),
            destination,
            True,
        )
    return destination.read_bytes(), destination, False


def run_downloads(
    instrument: str,
    side: str,
    requested_date: date,
    aggregation_minutes: int | str | Sequence[int | str],
    *,
    output_root: Path,
    fetcher: Callable[[str], bytes] = download_json_bytes,
    output_format: str = "parquet",
) -> DownloadBatchResult | CombinedDownloadBatchResult:
    """Reuse or download raw data, then publish requested aggregations."""

    normalized_instrument = validate_instrument(instrument)
    normalized_side = validate_download_side(side)
    normalized_aggregations = validate_aggregations(aggregation_minutes)
    normalized_format = validate_output_format(output_format)
    normalized_root = Path(output_root)
    if normalized_side == "COMB":
        return run_combined_downloads(
            normalized_instrument,
            requested_date,
            normalized_aggregations,
            output_root=normalized_root,
            fetcher=fetcher,
            output_format=normalized_format,
        )

    raw_bytes, json_destination, raw_was_downloaded = _load_or_download_raw(
        normalized_instrument,
        normalized_side,
        requested_date,
        output_root=normalized_root,
        fetcher=fetcher,
    )
    minute_candles = decode_json_bytes(raw_bytes)

    if not minute_candles:
        if raw_was_downloaded:
            _write_raw_json(raw_bytes, json_destination)
        return DownloadBatchResult(
            json_path=json_destination,
            output_paths=(),
            minute_count=0,
            raw_was_downloaded=raw_was_downloaded,
            created_aggregations=(),
            skipped_aggregations=(),
            output_format=normalized_format,
        )

    published_outputs: list[Path] = []
    created_aggregations: list[int] = []
    skipped_aggregations: list[int] = []
    try:
        for aggregation in normalized_aggregations:
            aggregated_candles = aggregate_candles(minute_candles, aggregation)
            output_paths = candle_output_paths(
                aggregated_candles,
                normalized_root,
                normalized_instrument,
                requested_date,
                normalized_side,
                aggregation,
                normalized_format,
            )
            if any(path.exists() for path in output_paths):
                skipped_aggregations.append(aggregation)
                continue

            if normalized_format == "csv":
                published_outputs.extend(
                    write_csv(
                        aggregated_candles,
                        normalized_root,
                        normalized_instrument,
                        requested_date,
                        normalized_side,
                        aggregation,
                    )
                )
            else:
                published_outputs.extend(
                    write_parquet(
                        aggregated_candles,
                        normalized_root,
                        normalized_instrument,
                        requested_date,
                        normalized_side,
                        aggregation,
                    )
                )
            created_aggregations.append(aggregation)
        if raw_was_downloaded:
            _write_raw_json(raw_bytes, json_destination)
    except Exception:
        for path in published_outputs:
            path.unlink(missing_ok=True)
        raise
    return DownloadBatchResult(
        json_path=json_destination,
        output_paths=tuple(published_outputs),
        minute_count=len(minute_candles),
        raw_was_downloaded=raw_was_downloaded,
        created_aggregations=tuple(created_aggregations),
        skipped_aggregations=tuple(skipped_aggregations),
        output_format=normalized_format,
    )


def run_combined_downloads(
    instrument: str,
    requested_date: date,
    aggregation_minutes: int | str | Sequence[int | str],
    *,
    output_root: Path,
    fetcher: Callable[[str], bytes] = download_json_bytes,
    output_format: str = "parquet",
) -> CombinedDownloadBatchResult:
    """Reuse or download both sides, then publish combined aggregations."""

    normalized_instrument = validate_instrument(instrument)
    normalized_aggregations = validate_aggregations(aggregation_minutes)
    normalized_format = validate_output_format(output_format)
    normalized_root = Path(output_root)

    raw_by_side: dict[str, tuple[bytes, Path, bool]] = {}
    for side in ("BID", "ASK"):
        raw_by_side[side] = _load_or_download_raw(
            normalized_instrument,
            side,
            requested_date,
            output_root=normalized_root,
            fetcher=fetcher,
        )

    bid_bytes, bid_json_path, bid_was_downloaded = raw_by_side["BID"]
    ask_bytes, ask_json_path, ask_was_downloaded = raw_by_side["ASK"]
    bid_candles = decode_json_bytes(bid_bytes)
    ask_candles = decode_json_bytes(ask_bytes)
    combine_candles(bid_candles, ask_candles)

    raw_downloaded_sides = tuple(
        side
        for side, was_downloaded in (
            ("BID", bid_was_downloaded),
            ("ASK", ask_was_downloaded),
        )
        if was_downloaded
    )
    raw_cached_sides = tuple(
        side
        for side, was_downloaded in (
            ("BID", bid_was_downloaded),
            ("ASK", ask_was_downloaded),
        )
        if not was_downloaded
    )

    if not bid_candles:
        for raw_bytes, destination, was_downloaded in raw_by_side.values():
            if was_downloaded:
                _write_raw_json(raw_bytes, destination)
        return CombinedDownloadBatchResult(
            json_paths=(bid_json_path, ask_json_path),
            output_paths=(),
            minute_count=0,
            raw_downloaded_sides=raw_downloaded_sides,
            raw_cached_sides=raw_cached_sides,
            created_aggregations=(),
            skipped_aggregations=(),
            output_format=normalized_format,
        )

    published_outputs: list[Path] = []
    created_aggregations: list[int] = []
    skipped_aggregations: list[int] = []
    written_raw_paths: list[Path] = []
    try:
        for aggregation in normalized_aggregations:
            aggregated_candles = aggregate_combined_candles(
                bid_candles,
                ask_candles,
                aggregation,
            )
            output_paths = combined_candle_output_paths(
                aggregated_candles,
                normalized_root,
                normalized_instrument,
                requested_date,
                aggregation,
                normalized_format,
            )
            if any(path.exists() for path in output_paths):
                skipped_aggregations.append(aggregation)
                continue

            if normalized_format == "csv":
                published_outputs.extend(
                    write_combined_csv(
                        aggregated_candles,
                        normalized_root,
                        normalized_instrument,
                        requested_date,
                        aggregation,
                    )
                )
            else:
                published_outputs.extend(
                    write_combined_parquet(
                        aggregated_candles,
                        normalized_root,
                        normalized_instrument,
                        requested_date,
                        aggregation,
                    )
                )
            created_aggregations.append(aggregation)

        for raw_bytes, destination, was_downloaded in raw_by_side.values():
            if was_downloaded:
                _write_raw_json(raw_bytes, destination)
                written_raw_paths.append(destination)
    except Exception:
        for path in published_outputs:
            path.unlink(missing_ok=True)
        for path in written_raw_paths:
            path.unlink(missing_ok=True)
        raise

    return CombinedDownloadBatchResult(
        json_paths=(bid_json_path, ask_json_path),
        output_paths=tuple(published_outputs),
        minute_count=len(bid_candles),
        raw_downloaded_sides=raw_downloaded_sides,
        raw_cached_sides=raw_cached_sides,
        created_aggregations=tuple(created_aggregations),
        skipped_aggregations=tuple(skipped_aggregations),
        output_format=normalized_format,
    )


def run_download(
    instrument: str,
    side: str,
    requested_date: date,
    aggregation_minutes: int | str,
    *,
    output_root: Path,
    fetcher: Callable[[str], bytes] = download_json_bytes,
    output_format: str = "parquet",
) -> tuple[Path, list[Path], int]:
    """Process one aggregation while preserving the original return shape."""

    normalized_side = validate_side(side)
    result = run_downloads(
        instrument,
        normalized_side,
        requested_date,
        (aggregation_minutes,),
        output_root=output_root,
        fetcher=fetcher,
        output_format=output_format,
    )
    return result.json_path, list(result.output_paths), result.minute_count


def run_date_range(
    instrument: str,
    side: str,
    requested_dates: Sequence[date],
    aggregation_minutes: int | str | Sequence[int | str],
    *,
    output_root: Path,
    fetcher: Callable[[str], bytes] = download_json_bytes,
    output_format: str = "parquet",
) -> tuple[DateRunOutcome, ...]:
    """Process dates independently and retain failures for the final summary."""

    if not requested_dates:
        raise ValueError("at least one requested date is required")

    normalized_side = validate_download_side(side)
    outcomes: list[DateRunOutcome] = []
    for requested_date in requested_dates:
        try:
            result = run_downloads(
                instrument,
                normalized_side,
                requested_date,
                aggregation_minutes,
                output_root=output_root,
                fetcher=fetcher,
                output_format=output_format,
            )
        except Exception as exc:
            outcomes.append(DateRunOutcome(requested_date, None, str(exc)))
        else:
            outcomes.append(DateRunOutcome(requested_date, result, None))
    return tuple(outcomes)
