"""Download, decode, cache, and store Dukascopy hourly tick data."""

from __future__ import annotations

import csv
import json
import os
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

import pyarrow as pa
import pyarrow.parquet as pq

from .candles import (
    INT64_MAX,
    DataValidationError,
    _as_decimal,
    _as_int,
    _write_raw_json,
    download_json_bytes,
    format_timestamp_utc,
    validate_output_format,
    validate_instrument,
)


TICKS_BASE_URL = "https://jetta.dukascopy.com/v1/ticks"
MILLISECONDS_PER_HOUR = 3_600_000
HOURS_PER_DAY = 24


@dataclass(frozen=True)
class Tick:
    """One expanded quote tick."""

    timestamp_ms: int
    bid_price: Decimal
    ask_price: Decimal
    bid_volume: int
    ask_volume: int


@dataclass(frozen=True)
class TickHourResult:
    """Result of processing one requested tick hour."""

    requested_date: date
    hour: int
    json_path: Path
    output_path: Path | None
    tick_count: int
    raw_was_downloaded: bool
    parquet_created: bool
    parquet_skipped: bool
    output_format: str = "parquet"
    cache_enabled: bool = True

    @property
    def parquet_path(self) -> Path | None:
        """Backward-compatible Parquet path view."""

        return self.output_path if self.output_format == "parquet" else None

    @property
    def csv_path(self) -> Path | None:
        """Return the generated CSV path for CSV output."""

        return self.output_path if self.output_format == "csv" else None

    @property
    def is_empty(self) -> bool:
        return self.tick_count == 0

    @property
    def source_message(self) -> str:
        if not self.cache_enabled:
            return "Downloaded without caching"
        return "Downloaded" if self.raw_was_downloaded else "Reused cached JSON"


@dataclass(frozen=True)
class TickHourOutcome:
    """Success or failure for one tick hour."""

    requested_date: date
    hour: int
    result: TickHourResult | None
    error: str | None


@dataclass(frozen=True)
class TickDateResult:
    """Results for all requested tick hours of one date."""

    requested_date: date
    hour_outcomes: tuple[TickHourOutcome, ...]

    @property
    def successful_hours(self) -> tuple[TickHourResult, ...]:
        return tuple(
            outcome.result
            for outcome in self.hour_outcomes
            if outcome.result is not None
        )

    @property
    def failed_hours(self) -> tuple[TickHourOutcome, ...]:
        return tuple(outcome for outcome in self.hour_outcomes if outcome.error is not None)

    @property
    def empty_hours(self) -> tuple[int, ...]:
        return tuple(result.hour for result in self.successful_hours if result.is_empty)

    @property
    def created_hours(self) -> tuple[int, ...]:
        return tuple(result.hour for result in self.successful_hours if result.parquet_created)

    @property
    def skipped_hours(self) -> tuple[int, ...]:
        return tuple(result.hour for result in self.successful_hours if result.parquet_skipped)

    @property
    def tick_count(self) -> int:
        return sum(result.tick_count for result in self.successful_hours)

    @property
    def requested_hours(self) -> tuple[int, ...]:
        return tuple(outcome.hour for outcome in self.hour_outcomes)

    @property
    def has_data(self) -> bool:
        return self.tick_count > 0


def validate_hour(value: int | str) -> int:
    """Validate one Dukascopy hourly bucket number."""

    if isinstance(value, bool):
        raise ValueError(f"hour must be an integer from 0 to 23; received {value!r}")
    try:
        hour = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"hour must be an integer from 0 to 23; received {value!r}") from exc
    if isinstance(value, float) and value != hour:
        raise ValueError(f"hour must be an integer from 0 to 23; received {value!r}")
    if hour < 0 or hour >= HOURS_PER_DAY:
        raise ValueError(f"hour must be an integer from 0 to 23; received {value!r}")
    return hour


def resolve_hours(hour: int | str | None) -> tuple[int, ...]:
    """Return one requested hour or all hours in a UTC day."""

    if hour is None:
        return tuple(range(HOURS_PER_DAY))
    return (validate_hour(hour),)


def build_tick_endpoint_url(instrument: str, requested_date: date, hour: int | str) -> str:
    """Build the unpadded Dukascopy hourly tick URL."""

    normalized_instrument = validate_instrument(instrument)
    normalized_hour = validate_hour(hour)
    if not isinstance(requested_date, date):
        raise TypeError("requested_date must be a datetime.date")
    return (
        f"{TICKS_BASE_URL}/{normalized_instrument}/"
        f"{requested_date.year}/{requested_date.month}/{requested_date.day}/{normalized_hour}"
    )


def tick_json_path(
    output_root: Path,
    instrument: str,
    requested_date: date,
    hour: int | str,
) -> Path:
    """Return the partitioned raw tick JSON cache path."""

    normalized_instrument = validate_instrument(instrument)
    normalized_hour = validate_hour(hour)
    return (
        Path(output_root)
        / f"instrument={normalized_instrument}"
        / "json"
        / "ticks"
        / f"year={requested_date.year:04d}"
        / f"month={requested_date.month:02d}"
        / f"day={requested_date.day:02d}"
        / f"{normalized_instrument}-{requested_date.isoformat()}-{normalized_hour:02d}-TICKS.json"
    )


def tick_output_path(
    output_root: Path,
    instrument: str,
    requested_date: date,
    hour: int | str,
    output_format: str = "parquet",
) -> Path:
    """Return the one-file-per-hour tick output path."""

    normalized_instrument = validate_instrument(instrument)
    normalized_hour = validate_hour(hour)
    normalized_format = validate_output_format(output_format)
    extension = ".csv" if normalized_format == "csv" else ".parquet"
    return (
        Path(output_root)
        / f"instrument={normalized_instrument}"
        / "tf=1tick"
        / f"year={requested_date.year:04d}"
        / f"month={requested_date.month:02d}"
        / f"day={requested_date.day:02d}"
        / f"hour={normalized_hour:02d}"
        / f"{normalized_instrument}-{requested_date.isoformat()}-{normalized_hour:02d}-TICKS{extension}"
    )


def tick_parquet_path(
    output_root: Path,
    instrument: str,
    requested_date: date,
    hour: int | str,
) -> Path:
    """Return the one-file-per-hour tick Parquet path."""

    return tick_output_path(output_root, instrument, requested_date, hour, "parquet")


def tick_csv_path(
    output_root: Path,
    instrument: str,
    requested_date: date,
    hour: int | str,
) -> Path:
    """Return the one-file-per-hour tick CSV path."""

    return tick_output_path(output_root, instrument, requested_date, hour, "csv")


def _require_field(payload: Mapping[str, Any], field: str) -> Any:
    if field not in payload:
        raise DataValidationError(f"tick response is missing required field {field!r}")
    return payload[field]


def _require_array(payload: Mapping[str, Any], field: str) -> list[Any]:
    value = _require_field(payload, field)
    if not isinstance(value, list):
        raise DataValidationError(f"tick field {field!r} must be a JSON array")
    return value


def _expand_tick_prices(
    initial_value: Decimal,
    deltas: Sequence[Any],
    multiplier: Decimal,
    field: str,
) -> list[Decimal]:
    if not deltas:
        return []
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


def _decode_tick_volume(value: Any, field: str) -> int:
    volume_decimal = _as_decimal(value, field)
    if volume_decimal < 0:
        raise DataValidationError(f"{field} must be non-negative")
    if volume_decimal != volume_decimal.to_integral_value():
        raise DataValidationError(f"{field} must be an exact integer; received {value!r}")
    volume = int(volume_decimal)
    if volume > INT64_MAX:
        raise DataValidationError(f"{field} exceeds int64 range")
    return volume


def decode_tick_payload(
    payload: Mapping[str, Any],
    *,
    requested_date: date | None = None,
    hour: int | str | None = None,
) -> list[Tick]:
    """Expand one compressed Dukascopy tick response."""

    if not isinstance(payload, Mapping):
        raise DataValidationError("tick response must be a JSON object")

    base_timestamp = _as_int(_require_field(payload, "timestamp"), "timestamp")
    if base_timestamp <= 0:
        raise DataValidationError("tick timestamp must be positive")

    multiplier = _as_decimal(_require_field(payload, "multiplier"), "multiplier")
    if multiplier <= 0:
        raise DataValidationError("tick multiplier must be positive")

    expected_start: int | None = None
    expected_end: int | None = None
    if requested_date is not None or hour is not None:
        if requested_date is None or hour is None:
            raise ValueError("requested_date and hour must be supplied together")
        normalized_hour = validate_hour(hour)
        expected_start = int(
            datetime.combine(requested_date, time.min, tzinfo=timezone.utc).timestamp() * 1000
        ) + normalized_hour * MILLISECONDS_PER_HOUR
        expected_end = expected_start + MILLISECONDS_PER_HOUR
        if base_timestamp != expected_start:
            raise DataValidationError(
                "tick timestamp does not match the requested UTC hour; "
                f"received {base_timestamp}, expected {expected_start}"
            )

    times = _require_array(payload, "times")
    asks = _require_array(payload, "asks")
    bids = _require_array(payload, "bids")
    ask_volumes = _require_array(payload, "askVolumes")
    bid_volumes = _require_array(payload, "bidVolumes")
    arrays = {
        "times": times,
        "asks": asks,
        "bids": bids,
        "askVolumes": ask_volumes,
        "bidVolumes": bid_volumes,
    }
    length = len(times)
    for field, values in arrays.items():
        if len(values) != length:
            raise DataValidationError(
                "tick arrays must have equal lengths; "
                f"times has {length}, {field} has {len(values)}"
            )

    if length == 0:
        if _require_field(payload, "ask") is not None or _require_field(payload, "bid") is not None:
            raise DataValidationError("empty tick responses must have null bid and ask values")
        return []

    initial_ask = _as_decimal(_require_field(payload, "ask"), "ask")
    initial_bid = _as_decimal(_require_field(payload, "bid"), "bid")
    time_deltas = [_as_int(value, f"times[{index}]") for index, value in enumerate(times)]
    if any(value < 0 for value in time_deltas):
        raise DataValidationError("tick time deltas must be non-negative")

    expanded_asks = _expand_tick_prices(initial_ask, asks, multiplier, "asks")
    expanded_bids = _expand_tick_prices(initial_bid, bids, multiplier, "bids")
    decoded_ask_volumes = [
        _decode_tick_volume(value, f"askVolumes[{index}]")
        for index, value in enumerate(ask_volumes)
    ]
    decoded_bid_volumes = [
        _decode_tick_volume(value, f"bidVolumes[{index}]")
        for index, value in enumerate(bid_volumes)
    ]

    ticks: list[Tick] = []
    elapsed_ms = 0
    previous_timestamp: int | None = None
    for index, time_delta in enumerate(time_deltas):
        elapsed_ms += time_delta
        timestamp_ms = base_timestamp + elapsed_ms
        if timestamp_ms <= 0:
            raise DataValidationError(f"decoded tick timestamp at index {index} must be positive")
        if previous_timestamp is not None and timestamp_ms < previous_timestamp:
            raise DataValidationError(
                "decoded tick timestamps must be non-decreasing; "
                f"index {index} is {timestamp_ms} after {previous_timestamp}"
            )
        if expected_start is not None and not expected_start <= timestamp_ms < expected_end:
            raise DataValidationError(
                f"decoded tick timestamp at index {index} is outside the requested UTC hour"
            )
        previous_timestamp = timestamp_ms
        ticks.append(
            Tick(
                timestamp_ms=timestamp_ms,
                bid_price=expanded_bids[index],
                ask_price=expanded_asks[index],
                bid_volume=decoded_bid_volumes[index],
                ask_volume=decoded_ask_volumes[index],
            )
        )
    return ticks


def decode_tick_json_bytes(
    raw_bytes: bytes,
    *,
    requested_date: date | None = None,
    hour: int | str | None = None,
) -> list[Tick]:
    """Parse and validate one raw tick JSON response."""

    try:
        text = raw_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise DataValidationError("tick response is not valid UTF-8 JSON") from exc
    try:
        payload = json.loads(text, parse_float=Decimal, parse_int=int)
    except json.JSONDecodeError as exc:
        raise DataValidationError(f"tick response is not valid JSON: {exc.msg}") from exc
    return decode_tick_payload(payload, requested_date=requested_date, hour=hour)


def _tick_table(
    ticks: Sequence[Tick],
    instrument: str,
    requested_date: date,
    hour: int,
) -> pa.Table:
    table = pa.table(
        {
            "timestamp": pa.array(
                [tick.timestamp_ms for tick in ticks],
                type=pa.timestamp("ms", tz="UTC"),
            ),
            "bidPrice": pa.array([float(tick.bid_price) for tick in ticks], type=pa.float64()),
            "askPrice": pa.array([float(tick.ask_price) for tick in ticks], type=pa.float64()),
            "bidVolume": pa.array([tick.bid_volume for tick in ticks], type=pa.int64()),
            "askVolume": pa.array([tick.ask_volume for tick in ticks], type=pa.int64()),
        }
    )
    metadata = {
        b"instrument": instrument.encode("utf-8"),
        b"requested_date": requested_date.isoformat().encode("ascii"),
        b"hour": f"{hour:02d}".encode("ascii"),
        b"source_url": build_tick_endpoint_url(instrument, requested_date, hour).encode("utf-8"),
    }
    return table.replace_schema_metadata(metadata)


def write_ticks_parquet(
    ticks: Sequence[Tick],
    output_root: Path,
    instrument: str,
    requested_date: date,
    hour: int | str,
) -> Path:
    """Write one tick Parquet file atomically without overwriting."""

    if not ticks:
        raise ValueError("cannot write an empty tick sequence")
    normalized_instrument = validate_instrument(instrument)
    normalized_hour = validate_hour(hour)
    final_path = tick_parquet_path(
        Path(output_root), normalized_instrument, requested_date, normalized_hour
    )
    if final_path.exists():
        raise FileExistsError("refusing to overwrite existing tick Parquet output")

    final_path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(
        prefix=".ticks-parquet-",
        suffix=".tmp",
        dir=final_path.parent,
    )
    os.close(fd)
    temporary_path = Path(temporary_name)
    published = False
    try:
        pq.write_table(
            _tick_table(ticks, normalized_instrument, requested_date, normalized_hour),
            temporary_path,
            compression="zstd",
        )
        if final_path.exists():
            raise FileExistsError("refusing to overwrite existing tick Parquet output")
        os.replace(temporary_path, final_path)
        published = True
        return final_path
    finally:
        if not published:
            temporary_path.unlink(missing_ok=True)


def write_ticks_csv(
    ticks: Sequence[Tick],
    output_root: Path,
    instrument: str,
    requested_date: date,
    hour: int | str,
) -> Path:
    """Write one tick CSV file atomically without overwriting."""

    if not ticks:
        raise ValueError("cannot write an empty tick sequence")
    normalized_instrument = validate_instrument(instrument)
    normalized_hour = validate_hour(hour)
    final_path = tick_csv_path(
        Path(output_root), normalized_instrument, requested_date, normalized_hour
    )
    if final_path.exists():
        raise FileExistsError("refusing to overwrite existing tick CSV output")

    final_path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(
        prefix=".ticks-csv-",
        suffix=".tmp",
        dir=final_path.parent,
    )
    os.close(fd)
    temporary_path = Path(temporary_name)
    published = False
    try:
        with temporary_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle, lineterminator="\n")
            writer.writerow(("timestamp", "bidPrice", "askPrice", "bidVolume", "askVolume"))
            for tick in ticks:
                writer.writerow(
                    (
                        format_timestamp_utc(tick.timestamp_ms),
                        str(tick.bid_price),
                        str(tick.ask_price),
                        str(tick.bid_volume),
                        str(tick.ask_volume),
                    )
                )
        if final_path.exists():
            raise FileExistsError("refusing to overwrite existing tick CSV output")
        os.replace(temporary_path, final_path)
        published = True
        return final_path
    finally:
        if not published:
            temporary_path.unlink(missing_ok=True)


def _load_or_download_tick_raw(
    instrument: str,
    requested_date: date,
    hour: int,
    *,
    output_root: Path,
    fetcher: Callable[[str], bytes],
    use_cache: bool = True,
) -> tuple[bytes, Path, bool]:
    normalized_instrument = validate_instrument(instrument)
    destination = tick_json_path(output_root, normalized_instrument, requested_date, hour)
    if use_cache and destination.exists():
        return destination.read_bytes(), destination, False
    return (
        fetcher(build_tick_endpoint_url(normalized_instrument, requested_date, hour)),
        destination,
        True,
    )


def run_tick_hour(
    instrument: str,
    requested_date: date,
    hour: int | str,
    *,
    output_root: Path,
    fetcher: Callable[[str], bytes] = download_json_bytes,
    output_format: str = "parquet",
    no_cache: bool = False,
) -> TickHourResult:
    """Reuse or download, validate, and publish one tick hour."""

    normalized_instrument = validate_instrument(instrument)
    normalized_hour = validate_hour(hour)
    normalized_format = validate_output_format(output_format)
    raw_bytes, json_destination, raw_was_downloaded = _load_or_download_tick_raw(
        normalized_instrument,
        requested_date,
        normalized_hour,
        output_root=Path(output_root),
        fetcher=fetcher,
        use_cache=not no_cache,
    )
    ticks = decode_tick_json_bytes(
        raw_bytes,
        requested_date=requested_date,
        hour=normalized_hour,
    )
    output_path = tick_output_path(
        Path(output_root), normalized_instrument, requested_date, normalized_hour, normalized_format
    )

    if not ticks:
        if raw_was_downloaded and not no_cache:
            _write_raw_json(raw_bytes, json_destination)
        return TickHourResult(
            requested_date=requested_date,
            hour=normalized_hour,
            json_path=json_destination,
            output_path=None,
            tick_count=0,
            raw_was_downloaded=raw_was_downloaded,
            parquet_created=False,
            parquet_skipped=False,
            output_format=normalized_format,
            cache_enabled=not no_cache,
        )

    if output_path.exists():
        if raw_was_downloaded and not no_cache:
            _write_raw_json(raw_bytes, json_destination)
        return TickHourResult(
            requested_date=requested_date,
            hour=normalized_hour,
            json_path=json_destination,
            output_path=output_path,
            tick_count=len(ticks),
            raw_was_downloaded=raw_was_downloaded,
            parquet_created=False,
            parquet_skipped=True,
            output_format=normalized_format,
            cache_enabled=not no_cache,
        )

    published_path: Path | None = None
    try:
        if normalized_format == "csv":
            published_path = write_ticks_csv(
                ticks,
                Path(output_root),
                normalized_instrument,
                requested_date,
                normalized_hour,
            )
        else:
            published_path = write_ticks_parquet(
                ticks,
                Path(output_root),
                normalized_instrument,
                requested_date,
                normalized_hour,
            )
        if raw_was_downloaded and not no_cache:
            _write_raw_json(raw_bytes, json_destination)
    except Exception:
        if published_path is not None:
            published_path.unlink(missing_ok=True)
        raise

    return TickHourResult(
        requested_date=requested_date,
        hour=normalized_hour,
        json_path=json_destination,
        output_path=published_path,
        tick_count=len(ticks),
        raw_was_downloaded=raw_was_downloaded,
        parquet_created=True,
        parquet_skipped=False,
        output_format=normalized_format,
        cache_enabled=not no_cache,
    )


def run_tick_date(
    instrument: str,
    requested_date: date,
    hours: Sequence[int | str] | None = None,
    *,
    output_root: Path,
    fetcher: Callable[[str], bytes] = download_json_bytes,
    output_format: str = "parquet",
    no_cache: bool = False,
) -> TickDateResult:
    """Process requested hours independently and retain hour failures."""

    normalized_hours = resolve_hours(None) if hours is None else tuple(validate_hour(hour) for hour in hours)
    if not normalized_hours:
        raise ValueError("at least one tick hour is required")

    outcomes: list[TickHourOutcome] = []
    for hour in normalized_hours:
        try:
            result = run_tick_hour(
                instrument,
                requested_date,
                hour,
                output_root=output_root,
                fetcher=fetcher,
                output_format=output_format,
                no_cache=no_cache,
            )
        except Exception as exc:
            outcomes.append(TickHourOutcome(requested_date, hour, None, str(exc)))
        else:
            outcomes.append(TickHourOutcome(requested_date, hour, result, None))
    return TickDateResult(requested_date, tuple(outcomes))
