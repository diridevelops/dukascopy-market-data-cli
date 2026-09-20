"""Command-line interface for the Dukascopy market-data tools."""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Callable, Sequence

from .candles import (
    download_json_bytes,
    parse_date,
    resolve_requested_dates,
    run_date_range,
    run_downloads,
    validate_aggregations,
    validate_download_side,
    validate_instrument,
)
from .instruments import fetch_instrument_codes
from .ticks import TickDateResult, run_tick_date, resolve_hours, validate_hour


def _date_argument(value: str) -> date:
    try:
        return parse_date(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _instrument_argument(value: str) -> str:
    try:
        return validate_instrument(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _side_argument(value: str) -> str:
    try:
        return validate_download_side(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _aggregations_argument(value: str) -> tuple[int, ...]:
    try:
        return validate_aggregations(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _hour_argument(value: str) -> int:
    try:
        return validate_hour(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _add_download_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--instrument", required=True, type=_instrument_argument)
    parser.add_argument(
        "--side",
        default="COMB",
        type=_side_argument,
        help="output mode: BID, ASK, or COMB (default: COMB)",
    )
    date_group = parser.add_mutually_exclusive_group()
    date_group.add_argument("--date", type=_date_argument, dest="requested_date")
    date_group.add_argument("--start-date", type=_date_argument)
    parser.add_argument("--end-date", type=_date_argument)
    parser.add_argument(
        "--aggregation",
        type=_aggregations_argument,
        help="comma-separated positive aggregation sizes in minutes; required for candles",
    )
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--only-ticks",
        action="store_true",
        help="download only hourly ticks; --aggregation is not used",
    )
    mode_group.add_argument(
        "--include-ticks",
        action="store_true",
        help="download candles and all 24 tick hours for each date",
    )
    parser.add_argument(
        "--hour",
        type=_hour_argument,
        help="tick hour 0-23; valid only with --only-ticks (default: all hours)",
    )
    parser.add_argument(
        "--csv",
        action="store_true",
        help="write derived candle and tick data as CSV instead of Parquet",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="bypass raw JSON caches and do not save downloaded JSON responses",
    )
    parser.add_argument(
        "-o",
        "--output",
        metavar="PATH",
        help="exact output root; missing directories and parents are created",
    )


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Download Dukascopy candles or list available instrument codes."
    )
    subparsers = parser.add_subparsers(dest="command", metavar="COMMAND")

    subparsers.add_parser(
        "instruments",
        help="print all current instrument codes, one per line",
        description="Fetch and print all current Dukascopy instrument codes.",
    )

    download_parser = subparsers.add_parser(
        "download",
        help="download Dukascopy candles and/or hourly ticks",
        description="Download Dukascopy candles and/or compressed hourly ticks.",
    )
    _add_download_arguments(download_parser)
    return parser


def _print_download_results(outcomes) -> int:
    for outcome in outcomes:
        date_label = outcome.requested_date.isoformat()
        if outcome.error is not None:
            print(f"[{date_label}] ERROR: {_path_free_error_text(outcome.error)}", file=sys.stderr)
            continue

        result = outcome.result
        assert result is not None
        source_message = result.source_message
        if result.minute_count == 0:
            print(f"[{date_label}] {source_message}; no candles (empty date)")
            continue

        print(f"[{date_label}] {source_message} and decoded {result.minute_count} minute candles")
        if result.created_aggregations:
            created = ", ".join(f"{aggregation}m" for aggregation in result.created_aggregations)
            print(f"[{date_label}] Created aggregations: {created}")
        if result.skipped_aggregations:
            skipped = ", ".join(f"{aggregation}m" for aggregation in result.skipped_aggregations)
            print(f"[{date_label}] Skipped existing aggregations: {skipped}")

    failed_count = sum(outcome.error is not None for outcome in outcomes)
    empty_dates = [
        outcome.requested_date.isoformat()
        for outcome in outcomes
        if outcome.result is not None and outcome.result.minute_count == 0
    ]
    failed_dates = [
        outcome.requested_date.isoformat() for outcome in outcomes if outcome.error is not None
    ]
    skipped_dates = [
        outcome.requested_date.isoformat()
        for outcome in outcomes
        if outcome.result is not None
        and outcome.result.minute_count > 0
        and not outcome.result.created_aggregations
        and bool(outcome.result.skipped_aggregations)
    ]
    empty_count = len(empty_dates)
    skipped_date_count = len(skipped_dates)
    successful_count = len(outcomes) - failed_count - empty_count - skipped_date_count
    created_count = sum(
        len(outcome.result.created_aggregations)
        for outcome in outcomes
        if outcome.result is not None
    )
    skipped_count = sum(
        len(outcome.result.skipped_aggregations)
        for outcome in outcomes
        if outcome.result is not None
    )
    print(f"Empty days: {', '.join(empty_dates) if empty_dates else 'none'}")
    print(f"Failed days: {', '.join(failed_dates) if failed_dates else 'none'}")
    print(f"Skipped days: {', '.join(skipped_dates) if skipped_dates else 'none'}")
    print(
        "Summary: "
        f"processed={len(outcomes)} "
        f"successful={successful_count} "
        f"empty={empty_count} "
        f"failed={failed_count} "
        f"skipped={skipped_date_count} "
        f"created_aggregations={created_count} "
        f"skipped_aggregations={skipped_count}"
    )
    return 1 if failed_count else 0


@dataclass(frozen=True)
class _ExtendedDateOutcome:
    """One date result for candle-plus-tick or tick-only processing."""

    requested_date: date
    candle_result: object | None
    tick_result: TickDateResult | None
    error: str | None


def _path_free_error_text(message: str) -> str:
    """Keep local filesystem paths out of user-facing status logs."""

    lowered = message.lower()
    if "refusing to overwrite existing" in lowered:
        return "existing output artifact"
    if "permission denied" in lowered or "access is denied" in lowered:
        return "filesystem permission error while writing output"
    if (
        "cannot find" in lowered
        or "no such file" in lowered
        or "not a directory" in lowered
    ):
        return "filesystem path error while writing output"
    return message


def _resolve_output_root(output_option: str | None, output_override: Path | None) -> Path:
    """Resolve the exact output root used by a download invocation."""

    if output_option is not None:
        candidate = Path(output_option).expanduser()
    elif output_override is not None:
        candidate = Path(output_override).expanduser()
    else:
        candidate = Path.cwd() / "output"
    if not candidate.is_absolute():
        candidate = Path.cwd() / candidate
    return candidate.resolve()


def _ensure_output_root(path: Path) -> Path:
    """Create and write-probe an output root before starting network work."""

    try:
        if path.exists() and not path.is_dir():
            raise OSError("output location is not a directory")
        path.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(prefix=".output-write-test-", dir=path)
        os.close(descriptor)
        Path(temporary_name).unlink(missing_ok=True)
    except OSError as exc:
        raise OSError("output location cannot be written") from exc
    return path


def _run_extended_date_range(
    instrument: str,
    side: str,
    requested_dates: Sequence[date],
    aggregation: tuple[int, ...] | None,
    *,
    include_ticks: bool,
    only_ticks: bool,
    tick_hours: Sequence[int],
    output_root: Path,
    fetcher: Callable[[str], bytes],
    output_format: str,
    no_cache: bool,
) -> tuple[_ExtendedDateOutcome, ...]:
    outcomes: list[_ExtendedDateOutcome] = []
    for requested_date in requested_dates:
        candle_result = None
        tick_result = None
        errors: list[str] = []

        if not only_ticks:
            assert aggregation is not None
            try:
                candle_result = run_downloads(
                    instrument,
                    side,
                    requested_date,
                    aggregation,
                    output_root=output_root,
                    fetcher=fetcher,
                    output_format=output_format,
                    no_cache=no_cache,
                )
            except Exception as exc:
                errors.append(f"candles: {_path_free_error_text(str(exc))}")

        if only_ticks or include_ticks:
            try:
                tick_result = run_tick_date(
                    instrument,
                    requested_date,
                    tick_hours,
                    output_root=output_root,
                    fetcher=fetcher,
                    output_format=output_format,
                    no_cache=no_cache,
                )
            except Exception as exc:
                errors.append(f"ticks: {_path_free_error_text(str(exc))}")
            else:
                for failed_hour in tick_result.failed_hours:
                    assert failed_hour.error is not None
                    errors.append(
                        f"ticks hour {failed_hour.hour:02d}: "
                        f"{_path_free_error_text(failed_hour.error)}"
                    )

        outcomes.append(
            _ExtendedDateOutcome(
                requested_date=requested_date,
                candle_result=candle_result,
                tick_result=tick_result,
                error="; ".join(errors) if errors else None,
            )
        )
    return tuple(outcomes)


def _print_extended_download_results(outcomes: Sequence[_ExtendedDateOutcome]) -> int:
    for outcome in outcomes:
        date_label = outcome.requested_date.isoformat()
        if outcome.error is not None:
            print(f"[{date_label}] ERROR: {_path_free_error_text(outcome.error)}", file=sys.stderr)

        candle_result = outcome.candle_result
        if candle_result is not None:
            if candle_result.minute_count == 0:
                print(f"[{date_label}] {candle_result.source_message}; no candles (empty date)")
            else:
                print(
                    f"[{date_label}] {candle_result.source_message} and decoded "
                    f"{candle_result.minute_count} minute candles"
                )
                if candle_result.created_aggregations:
                    created = ", ".join(
                        f"{aggregation}m" for aggregation in candle_result.created_aggregations
                    )
                    print(f"[{date_label}] Created aggregations: {created}")
                if candle_result.skipped_aggregations:
                    skipped = ", ".join(
                        f"{aggregation}m" for aggregation in candle_result.skipped_aggregations
                    )
                    print(f"[{date_label}] Skipped existing aggregations: {skipped}")

        tick_result = outcome.tick_result
        if tick_result is not None:
            print(
                f"[{date_label}] Ticks: decoded={tick_result.tick_count} "
                f"created_hours={len(tick_result.created_hours)} "
                f"skipped_hours={len(tick_result.skipped_hours)} "
                f"empty_hours={len(tick_result.empty_hours)} "
                f"failed_hours={len(tick_result.failed_hours)}"
            )

    failed_dates = [
        outcome.requested_date.isoformat() for outcome in outcomes if outcome.error is not None
    ]
    empty_dates: list[str] = []
    skipped_dates: list[str] = []
    created_aggregation_count = 0
    skipped_aggregation_count = 0
    created_tick_hour_count = 0
    skipped_tick_hour_count = 0

    for outcome in outcomes:
        candle_result = outcome.candle_result
        tick_result = outcome.tick_result
        if candle_result is not None:
            created_aggregation_count += len(candle_result.created_aggregations)
            skipped_aggregation_count += len(candle_result.skipped_aggregations)
        if tick_result is not None:
            created_tick_hour_count += len(tick_result.created_hours)
            skipped_tick_hour_count += len(tick_result.skipped_hours)

        if outcome.error is not None:
            continue

        has_candle_data = candle_result is not None and candle_result.minute_count > 0
        has_tick_data = tick_result is not None and tick_result.has_data
        if not has_candle_data and not has_tick_data:
            empty_dates.append(outcome.requested_date.isoformat())
            continue

        has_created_output = (
            candle_result is not None and bool(candle_result.created_aggregations)
        ) or (tick_result is not None and bool(tick_result.created_hours))
        has_skipped_output = (
            candle_result is not None and bool(candle_result.skipped_aggregations)
        ) or (tick_result is not None and bool(tick_result.skipped_hours))
        if not has_created_output and has_skipped_output:
            skipped_dates.append(outcome.requested_date.isoformat())

    empty_count = len(empty_dates)
    failed_count = len(failed_dates)
    skipped_count = len(skipped_dates)
    successful_count = len(outcomes) - empty_count - failed_count - skipped_count
    print(f"Empty days: {', '.join(empty_dates) if empty_dates else 'none'}")
    print(f"Failed days: {', '.join(failed_dates) if failed_dates else 'none'}")
    print(f"Skipped days: {', '.join(skipped_dates) if skipped_dates else 'none'}")
    print(
        "Summary: "
        f"processed={len(outcomes)} "
        f"successful={successful_count} "
        f"empty={empty_count} "
        f"failed={failed_count} "
        f"skipped={skipped_count} "
        f"created_aggregations={created_aggregation_count} "
        f"skipped_aggregations={skipped_aggregation_count} "
        f"created_tick_hours={created_tick_hour_count} "
        f"skipped_tick_hours={skipped_tick_hour_count}"
    )
    return 1 if failed_count else 0


def main(
    argv: Sequence[str] | None = None,
    *,
    output_root: Path | None = None,
    fetcher: Callable[[str], bytes] = download_json_bytes,
) -> int:
    parser = build_argument_parser()
    arguments = parser.parse_args(argv)

    if arguments.command is None:
        parser.print_help()
        return 0

    if arguments.command == "instruments":
        try:
            for code in fetch_instrument_codes(fetcher):
                print(code)
        except Exception as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 1
        return 0

    try:
        requested_dates = resolve_requested_dates(
            arguments.requested_date,
            arguments.start_date,
            arguments.end_date,
        )
    except ValueError as exc:
        parser.error(str(exc))

    if arguments.only_ticks:
        if arguments.aggregation is not None:
            parser.error("--aggregation cannot be combined with --only-ticks")
        if arguments.hour is None:
            tick_hours = resolve_hours(None)
        else:
            tick_hours = (arguments.hour,)
    else:
        if arguments.aggregation is None:
            parser.error("--aggregation is required unless --only-ticks is used")
        if arguments.hour is not None:
            parser.error("--hour is valid only with --only-ticks")
        tick_hours = resolve_hours(None)

    output_format = "csv" if arguments.csv else "parquet"
    try:
        resolved_output_root = _ensure_output_root(
            _resolve_output_root(arguments.output, output_root)
        )
    except OSError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    if arguments.only_ticks or arguments.include_ticks:
        outcomes = _run_extended_date_range(
            arguments.instrument,
            arguments.side,
            requested_dates,
            arguments.aggregation,
            include_ticks=arguments.include_ticks,
            only_ticks=arguments.only_ticks,
            tick_hours=tick_hours,
            output_root=resolved_output_root,
            fetcher=fetcher,
            output_format=output_format,
            no_cache=arguments.no_cache,
        )
        return _print_extended_download_results(outcomes)

    outcomes = run_date_range(
        arguments.instrument,
        arguments.side,
        requested_dates,
        arguments.aggregation,
        output_root=resolved_output_root,
        fetcher=fetcher,
        output_format=output_format,
        no_cache=arguments.no_cache,
    )
    return _print_download_results(outcomes)
