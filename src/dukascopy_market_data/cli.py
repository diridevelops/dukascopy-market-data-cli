"""Command-line interface for the Dukascopy market-data tools."""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path
from typing import Callable, Sequence

from .candles import (
    download_json_bytes,
    parse_date,
    resolve_requested_dates,
    run_date_range,
    validate_aggregations,
    validate_download_side,
    validate_instrument,
)
from .instruments import fetch_instrument_codes


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
        required=True,
        type=_aggregations_argument,
        help="comma-separated positive aggregation sizes in minutes, for example 1,5,15",
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
        help="download and aggregate compressed minute candles",
        description="Download and aggregate compressed Dukascopy minute candles.",
    )
    _add_download_arguments(download_parser)
    return parser


def _print_download_results(outcomes) -> int:
    for outcome in outcomes:
        date_label = outcome.requested_date.isoformat()
        if outcome.error is not None:
            print(f"[{date_label}] ERROR: {outcome.error}", file=sys.stderr)
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

    resolved_output_root = Path.cwd() if output_root is None else Path(output_root)
    outcomes = run_date_range(
        arguments.instrument,
        arguments.side,
        requested_dates,
        arguments.aggregation,
        output_root=resolved_output_root,
        fetcher=fetcher,
    )
    return _print_download_results(outcomes)
