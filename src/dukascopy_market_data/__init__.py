"""Dukascopy market-data downloader package."""

from .candles import (
    CombinedDownloadBatchResult,
    DataValidationError,
    DateRunOutcome,
    DownloadBatchResult,
    DownloadError,
    run_date_range,
    run_downloads,
)
from .ticks import TickDateResult, TickHourOutcome, TickHourResult, run_tick_date, run_tick_hour
from .cli import main

__all__ = [
    "CombinedDownloadBatchResult",
    "DataValidationError",
    "DateRunOutcome",
    "DownloadBatchResult",
    "DownloadError",
    "TickDateResult",
    "TickHourOutcome",
    "TickHourResult",
    "main",
    "run_date_range",
    "run_downloads",
    "run_tick_date",
    "run_tick_hour",
]
