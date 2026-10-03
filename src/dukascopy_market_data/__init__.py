"""Dukascopy market-data downloader package."""

from .candles import (
    CombinedAggregatedCandle,
    CombinedDownloadBatchResult,
    DataValidationError,
    DateRunOutcome,
    DownloadBatchResult,
    DownloadError,
    download_combined_candles,
    run_date_range,
    run_downloads,
)
from .ticks import TickDateResult, TickHourOutcome, TickHourResult, run_tick_date, run_tick_hour
from .cli import main

__all__ = [
    "CombinedAggregatedCandle",
    "CombinedDownloadBatchResult",
    "DataValidationError",
    "DateRunOutcome",
    "DownloadBatchResult",
    "DownloadError",
    "TickDateResult",
    "TickHourOutcome",
    "TickHourResult",
    "download_combined_candles",
    "main",
    "run_date_range",
    "run_downloads",
    "run_tick_date",
    "run_tick_hour",
]
