"""Fetch and validate the current Dukascopy instrument catalogue."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from typing import Any

from .candles import DataValidationError, download_json_bytes, validate_instrument


INSTRUMENTS_URL = "https://jetta.dukascopy.com/v1/instruments"


def _decode_utf8_json(raw_bytes: bytes) -> Any:
    try:
        text = raw_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise DataValidationError("instrument response is not valid UTF-8 JSON") from exc
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise DataValidationError(f"instrument response is not valid JSON: {exc.msg}") from exc


def decode_instrument_codes(raw_bytes: bytes) -> tuple[str, ...]:
    """Decode, validate, sort, and deduplicate instrument codes."""

    payload = _decode_utf8_json(raw_bytes)
    if not isinstance(payload, Mapping):
        raise DataValidationError("instrument response must be a JSON object")

    instruments = payload.get("instruments")
    if not isinstance(instruments, list):
        raise DataValidationError("instrument response field 'instruments' must be an array")

    codes: set[str] = set()
    for index, item in enumerate(instruments):
        if not isinstance(item, Mapping):
            raise DataValidationError(f"instruments[{index}] must be a JSON object")
        code = item.get("code")
        if not isinstance(code, str) or not code:
            raise DataValidationError(
                f"instruments[{index}].code must be a non-empty string"
            )
        if code != code.strip():
            raise DataValidationError(
                f"instruments[{index}].code must not contain leading or trailing whitespace"
            )
        try:
            codes.add(validate_instrument(code))
        except ValueError as exc:
            raise DataValidationError(
                f"instruments[{index}].code is not a valid instrument code: {code!r}"
            ) from exc
    return tuple(sorted(codes))


def fetch_instrument_codes(
    fetcher: Callable[[str], bytes] = download_json_bytes,
) -> tuple[str, ...]:
    """Fetch the live instrument catalogue and return its usable codes."""

    return decode_instrument_codes(fetcher(INSTRUMENTS_URL))
