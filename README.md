# Dukascopy market-data CLI

This project provides two commands:

- `instruments` fetches the current Dukascopy instrument catalogue and prints one exact instrument code per line.
- `download` downloads compressed Dukascopy minute candles, expands the deltas, aggregates them, and writes Hive-partitioned Parquet files.

The project declares PyArrow in `pyproject.toml`. Use `uv run` for the project environment, or `uv run --active` when you want to use the currently active virtual environment.

## Project layout

```text
/
├── pyproject.toml
├── README.md
├── src/
│   ├── dukascopy.py
│   └── dukascopy_market_data/
│       ├── candles.py
│       ├── cli.py
│       └── instruments.py
├── tests/
└── candles/
```

Run commands from the root directory:

`uv run` uses the project environment. `uv run --active` prefers the currently active virtual environment; see the [uv CLI documentation](https://docs.astral.sh/uv/reference/cli/).

For example:

```bash
uv run dukascopy instruments
uv run --active dukascopy instruments
```

## Help and subcommands

Running without a subcommand prints the available commands and exits successfully:

```bash
uv run dukascopy
```

The same help is available with:

```bash
uv run dukascopy --help
```

## List instrument codes

Fetch the live instrument catalogue and print only sorted, unique codes:

```bash
uv run dukascopy instruments
```

The output is intended to be piped to `grep`:

```bash
uv run dukascopy instruments | grep -i '^EUR'
uv run dukascopy instruments | grep -i 'USD'
```

Codes must be copied exactly, including case and dots. Examples include `EUR-USD`, `A.US-USD`, `0005.HK-HKD`, and `DUKplus-EUR`. The command makes a fresh request each time and does not create a local catalogue cache. Network or malformed-response errors are written to stderr and return exit status `1`.

## Download candles

Use the `download` subcommand for one date and one or more aggregations:

```bash
uv run dukascopy download \
  --instrument EUR-USD \
  --date 2026-09-13 \
  --aggregation 1,5,15
```

When `--side` is omitted, it defaults to `COMB` and downloads or reuses both
the BID and ASK source responses. To request a mode explicitly:

```bash
uv run dukascopy download --instrument EUR-USD --side COMB --date 2026-09-13 --aggregation 1,5,15
uv run dukascopy download --instrument EUR-USD --side BID --date 2026-09-13 --aggregation 1,5,15
uv run dukascopy download --instrument EUR-USD --side ASK --date 2026-09-13 --aggregation 1,5,15
```

For an inclusive date range:

```bash
uv run dukascopy download \
  --instrument EUR-USD \
  --start-date 2026-09-01 \
  --end-date 2026-09-13 \
  --aggregation 1,5,15
```

The download arguments are:

| Argument | Description | Example |
| --- | --- | --- |
| `--instrument` | Exact code from the `instruments` command. | `EUR-USD` |
| `--side` | Output mode: `COMB`, `BID`, or `ASK`. `COMB` is the default. | `COMB` |
| `--date` | One endpoint date in `YYYY-MM-DD` format. | `2026-09-13` |
| `--start-date` and `--end-date` | Inclusive range; both are required together and cannot be combined with `--date`. | `2026-09-01` / `2026-09-13` |
| `--aggregation` | One or more positive minute sizes separated by commas. | `1`, `1,5,15`, `60,240` |

Use `--help` for the complete command syntax:

```bash
uv run dukascopy download --help
```

Dates in a range are processed sequentially and independently. Each date has its own raw JSON cache and Parquet outputs. A failure for one date is reported while later dates continue processing; the final exit status is `1` if any date failed and `0` if every date succeeded or was a valid empty day. Download logging reports created and skipped aggregations without printing local filesystem paths. Before the final summary it lists the empty, failed, and fully skipped dates.

## Decoding and aggregation

For each endpoint response, the downloader:

1. Accumulates `times` and multiplies them by `shift` to recover candle timestamps.
2. Applies cumulative OHLC deltas using `multiplier`.
3. Converts volumes from millions to exact integer units. For example, `1.62` becomes `1,620,000`.
4. Aligns aggregation buckets to UTC epoch boundaries. A 15-minute candle starts at `:00`, `:15`, `:30`, or `:45`.
5. Uses the first open, maximum high, minimum low, last close, and summed volume for each non-empty bucket.

In `COMB` mode, BID and ASK responses must contain the same number of candles
with matching timestamps at every position. The corresponding candles are
aggregated independently and written together. `BID` and `ASK` modes retain
the single-side schema.

Missing source minutes do not create synthetic candles. Partial final buckets are retained when they contain source data. Dates are never merged with neighboring dates.

## Output layout

Outputs are written below `market-data/candles/`:

```text
candles/
├── minute/
│   └── json/
│       ├── EUR-USD-2026-09-13-BID.json
│       └── EUR-USD-2026-09-13-ASK.json
└── 15m/
    └── year=2026/
        └── month=09/
            └── day=13/
                └── EUR-USD-2026-09-13-COMB.parquet
```

`COMB` Parquet columns are:

| Column | Type | Meaning |
| --- | --- | --- |
| `timestamp` | `timestamp[ms, tz=UTC]` | Aggregate bucket start |
| `bidOpen`, `bidHigh`, `bidLow`, `bidClose` | `float64` | Aggregated BID prices |
| `askOpen`, `askHigh`, `askLow`, `askClose` | `float64` | Aggregated ASK prices |
| `bidVolume`, `askVolume` | `int64` | Summed side-specific volume in units |

`BID` and `ASK` Parquet files retain the existing `open`, `high`, `low`,
`close`, and `volume` columns and include the corresponding side suffix in
their filenames.

Hive partition directories use the UTC date of each aggregate bucket. The raw JSON preserves the exact validated response bytes.

## Rerunning downloads

Raw JSON is a reusable source cache keyed by instrument, date, and side. In
`COMB` mode, the BID and ASK JSON files are cached independently. If a new
aggregation is requested for an existing raw JSON file, the file is validated
locally and reused without another network request.

Existing target Parquet aggregations are skipped; missing aggregations are created. Existing raw JSON and Parquet files are never overwritten. Refreshing source data requires deliberately removing the relevant raw JSON and derived outputs before rerunning. Invalid cached JSON is rejected rather than silently redownloaded.

Valid empty endpoint responses, such as weekends, are cached without creating Parquet files and are reported as empty dates.

## Tests

Run the offline unit suite with the project environment:

```bash
uv run python -m unittest discover -s ./tests -p 'test_*.py' -v
```

To run the same suite with the active virtual environment:

```bash
uv run --active python -m unittest discover -s ./tests -p 'test_*.py' -v
```

The tests cover subcommand dispatch, no-subcommand help, instrument-code decoding and validation, exact dotted/mixed-case codes, URL formation, compressed candle decoding, empty days, date ranges, cached-JSON reuse, BID/ASK/COMB modes, positional side matching, sparse aggregation, UTC Parquet schema, Hive partitions, failure continuation, and no-overwrite behavior.

## Common errors

- **Missing PyArrow**: install PyArrow into the interpreter used to run the command.
- **Invalid instrument code**: run `instruments` and copy the exact code, including case and dots.
- **Invalid side**: use `COMB`, `BID`, or `ASK`; omitting `--side` is equivalent to `COMB`.
- **Existing Parquet output**: the requested aggregation is skipped; no overwrite occurs.
- **Invalid cached JSON**: the source cache is malformed or violates the endpoint contract; remove it deliberately before retrying a fresh download.
- **Empty date**: the endpoint returned a valid response with no candles; this is reported and creates no Parquet output.
- **Range failure**: the failed date is reported, other dates continue, and the process exits with status `1`.
- **HTTP or connection error**: check network access. Transient connection failures and selected HTTP statuses are retried automatically.
