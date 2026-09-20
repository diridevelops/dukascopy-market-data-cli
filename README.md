# Dukascopy market-data downloader CLI

A command-line tool for downloading historical market data from the [Dukascopy](https://www.dukascopy.com) public endpoint.

It offers functionality similar to Dukascopy's [Historical Data Export](https://www.dukascopy.com/swiss/english/marketwatch/historical/) webpage, with additional support for conveniently downloading arbitrary date ranges and integrating data retrieval into scripts and automated workflows.

> This project is not affiliated with or endorsed by Dukascopy.

The CLI provides two commands:

- `instruments` fetches the current Dukascopy instrument catalogue and prints one exact instrument code per line.
- `download` downloads compressed Dukascopy minute candles and/or hourly tick data, expands the deltas, and writes Hive-partitioned Parquet or CSV files.

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
│       ├── instruments.py
│       └── ticks.py
├── tests/
└── output/
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

## Download candles and ticks

Use the `download` subcommand for one date and one or more aggregations:

```bash
uv run dukascopy download \
  --instrument EUR-USD \
  --date 2026-09-13 \
  --aggregation 1,5,15
```

By default, derived data is written below `./output`. To write CSV instead of
Parquet, add `--csv`:

```bash
uv run dukascopy download \
  --instrument EUR-USD \
  --date 2026-09-13 \
  --aggregation 1,5,15 \
  --csv
```

Use `-o` or `--output` to select the exact output root. The path may be
relative or absolute; relative paths are resolved from the current working
directory, and missing parents are created:

```bash
uv run dukascopy download \
  --instrument EUR-USD \
  --date 2026-09-13 \
  --aggregation 15 \
  --output ./data
```

For example, an absolute home-directory path in Bash is also accepted:

```bash
uv run dukascopy download \
  --instrument EUR-USD \
  --date 2026-09-13 \
  --aggregation 15 \
  --output "$HOME/Downloads/data"
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
| `--aggregation` | One or more positive minute sizes separated by commas; required unless `--only-ticks` is used. | `1`, `1,5,15`, `60,240` |
| `--only-ticks` | Download only tick data. It cannot be combined with `--include-ticks` or `--aggregation`. | flag |
| `--include-ticks` | Download candles and all 24 tick hours for every requested date. | flag |
| `--hour` | Tick hour from `0` through `23`; valid only with `--only-ticks`. Without it, all hours are downloaded. | `1` |
| `--csv` | Write derived candle and tick data as CSV instead of Parquet. Raw JSON caches are unchanged. | flag |
| `--no-cache` | Ignore existing raw JSON and do not save downloaded JSON responses. Existing raw files remain unchanged. | flag |
| `-o`, `--output` | Exact output root. Relative paths use the current working directory; missing parents are created. | `./data` |

Use `--help` for the complete command syntax:

```bash
uv run dukascopy download --help
```

Tick-only downloads do not require candle aggregations:

```bash
uv run dukascopy download \
  --instrument EUR-USD \
  --date 2026-09-01 \
  --only-ticks

uv run dukascopy download \
  --instrument EUR-USD \
  --date 2026-09-01 \
  --only-ticks \
  --hour 1
```

To download candles and all 24 tick hours for each date, use
`--include-ticks`:

```bash
uv run dukascopy download \
  --instrument EUR-USD \
  --start-date 2026-09-01 \
  --end-date 2026-09-13 \
  --aggregation 1,5,15 \
  --include-ticks
```

Dates in a range are processed sequentially and independently. Each date has its own raw JSON cache and derived outputs. A failure for one date or tick hour is reported while later dates and hours continue processing; the final exit status is `1` if any date/hour failed and `0` if all requested work succeeded or was validly empty. Download logging reports created and skipped aggregations/hours without printing local filesystem paths. Before the final summary it lists the empty, failed, and fully skipped dates.

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

### Tick decoding

Tick responses are hourly compressed payloads. `times` are cumulative
millisecond deltas from the hourly `timestamp`; `bids` and `asks` are
cumulative price deltas applied with `multiplier`. Tick output keeps every
source tick and does not resample or aggregate it.

Tick Parquet columns are:

| Column | Type | Meaning |
| --- | --- | --- |
| `timestamp` | `timestamp[ms, tz=UTC]` | Source tick time |
| `bidPrice`, `askPrice` | `float64` | Best bid and ask prices |
| `bidVolume`, `askVolume` | `int64` | Quoted liquidity at the best bid and ask |

With `--csv`, tick files use the same five columns and order. CSV timestamps
are ISO-8601 UTC values with millisecond precision, such as
`2026-09-01T01:00:00.010Z`. CSV price and volume fields are written as decimal
and integer text respectively.

These volumes are quoted liquidity, not completed transaction volume:

- **`bidVolumes[i]`** is the quantity available from buyers at the current best bid.
- **`askVolumes[i]`** is the quantity available from sellers at the current best ask.

The candle `volume` and combined `bidVolume`/`askVolume` values are sums of
the corresponding bid or ask quoted-liquidity values for the candle. They do
not represent executed orders or traded volume. See the [Dukascopy ITick
documentation](https://www.dukascopy.com/client/javadoc/com/dukascopy/api/ITick.html)
for the API definition of best bid/ask volume.

## Output layout

By default, outputs are written below `output/`. The same layout is used for
CSV; only the derived filename extension changes from `.parquet` to `.csv`.
An explicit `--output PATH` replaces `output/` with the selected path:

```text
output/
└── instrument=EUR-USD/
    ├── json/
    │   ├── minute/
    │   │   └── year=2026/
    │   │       └── month=09/
    │   │           └── day=13/
    │   │               ├── EUR-USD-2026-09-13-BID.json
    │   │               └── EUR-USD-2026-09-13-ASK.json
    │   └── ticks/
    │       └── year=2026/
    │           └── month=09/
    │               └── day=13/
    │                   └── EUR-USD-2026-09-13-01-TICKS.json
    ├── tf=15m/
    │   └── year=2026/
    │       └── month=09/
    │           └── day=13/
    │               └── EUR-USD-2026-09-13-COMB.parquet
    └── tf=1tick/
        └── year=2026/
            └── month=09/
                └── day=13/
                    └── hour=01/
                        └── EUR-USD-2026-09-13-01-TICKS.parquet
```

For CSV output, the equivalent files are named
`EUR-USD-2026-09-13-COMB.csv` and
`EUR-USD-2026-09-13-01-TICKS.csv` in the same partition directories.
All candle and tick CSV timestamp columns use ISO-8601 UTC text with
millisecond precision.

`COMB` Parquet columns are:

| Column | Type | Meaning |
| --- | --- | --- |
| `timestamp` | `timestamp[ms, tz=UTC]` | Aggregate bucket start |
| `bidOpen`, `bidHigh`, `bidLow`, `bidClose` | `float64` | Aggregated BID prices |
| `askOpen`, `askHigh`, `askLow`, `askClose` | `float64` | Aggregated ASK prices |
| `bidVolume`, `askVolume` | `int64` | Summed side-specific volume in units |

COMB CSV files use the same eleven columns and order; only the timestamp is
serialized as ISO-8601 UTC text.

`BID` and `ASK` Parquet files retain the existing `open`, `high`, `low`,
`close`, and `volume` columns and include the corresponding side suffix in
their filenames. Their CSV equivalents use the same column names and order.

Hive partition directories use the UTC date of each aggregate bucket. Raw JSON
directories use the requested endpoint date. Tick derived-data directories use
the requested endpoint date and zero-padded hour. Raw JSON preserves the exact
validated response bytes. Tick JSON, Parquet, and CSV contain both bid and ask
data; `--side` does not select a tick-only side.

## Rerunning downloads

Raw JSON is a reusable source cache. Candle caches are keyed by instrument,
date, and side; tick caches are additionally keyed by hour. In `COMB` mode,
the BID and ASK JSON files are cached independently. If a new aggregation or
tick output is requested for an existing raw JSON file, the file is validated
locally and reused without another network request.

Use `--no-cache` when the invocation must fetch fresh source responses:

```bash
uv run dukascopy download \
  --instrument EUR-USD \
  --date 2026-09-13 \
  --aggregation 1,5,15 \
  --no-cache
```

No-cache mode bypasses existing candle and tick JSON files, including invalid
ones, and never saves the fresh response bytes. Existing raw JSON files are
preserved byte-for-byte. The fresh responses are still validated and used to
produce derived outputs in memory. This applies to single-side candles,
`COMB`, tick-only downloads, `--include-ticks`, and date ranges.

Existing target files in the selected format are skipped; missing outputs are
created. Parquet and CSV targets are independent, so an existing Parquet file
does not block a later CSV request and vice versa. Existing raw JSON and
derived files are never overwritten. In no-cache mode, existing derived files
are still skipped and are not overwritten. Refreshing source data in normal
mode requires
deliberately removing the relevant raw JSON and derived outputs before
rerunning. Invalid cached JSON is rejected rather than silently redownloaded
in normal mode; `--no-cache` does not read cached JSON and therefore ignores
it.

The current default layout is rooted at `output/`. Existing files under the
legacy `candles/` or former `artifacts/` directories are left untouched and
are not migrated or reused.

Valid empty candle days and tick hours are cached without creating derived
files in normal mode. In no-cache mode they are processed as empty work but
neither the raw JSON nor derived files are created. Empty work is reported as
successful rather than failed.

## Tests

Run the offline unit suite with the project environment:

```bash
uv run python -m unittest discover -s ./tests -p 'test_*.py' -v
```

To run the same suite with the active virtual environment:

```bash
uv run --active python -m unittest discover -s ./tests -p 'test_*.py' -v
```

The tests cover subcommand dispatch, no-subcommand help, instrument-code decoding and validation, exact dotted/mixed-case codes, candle and tick URL formation, compressed candle/tick decoding, empty days and hours, date ranges, cached-JSON reuse, strict no-cache behavior, BID/ASK/COMB modes, positional side matching, sparse aggregation, one-row-per-tick output, UTC Parquet schemas, ISO-8601 CSV output, configurable output roots, Hive partitions, failure continuation, format-specific collisions, and no-overwrite behavior.

## Common errors

- **Missing PyArrow**: install PyArrow into the interpreter used to run the command.
- **Invalid instrument code**: run `instruments` and copy the exact code, including case and dots.
- **Invalid side**: use `COMB`, `BID`, or `ASK`; omitting `--side` is equivalent to `COMB`.
- **Invalid tick flags**: `--only-ticks` must omit `--aggregation`; `--include-ticks` requires `--aggregation`; `--hour` is valid only with `--only-ticks`.
- **Existing output**: the requested aggregation or tick hour is skipped when the same format already exists; Parquet and CSV outputs are independent.
- **Output location**: the default is `./output`; `--output` creates missing parents but returns an error if the location is a file or cannot be written.
- **Invalid cached JSON**: normal mode rejects malformed or contract-invalid cached data instead of redownloading it; use `--no-cache` to fetch fresh data without reading or replacing that cache.
- **Empty date**: the endpoint returned a valid response with no candles; this is reported and creates no derived output.
- **Empty tick hour**: the endpoint returned a valid empty hourly response; its JSON is cached in normal mode, while `--no-cache` leaves no raw cache, and no tick derived file is created.
- **Range failure**: the failed date is reported, other dates continue, and the process exits with status `1`.
- **HTTP or connection error**: check network access. Transient connection failures and selected HTTP statuses are retried automatically.
