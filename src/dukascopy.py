"""Direct source-tree launcher for the Dukascopy CLI."""

from pathlib import Path

from dukascopy_market_data.cli import main


if __name__ == "__main__":
    raise SystemExit(main(output_root=Path(__file__).resolve().parents[1]))
