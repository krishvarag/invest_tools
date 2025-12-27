"""
Helper module to read Fidelity dividend CSV files and report individual stock dividends and reinvestments.
Fidelity CSV Format fields
    Run Date,Action,Symbol,Description,Type,Price ($),
    Quantity,Commission ($),Fees ($),Accrued Interest ($),Amount ($),
    Cash Balance ($),Settlement Date
"""

import pandas as pd
import os
import logging
import click
from enum import Enum

logger = logging.getLogger(__name__)
REPORTS = ["sum", "symbols", "details", "print", "all"]

class Action(Enum):
    DIVIDEND = "dividend"
    INVESTMENT = "investment"

report_map = {
    "sum": lambda divs: print(divs[["Run Date", "Symbol", "Amount ($)"]]),
    "symbols": lambda divs: print(divs["Symbol"].unique().tolist()),
    "details": lambda divs: print(divs),
    "print": lambda divs: print(divs[["Run Date", "Symbol", "Amount ($)"]]),
    "all": lambda divs: print(divs[["Run Date", "Symbol", "Amount ($)"]].to_string()),
}
REPORTS = list(report_map.keys())


class Dividend:
    def __init__(self, file_path, action: Action, symbol=None):
        self.file_path = file_path
        self.symbol = symbol
        self.action = action

    def __enter__(self):
        """
        Enter the context manager and load filtered dividend records from a CSV file.

        This method is called when entering a 'with' statement. It performs the following operations:
        1. Validates that the CSV file exists at the specified file path
        2. Reads the CSV file into a pandas DataFrame
        3. Cleans column names by stripping whitespace
        4. Converts the 'Amount ($)' column to numeric values, coercing errors to NaN
        5. Filters records to include only those with 'DIVIDEND' in the Action column
        6. Excludes records with negative amounts
        7. Optionally filters by symbol (if specified) using case-insensitive matching

        Returns:
            pd.DataFrame: A filtered DataFrame containing dividend records that match the criteria.

        Raises:
            FileNotFoundError: If the CSV file does not exist at self.file_path.
            RuntimeError: If the CSV file has an invalid format or cannot be parsed.

        Logs:
            - DEBUG: Entry message with file path and number of loaded dividend records
        """
        logger.debug(f"Entering Dividend context manager for file: {self.file_path}")
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"File not found: {self.file_path}")
        try:
            if self.action == Action.DIVIDEND:
                df = self._load_and_filter_dividends()
            else:
                df = self._load_and_filter_investments()
            logger.debug(f"Loaded {len(df)} dividend records")
            return df
        except (pd.errors.EmptyDataError, pd.errors.ParserError) as e:
            raise RuntimeError(f"Invalid CSV format in {self.file_path}") from e

    def _load_and_filter_dividends(self) -> pd.DataFrame:
        df = pd.read_csv(self.file_path)
        df.columns = df.columns.str.strip()
        df["Amount ($)"] = pd.to_numeric(df["Amount ($)"], errors="coerce")

        mask = (df["Action"].str.contains("DIVIDEND", case=False, na=False)) & (
            df["Amount ($)"] >= 0
        )
        if self.symbol:
            mask &= df["Symbol"] == self.symbol.upper()
        return df[mask]

    def _load_and_filter_investments(self) -> pd.DataFrame:
        df = pd.read_csv(self.file_path)
        df.columns = df.columns.str.strip()
        df["Amount ($)"] = pd.to_numeric(df["Amount ($)"], errors="coerce")

        mask = df["Action"].str.contains("REINVESTMENT", case=False, na=False)
        if self.symbol:
            mask &= df["Symbol"] == self.symbol.upper()
        return df[mask]

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            logger.debug("Exiting Dividend context manager successfully.")
            return True
        else:
            logger.error(
                f"Exiting context manager with exception: {exc_type.__name__}: {exc_value}"
            )
        return False  # Always propagate exceptions; do not suppress


def dividend_csv_process(
    report: str, action: Action, file_path: str, symbol: str = None
):
    print(f"Processing file: {file_path} for action: {action.value} with report: {report}")
    def sum(divs):
        if divs.empty:
            print("No dividends found.")
            return
        symbols = divs.groupby("Symbol")
        for sym, group in symbols:            
            total_amount = group["Amount ($)"].sum()
            #print(group[["Run Date", "Symbol", "Amount ($)"]])
            print(f"Total Amount for {sym}: ${total_amount:.2f}")


    def symbols_details(divs):
        print("Available Symbols:")
        symbols = divs["Symbol"].unique()
        for sym in symbols:
            print(f"{sym!s}")

    def details(divs):
        symbols = divs.groupby("Symbol")
        for sym, group in symbols:
            print(f"\nSymbol: {sym}")
            print(group[["Run Date", "Symbol", "Amount ($)"]])
            total_amount = group["Amount ($)"].sum()
            print(f"Total Amount for {sym}: ${total_amount:.2f}")

    # OVERRIDE global action_map
    report_map["sum"] = sum
    report_map["details"] = details
    report_map["symbols"] = symbols_details
    with Dividend(file_path, action, symbol) as divs:
        if divs is None:
            print("No data available.")
            return
        if report in report_map:
            report_map[report](divs)
        else:
            print(f"Unknown report: {report}")
    # Context manager will automatically call __exit__ here


@click.command()
@click.argument("report", type=click.Choice(REPORTS), default="sum")
@click.argument(
    "action", type=click.Choice([a.value for a in Action]), default="dividend"
)
@click.argument("file_path", type=click.Path(exists=True), default="2025d.csv")
@click.option("--symbol", default=None, help="Optional symbol filter for dividends")
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"]),
    default="INFO",
    help="Set logging level",
)
def main(action, report, file_path, symbol, log_level):
    """Process dividend data from Fidelity CSV files."""
    logging.basicConfig(level=getattr(logging, log_level))
    dividend_csv_process(report, Action(action), file_path, symbol)


if __name__ == "__main__":
    main()
