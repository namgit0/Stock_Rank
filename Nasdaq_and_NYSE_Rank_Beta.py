"""
Weekly Stock Performance Scraper — 52 Weeks Historical
Automatically fetches all NASDAQ and NYSE stocks with market cap > $20B,
then pulls weekly OHLCV data and % price change (close-to-close) for 52 weeks.

Install dependencies:
    pip install yfinance pandas requests

Run:
    python test_script.py
"""

import pandas as pd
import requests
import time
from datetime import datetime, timedelta

# ─────────────────────────────────────────────
# SETTINGS
# ─────────────────────────────────────────────
NUM_WEEKS          = 52
MIN_MARKET_CAP_USD = 20_000_000_000    # $20 billion
SEPARATOR_WIDTH    = 65  # named constant instead of magic number


# ─────────────────────────────────────────────
# FALLBACK LIST — used only if live NASDAQ/NYSE API fetch fails
# Representative sample of large-cap NASDAQ and NYSE stocks
# ─────────────────────────────────────────────
FALLBACK_TICKERS = [
    # NASDAQ
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AVGO", "COST", "NFLX",
    "AMD", "QCOM", "INTU", "CSCO", "TMUS", "TXN", "AMGN", "AMAT", "ISRG", "BKNG",
    "VRTX", "ADP", "ADI", "MU", "REGN", "PANW", "GILD", "LRCX", "KLAC", "MDLZ",
    "SBUX", "MELI", "INTC", "CDNS", "SNPS", "PYPL", "MRVL", "ADSK", "WDAY", "FTNT",
    "CRWD", "DDOG", "SNPS", "TTD", "TEAM", "HUBS", "NET", "MDB", "COIN", "APP",
    # NYSE
    "BRK-B", "JPM", "LLY", "V", "UNH", "XOM", "MA", "JNJ", "PG", "HD",
    "ABBV", "BAC", "MRK", "CVX", "WMT", "CRM", "KO", "ACN", "PEP", "TMO",
    "LIN", "MCD", "ABT", "DHR", "GE", "ADBE", "PM", "CAT", "IBM", "MS",
    "GS", "SPGI", "RTX", "T", "VZ", "HON", "NOW", "DE", "UBER", "PFE",
    "UNP", "SYK", "LOW", "BLK", "AXP", "ETN", "SCHW", "C", "BSX", "WFC",
]


# ─────────────────────────────────────────────
# TICKERS TO EXCLUDE
# Symbols that pass the market cap filter but cause yfinance errors
# (e.g. ultra-high-price share classes, known problematic symbols).
# ─────────────────────────────────────────────
EXCLUDE_TICKERS = {"BRK-A"}


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def _normalize_ticker(ticker: str) -> str:
    """
    FIX #5: Normalize ticker symbols — replace dots with hyphens
    (e.g. 'BRK.B' -> 'BRK-B') so yfinance handles them correctly.
    """
    return ticker.strip().replace(".", "-").replace("/", "-")


def get_tickers(min_market_cap: int = MIN_MARKET_CAP_USD) -> list:
    """
    Fetches all NASDAQ and NYSE tickers from the Nasdaq.com screener API
    and filters by market cap using the screener's built-in marketCap field
    (no per-ticker API calls needed).
    Falls back to FALLBACK_TICKERS if the fetch fails.
    """
    headers = {"User-Agent": "Mozilla/5.0"}
    all_tickers = []

    for exchange in ("NASDAQ", "NYSE"):
        url = (
            f"https://api.nasdaq.com/api/screener/stocks"
            f"?tableonly=true&limit=5000&exchange={exchange}&download=true"
        )
        print(f"Fetching {exchange} ticker list from Nasdaq.com...")
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            resp.raise_for_status()
            rows = resp.json()["data"]["rows"]
            print(f"  Found {len(rows)} {exchange} tickers.")
            all_tickers.extend(rows)
        except Exception as e:
            print(f"  WARNING: Could not fetch {exchange} list: {e}")

    if not all_tickers:
        print("  Falling back to built-in ticker list.\n")
        return {_normalize_ticker(t): None for t in sorted(FALLBACK_TICKERS)}

    print(f"\nFiltering {len(all_tickers)} tickers by market cap > ${min_market_cap/1e9:.0f}B...")
    qualified = {}
    for r in all_tickers:
        symbol = r.get("symbol", "").strip()
        if not symbol:
            continue
        mc_raw = str(r.get("marketCap", "") or "").strip().replace("$", "").replace(",", "")
        multipliers = {"T": 1e12, "B": 1e9, "M": 1e6, "K": 1e3}
        try:
            if mc_raw and mc_raw[-1].upper() in multipliers:
                mc = float(mc_raw[:-1]) * multipliers[mc_raw[-1].upper()]
            else:
                mc = float(mc_raw) if mc_raw else 0.0
        except ValueError:
            mc = 0.0

        if mc >= min_market_cap:
            ticker = _normalize_ticker(symbol)
            if ticker in EXCLUDE_TICKERS:
                print(f"  - {ticker:<10} excluded")
                continue
            # Keep highest market cap if ticker appears on both exchanges
            if ticker not in qualified or mc > qualified[ticker]:
                qualified[ticker] = round(mc / 1e9, 2)
            print(f"  + {ticker:<10} ${mc/1e9:.1f}B")

    qualified = dict(sorted(qualified.items()))
    print(f"\n  {len(qualified)} tickers qualify with market cap > ${min_market_cap/1e9:.0f}B.")
    return qualified


# ─────────────────────────────────────────────
# WEEK RANGE HELPERS
# ─────────────────────────────────────────────
def get_week_ranges(num_weeks: int) -> list:
    """
    Return a list of (start, end) date strings covering:
      - The past N full Mon-Fri weeks
      - Plus the current partial week up to today (weekdays only)

    On weekends, the week that just finished (Mon-Fri) is treated as the
    most recent completed week and is always included in the N weeks.
    On weekdays, the N completed weeks end with last week, and today's
    partial week is appended as an extra entry.
    """
    today = datetime.today()
    weekday = today.weekday()  # 0=Mon ... 6=Sun

    if weekday >= 5:
        # Weekend: the most recent completed week ended this past Friday
        last_monday = today - timedelta(days=weekday)       # Mon of this week
        partial_week = None                                  # no partial week to append
    else:
        # Weekday: most recent completed week ended last Friday
        last_monday = today - timedelta(days=weekday + 7)   # Mon of last week
        current_monday = today - timedelta(days=weekday)
        partial_week = (current_monday.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d"))

    weeks = []
    for i in range(num_weeks):
        week_start = last_monday - timedelta(weeks=i)
        week_end   = week_start + timedelta(days=4)         # Friday
        weeks.append((week_start.strftime("%Y-%m-%d"), week_end.strftime("%Y-%m-%d")))
    weeks = list(reversed(weeks))  # oldest first

    # Append the current partial week if on a weekday
    if partial_week:
        weeks.append(partial_week)

    return weeks


# ─────────────────────────────────────────────
# DATA FETCHING
# ─────────────────────────────────────────────
def _yahoo_session() -> requests.Session:
    """Create a requests session that mimics a browser to avoid Yahoo blocks."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    })
    try:
        session.get("https://finance.yahoo.com", timeout=10)
    except Exception:
        pass
    return session


def _fetch_yahoo_history(ticker: str, start: str, end: str, session: requests.Session) -> pd.DataFrame:
    """
    Fetch daily OHLCV data from Yahoo Finance v8 API directly.
    Returns a DataFrame with columns: Open, High, Low, Close, Volume
    indexed by date string (YYYY-MM-DD).
    """
    start_ts = int(datetime.strptime(start, "%Y-%m-%d").timestamp())
    end_ts   = int(datetime.strptime(end,   "%Y-%m-%d").timestamp()) + 86400

    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        f"?period1={start_ts}&period2={end_ts}&interval=1d&events=history"
    )

    for attempt in range(3):
        try:
            resp = session.get(url, timeout=20)
            if resp.status_code == 429:
                time.sleep(5 * (attempt + 1))
                continue
            if resp.status_code != 200:
                return pd.DataFrame()
            data = resp.json()
            result = data.get("chart", {}).get("result", [])
            if not result:
                return pd.DataFrame()

            r         = result[0]
            timestamps = r.get("timestamp", [])
            quote      = r["indicators"]["quote"][0]
            adjclose   = r["indicators"].get("adjclose", [{}])[0].get("adjclose", quote["close"])

            rows = []
            for i, ts in enumerate(timestamps):
                try:
                    rows.append({
                        "Date":   datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d"),
                        "Open":   quote["open"][i],
                        "High":   quote["high"][i],
                        "Low":    quote["low"][i],
                        "Close":  adjclose[i],
                        "Volume": quote["volume"][i],
                    })
                except (IndexError, TypeError):
                    continue

            if not rows:
                return pd.DataFrame()

            df = pd.DataFrame(rows).dropna(subset=["Close"])
            df = df.set_index("Date")
            return df

        except Exception:
            time.sleep(2)

    return pd.DataFrame()


def fetch_all_weeks(tickers: list, weeks: list) -> pd.DataFrame:
    """
    Download price data for all tickers using Yahoo Finance v8 API directly,
    then slice into weekly buckets.
    """
    if len(tickers) < 2:
        raise ValueError(f"Expected at least 2 tickers, got {len(tickers)}.")

    overall_start = weeks[0][0]
    overall_end   = (datetime.strptime(weeks[-1][1], "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"\nDownloading price data for {len(tickers)} stocks")
    print(f"Date range: {overall_start} to {weeks[-1][1]}")
    print("This may take a few minutes...\n")

    session = _yahoo_session()

    # Download all tickers
    all_data = {}
    failed = []
    for i, ticker in enumerate(tickers, 1):
        df = _fetch_yahoo_history(ticker, overall_start, overall_end, session)
        if df.empty:
            failed.append(ticker)
        else:
            all_data[ticker] = df
        if i % 50 == 0:
            print(f"  {i}/{len(tickers)} tickers fetched ({len(all_data)} succeeded)...")
        # Small delay to avoid rate limiting
        time.sleep(0.05)

    print(f"\n  {len(all_data)} tickers downloaded successfully ({len(failed)} failed).\n")

    # Fetch shares outstanding
    print("Fetching shares outstanding...")
    shares_outstanding = {}
    for i, ticker in enumerate(all_data.keys(), 1):
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?period1=0&period2=9999999999&interval=3mo&events=history"
            resp = session.get(url, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                meta = data.get("chart", {}).get("result", [{}])[0].get("meta", {})
                shares = meta.get("sharesOutstanding")
                if shares:
                    shares_outstanding[ticker] = shares
        except Exception:
            pass
        if i % 100 == 0:
            print(f"  {i} done...")
    print(f"  Shares outstanding fetched for {len(shares_outstanding)} tickers.\n")

    results = []

    for week_idx, (w_start, w_end) in enumerate(weeks, 1):
        label = f"{w_start} to {w_end}"
        print(f"  Processing week {week_idx:>2}/{len(weeks)}: {label}")

        if week_idx >= 2:
            prev_start, prev_end = weeks[week_idx - 2]
        else:
            prev_start, prev_end = None, None

        for ticker in tickers:
            if ticker not in all_data:
                continue
            try:
                full_df = all_data[ticker]
                df      = full_df.loc[w_start:w_end]
                df_prev = full_df.loc[prev_start:prev_end] if prev_start else None

                df = df.dropna(how="all")
                if df.empty:
                    continue

                week_open  = float(df["Open"].iloc[0])
                week_close = float(df["Close"].iloc[-1])
                week_high  = float(df["High"].max())
                week_low   = float(df["Low"].min())
                week_vol   = int(df["Volume"].sum())

                if df_prev is not None and not df_prev.dropna(how="all").empty:
                    prev_close = float(df_prev.dropna(how="all")["Close"].iloc[-1])
                    pct_change = ((week_close - prev_close) / prev_close) * 100
                else:
                    pct_change = None

                shares = shares_outstanding.get(ticker)
                hist_mc = round(week_close * shares / 1e9, 2) if shares else None

                results.append({
                    "Week":           label,
                    "Ticker":         ticker,
                    "Market Cap (B)": hist_mc,
                    "Open":           round(week_open, 2),
                    "Close":          round(week_close, 2),
                    "High":           round(week_high, 2),
                    "Low":            round(week_low, 2),
                    "Volume":         week_vol,
                    "% Change":       round(pct_change, 2) if pct_change is not None else None,
                    "Direction":      ("Up" if pct_change >= 0 else "Down") if pct_change is not None else "N/A",
                })

            except Exception as e:
                print(f"    WARNING {ticker}: {e}")

    return pd.DataFrame(results)


def add_next_week_change(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds forward-looking columns, all relative to the current week's Close:

      - % Close 1 Week           : close of week+1 vs current close
      - % High 1 Week : high of week+1 vs current close
      - % Low 1 Week  : low of week+1 vs current close
      - % Close 2 Weeks           : close of week+2 vs current close
      - % Close 3 Weeks           : close of week+3 vs current close
      - % Close 4 Weeks           : close of week+4 vs current close
      - % High 4 Weeks            : highest high across weeks+1 to +4 vs current close
      - % Low 4 Weeks             : lowest low across weeks+1 to +4 vs current close

    All columns use strict week-continuity checks so data gaps never
    silently link non-adjacent weeks.
    """
    df = df.sort_values(["Ticker", "Week"]).reset_index(drop=True)

    # Build a reference list of all weeks in strict order
    all_weeks = sorted(df["Week"].unique())
    week_to_idx = {w: i for i, w in enumerate(all_weeks)}

    def compute_forward_cols(group: pd.DataFrame) -> pd.DataFrame:
        n            = len(group)
        week_indices = group["Week"].map(week_to_idx).values
        close_values = group["Close"].values
        high_values  = group["High"].values
        low_values   = group["Low"].values
        pct_values   = group["% Change"].values

        close_1w       = [None] * n
        high_1w        = [None] * n
        low_1w         = [None] * n
        close_2w       = [None] * n
        high_2w        = [None] * n
        low_2w         = [None] * n
        close_3w       = [None] * n
        high_3w        = [None] * n
        low_3w         = [None] * n
        close_4w       = [None] * n
        high_4w        = [None] * n
        low_4w         = [None] * n
        close_8w       = [None] * n
        high_8w        = [None] * n
        low_8w         = [None] * n
        close_12w      = [None] * n
        high_12w       = [None] * n
        low_12w        = [None] * n

        for i in range(n):
            current_close = close_values[i]
            if not current_close:
                continue

            # ── 1-week forward ──────────────────────────────────────
            if i + 1 < n and week_indices[i + 1] == week_indices[i] + 1:
                close_1w[i] = round(pct_values[i + 1], 2) if pct_values[i + 1] is not None else None
                high_1w[i]  = round((high_values[i + 1] - current_close) / current_close * 100, 2)
                low_1w[i]   = round((low_values[i + 1]  - current_close) / current_close * 100, 2)

            # ── 2-week forward ──────────────────────────────────────
            if i + 2 < n and all(
                week_indices[i + k + 1] == week_indices[i + k] + 1 for k in range(2)
            ):
                close_2w[i] = round((close_values[i + 2] - current_close) / current_close * 100, 2)
                high_2w[i]  = round((max(high_values[i + 1 : i + 3]) - current_close) / current_close * 100, 2)
                low_2w[i]   = round((min(low_values[i + 1 : i + 3])  - current_close) / current_close * 100, 2)

            # ── 3-week forward ──────────────────────────────────────
            if i + 3 < n and all(
                week_indices[i + k + 1] == week_indices[i + k] + 1 for k in range(3)
            ):
                close_3w[i] = round((close_values[i + 3] - current_close) / current_close * 100, 2)
                high_3w[i]  = round((max(high_values[i + 1 : i + 4]) - current_close) / current_close * 100, 2)
                low_3w[i]   = round((min(low_values[i + 1 : i + 4])  - current_close) / current_close * 100, 2)

            # ── 4-week forward ──────────────────────────────────────
            if i + 4 < n and all(
                week_indices[i + k + 1] == week_indices[i + k] + 1 for k in range(4)
            ):
                close_4w[i] = round((close_values[i + 4] - current_close) / current_close * 100, 2)
                high_4w[i]  = round((max(high_values[i + 1 : i + 5]) - current_close) / current_close * 100, 2)
                low_4w[i]   = round((min(low_values[i + 1 : i + 5])  - current_close) / current_close * 100, 2)

            # ── 8-week forward ──────────────────────────────────────
            if i + 8 < n and all(
                week_indices[i + k + 1] == week_indices[i + k] + 1 for k in range(8)
            ):
                close_8w[i] = round((close_values[i + 8] - current_close) / current_close * 100, 2)
                high_8w[i]  = round((max(high_values[i + 1 : i + 9]) - current_close) / current_close * 100, 2)
                low_8w[i]   = round((min(low_values[i + 1 : i + 9])  - current_close) / current_close * 100, 2)

            # ── 12-week forward ─────────────────────────────────────
            if i + 12 < n and all(
                week_indices[i + k + 1] == week_indices[i + k] + 1 for k in range(12)
            ):
                close_12w[i] = round((close_values[i + 12] - current_close) / current_close * 100, 2)
                high_12w[i]  = round((max(high_values[i + 1 : i + 13]) - current_close) / current_close * 100, 2)
                low_12w[i]   = round((min(low_values[i + 1 : i + 13])  - current_close) / current_close * 100, 2)

        group = group.copy()
        group["% Close 1 Week"]           = close_1w
        group["% High 1 Week"] = high_1w
        group["% Low 1 Week"]  = low_1w
        group["% Close 2 Weeks"]           = close_2w
        group["% High 2 Weeks"]            = high_2w
        group["% Low 2 Weeks"]             = low_2w
        group["% Close 3 Weeks"]           = close_3w
        group["% High 3 Weeks"]            = high_3w
        group["% Low 3 Weeks"]             = low_3w
        group["% Close 4 Weeks"]           = close_4w
        group["% High 4 Weeks"]            = high_4w
        group["% Low 4 Weeks"]             = low_4w
        group["% Close 8 Weeks"]           = close_8w
        group["% High 8 Weeks"]            = high_8w
        group["% Low 8 Weeks"]             = low_8w
        group["% Close 12 Weeks"]          = close_12w
        group["% High 12 Weeks"]           = high_12w
        group["% Low 12 Weeks"]            = low_12w
        return group

    # Iterate manually instead of groupby().apply() to avoid pandas 2.x
    # dropping the grouping column (Ticker) from the result.
    chunks = []
    for ticker, group in df.groupby("Ticker", sort=False):
        chunks.append(compute_forward_cols(group))
    df = pd.concat(chunks).reset_index(drop=True)

    for col in ("% Close 1 Week", "% High 1 Week", "% Low 1 Week",
                "% Close 2 Weeks", "% High 2 Weeks", "% Low 2 Weeks",
                "% Close 3 Weeks", "% High 3 Weeks", "% Low 3 Weeks",
                "% Close 4 Weeks", "% High 4 Weeks", "% Low 4 Weeks",
                "% Close 8 Weeks", "% High 8 Weeks", "% Low 8 Weeks",
                "% Close 12 Weeks", "% High 12 Weeks", "% Low 12 Weeks"):
        df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

    return df


def add_weekly_rank(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a 'Rank' column — within each week, stocks are ranked 1 (best) to N (worst)
    by % Change. Stocks with no % Change data (first week) receive no rank.
    """
    df["Rank"] = df.groupby("Week")["% Change"].rank(ascending=False, method="first").astype("Int64")
    return df


def save_results(df: pd.DataFrame, ticker_market_caps: dict = None) -> tuple:
    """Save full results and a pivot summary to CSV files."""
    df = add_next_week_change(df)
    df = add_weekly_rank(df)
    df = df.sort_values(["Week", "% Change"], ascending=[True, False]).reset_index(drop=True)

    # Enforce column order
    ordered_cols = [
        "Week", "Ticker", "Market Cap (B)", "Open", "Close", "High", "Low", "Volume",
        "% Change", "Direction", "Rank",
        "% Close 1 Week", "% Close 2 Weeks", "% Close 3 Weeks", "% Close 4 Weeks", "% Close 8 Weeks", "% Close 12 Weeks",
        "% High 1 Week",  "% High 2 Weeks",  "% High 3 Weeks",  "% High 4 Weeks",  "% High 8 Weeks",  "% High 12 Weeks",
        "% Low 1 Week",   "% Low 2 Weeks",   "% Low 3 Weeks",   "% Low 4 Weeks",   "% Low 8 Weeks",   "% Low 12 Weeks",
    ]
    # Keep any unexpected extra columns at the end rather than silently dropping them
    extra_cols = [c for c in df.columns if c not in ordered_cols]
    df = df[ordered_cols + extra_cols]

    date_str = datetime.today().strftime("%Y-%m-%d")
    detail_file = f"nasdaq_and_nyse_rank_{date_str}.csv"
    df.to_csv(detail_file, index=False)
    print(f"\n  Full detail saved to  : {detail_file}")

    # pivot = df.pivot_table(index="Ticker", columns="Week", values="% Change")
    # pivot_file = "nasdaq_and_nyse_rank_pivot.csv"
    # pivot.to_csv(pivot_file)
    # print(f"  Pivot table saved to  : {pivot_file}")

    return detail_file


def print_summary(df: pd.DataFrame) -> None:
    valid = df.dropna(subset=["% Change"])
    print("\n" + "=" * SEPARATOR_WIDTH)  # FIX #9: use named constant
    print("          52-WEEK PERFORMANCE SUMMARY")
    print("=" * SEPARATOR_WIDTH)

    best  = valid.loc[valid["% Change"].idxmax()]
    worst = valid.loc[valid["% Change"].idxmin()]
    print(f"  Best single-week return  : {best['Ticker']} {best['% Change']:+.2f}% ({best['Week']})")
    print(f"  Worst single-week return : {worst['Ticker']} {worst['% Change']:+.2f}% ({worst['Week']})")

    avg_by_ticker = valid.groupby("Ticker")["% Change"].mean().sort_values(ascending=False)
    print(f"\n  Top 5 stocks by avg weekly % change (52 weeks):")
    for ticker, val in avg_by_ticker.head(5).items():
        print(f"    {ticker:<10} {val:+.2f}%")
    print(f"\n  Bottom 5 stocks by avg weekly % change (52 weeks):")
    for ticker, val in avg_by_ticker.tail(5).items():
        print(f"    {ticker:<10} {val:+.2f}%")
    print("=" * SEPARATOR_WIDTH)
    print(f"\n  Total rows: {len(df):,}  ({df['Ticker'].nunique()} stocks x {df['Week'].nunique()} weeks)")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
if __name__ == "__main__":
    # Step 1: Get qualifying tickers
    ticker_market_caps = get_tickers(MIN_MARKET_CAP_USD)
    tickers = list(ticker_market_caps.keys())
    print(f"\nRunning with {len(tickers)} tickers.")

    # Step 2: Build week ranges
    weeks = get_week_ranges(NUM_WEEKS)
    print(f"Fetching {len(weeks)} weeks: {weeks[0][0]}  to  {weeks[-1][1]}")

    # Step 3: Download and process
    df = fetch_all_weeks(tickers, weeks)

    if df.empty:
        print("\nNo data retrieved. Check your internet connection.")
    else:
        save_results(df, ticker_market_caps)
        print_summary(df)
