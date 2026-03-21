# Nasdaq & NYSE Weekly Stock Rankings

Automatically scrapes weekly OHLCV data and performance rankings for all NASDAQ and NYSE stocks with a market cap > $20B.

## What it does

- Fetches all qualifying tickers from the Nasdaq.com screener API
- Downloads 52 weeks of weekly price data via `yfinance`
- Calculates % price change, weekly rank, and forward-looking returns (1, 2, 3, 4, 8, 12 weeks)
- Saves results to a dated CSV file: `nasdaq_and_nyse_rank_YYYY-MM-DD.csv`

## Schedule

Runs automatically **twice a day at 6am and 6pm UTC, Monday–Friday** via GitHub Actions.
You can also trigger it manually from the **Actions** tab.

## Output columns

| Column | Description |
|---|---|
| Week | Date range (YYYY-MM-DD to YYYY-MM-DD) |
| Ticker | Stock symbol |
| Market Cap (B) | Historical market cap in billions |
| Open / Close / High / Low | Weekly OHLC prices |
| Volume | Total weekly volume |
| % Change | Close-to-close % change vs prior week |
| Direction | Up / Down |
| Rank | Weekly rank by % Change (1 = best) |
| % Close 1–12 Weeks | Forward close return over N weeks |
| % High / Low 1–12 Weeks | Forward high/low return over N weeks |

## Setup

1. Clone this repo
2. Go to the **Actions** tab and enable workflows
3. That's it — it runs on schedule automatically

## Local usage

```bash
pip install -r requirements.txt
python Nasdaq_and_NYSE_Rank_Beta.py
```

## Dependencies

- `yfinance`
- `pandas`
- `requests`
