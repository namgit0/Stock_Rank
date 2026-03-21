"""
Nasdaq & NYSE Weekly Rankings Dashboard
Streamlit app — reads the latest CSV produced by the scraper.
"""

import glob
import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ─────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="Nasdaq & NYSE Rankings",
    page_icon="📈",
    layout="wide",
)

# ─────────────────────────────────────────────
# CUSTOM STYLING
# ─────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;500;600&display=swap');

    html, body, [class*="css"] {
        font-family: 'DM Sans', sans-serif;
    }
    h1, h2, h3 {
        font-family: 'Space Mono', monospace !important;
    }
    .stMetric label {
        font-family: 'Space Mono', monospace !important;
        font-size: 0.7rem !important;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: #888 !important;
    }
    .stMetric value {
        font-family: 'Space Mono', monospace !important;
    }
    .block-container {
        padding-top: 2rem;
    }
    .up-tag {
        background: #00c853;
        color: #000;
        padding: 2px 8px;
        border-radius: 4px;
        font-size: 0.75rem;
        font-weight: 600;
    }
    .down-tag {
        background: #ff1744;
        color: #fff;
        padding: 2px 8px;
        border-radius: 4px;
        font-size: 0.75rem;
        font-weight: 600;
    }
    div[data-testid="stSidebarContent"] {
        background: #0d0d0d;
    }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# DATA LOADING
# ─────────────────────────────────────────────
@st.cache_data(ttl=1800)  # Cache for 30 minutes
def load_data() -> pd.DataFrame:
    files = sorted(glob.glob("nasdaq_and_nyse_rank_*.csv"), reverse=True)
    if not files:
        return pd.DataFrame()
    df = pd.read_csv(files[0])
    df["% Change"] = pd.to_numeric(df["% Change"], errors="coerce")
    df["Market Cap (B)"] = pd.to_numeric(df["Market Cap (B)"], errors="coerce")
    df["Rank"] = pd.to_numeric(df["Rank"], errors="coerce")
    return df


df = load_data()

if df.empty:
    st.error("⚠️ No data file found. Make sure the scraper has run at least once and produced a `nasdaq_and_nyse_rank_*.csv` file.")
    st.stop()

all_weeks = sorted(df["Week"].unique())
latest_week = all_weeks[-1]
df_latest = df[df["Week"] == latest_week].copy()

# ─────────────────────────────────────────────
# SIDEBAR FILTERS
# ─────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🔍 Filters")

    ticker_search = st.text_input("Search ticker", placeholder="e.g. AAPL").upper().strip()

    direction_filter = st.selectbox("Direction", ["All", "Up", "Down"])

    mc_min, mc_max = float(df["Market Cap (B)"].min()), float(df["Market Cap (B)"].max())
    mc_range = st.slider(
        "Market Cap (B)",
        min_value=mc_min,
        max_value=mc_max,
        value=(mc_min, mc_max),
        step=10.0,
        format="$%.0fB",
    )

    rank_max = int(df_latest["Rank"].max()) if not df_latest["Rank"].isna().all() else 500
    rank_filter = st.slider("Max Rank (latest week)", min_value=1, max_value=rank_max, value=rank_max)

    st.markdown("---")
    st.markdown(f"**Latest data:** `{latest_week}`")
    st.markdown(f"**Weeks loaded:** `{len(all_weeks)}`")
    st.markdown(f"**Stocks tracked:** `{df['Ticker'].nunique()}`")

# ─────────────────────────────────────────────
# APPLY FILTERS TO LATEST WEEK VIEW
# ─────────────────────────────────────────────
filtered = df_latest.copy()

if ticker_search:
    filtered = filtered[filtered["Ticker"].str.contains(ticker_search, na=False)]

if direction_filter != "All":
    filtered = filtered[filtered["Direction"] == direction_filter]

filtered = filtered[
    (filtered["Market Cap (B)"] >= mc_range[0]) &
    (filtered["Market Cap (B)"] <= mc_range[1])
]

filtered = filtered[filtered["Rank"] <= rank_filter]
filtered = filtered.sort_values("% Change", ascending=False).reset_index(drop=True)

# ─────────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────────
st.markdown("# 📈 Nasdaq & NYSE Rankings")
st.markdown(f"Weekly performance tracker — **{df['Ticker'].nunique()} stocks**, market cap > $20B")
st.markdown("---")

# ─────────────────────────────────────────────
# TOP METRICS
# ─────────────────────────────────────────────
valid = df_latest.dropna(subset=["% Change"])
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    up_count = (valid["Direction"] == "Up").sum()
    st.metric("Stocks Up", f"{up_count}", f"{up_count/len(valid)*100:.0f}%")
with col2:
    down_count = (valid["Direction"] == "Down").sum()
    st.metric("Stocks Down", f"{down_count}", f"-{down_count/len(valid)*100:.0f}%")
with col3:
    best = valid.loc[valid["% Change"].idxmax()]
    st.metric("Best Performer", best["Ticker"], f"{best['% Change']:+.2f}%")
with col4:
    worst = valid.loc[valid["% Change"].idxmin()]
    st.metric("Worst Performer", worst["Ticker"], f"{worst['% Change']:+.2f}%")
with col5:
    avg = valid["% Change"].mean()
    st.metric("Avg % Change", f"{avg:+.2f}%")

st.markdown("---")

# ─────────────────────────────────────────────
# TOP 10 / BOTTOM 10
# ─────────────────────────────────────────────
st.markdown("## 🏆 Top 10 & Bottom 10 — Latest Week")
col_top, col_bot = st.columns(2)

with col_top:
    top10 = valid.nlargest(10, "% Change")[["Ticker", "% Change", "Market Cap (B)", "Close", "Volume"]]
    fig_top = px.bar(
        top10.sort_values("% Change"),
        x="% Change",
        y="Ticker",
        orientation="h",
        color="% Change",
        color_continuous_scale=["#00c853", "#69f0ae"],
        text="% Change",
        title="Top 10 Performers",
    )
    fig_top.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
    fig_top.update_layout(
        showlegend=False,
        coloraxis_showscale=False,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=10, r=40, t=40, b=10),
        height=380,
    )
    st.plotly_chart(fig_top, use_container_width=True)

with col_bot:
    bot10 = valid.nsmallest(10, "% Change")[["Ticker", "% Change", "Market Cap (B)", "Close", "Volume"]]
    fig_bot = px.bar(
        bot10.sort_values("% Change", ascending=False),
        x="% Change",
        y="Ticker",
        orientation="h",
        color="% Change",
        color_continuous_scale=["#ff6d00", "#ff1744"],
        text="% Change",
        title="Bottom 10 Performers",
    )
    fig_bot.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
    fig_bot.update_layout(
        showlegend=False,
        coloraxis_showscale=False,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=10, r=40, t=40, b=10),
        height=380,
    )
    st.plotly_chart(fig_bot, use_container_width=True)

# ─────────────────────────────────────────────
# % CHANGE DISTRIBUTION
# ─────────────────────────────────────────────
st.markdown("## 📊 % Change Distribution — Latest Week")
fig_hist = px.histogram(
    valid,
    x="% Change",
    nbins=60,
    color_discrete_sequence=["#2979ff"],
    title="Distribution of weekly % price changes",
)
fig_hist.add_vline(x=0, line_dash="dash", line_color="#ff1744", annotation_text="0%")
fig_hist.add_vline(x=avg, line_dash="dot", line_color="#ffd600", annotation_text=f"avg {avg:+.2f}%")
fig_hist.update_layout(
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    margin=dict(l=10, r=10, t=40, b=10),
    height=300,
)
st.plotly_chart(fig_hist, use_container_width=True)

# ─────────────────────────────────────────────
# INDIVIDUAL STOCK PRICE HISTORY
# ─────────────────────────────────────────────
st.markdown("## 🔎 Individual Stock History")

all_tickers = sorted(df["Ticker"].unique())
selected_ticker = st.selectbox(
    "Select a stock",
    all_tickers,
    index=all_tickers.index("AAPL") if "AAPL" in all_tickers else 0,
)

df_ticker = df[df["Ticker"] == selected_ticker].sort_values("Week")

if not df_ticker.empty:
    col_a, col_b, col_c, col_d = st.columns(4)
    latest_row = df_ticker.iloc[-1]
    with col_a:
        st.metric("Latest Close", f"${latest_row['Close']:.2f}")
    with col_b:
        chg = latest_row["% Change"]
        st.metric("Latest % Change", f"{chg:+.2f}%" if pd.notna(chg) else "N/A")
    with col_c:
        mc = latest_row["Market Cap (B)"]
        st.metric("Market Cap", f"${mc:.1f}B" if pd.notna(mc) else "N/A")
    with col_d:
        rank = latest_row["Rank"]
        st.metric("Latest Rank", f"#{int(rank)}" if pd.notna(rank) else "N/A")

    fig_price = go.Figure()
    fig_price.add_trace(go.Scatter(
        x=df_ticker["Week"],
        y=df_ticker["Close"],
        mode="lines+markers",
        name="Close",
        line=dict(color="#2979ff", width=2),
        marker=dict(size=5),
    ))
    fig_price.add_trace(go.Scatter(
        x=df_ticker["Week"],
        y=df_ticker["High"],
        mode="lines",
        name="High",
        line=dict(color="#00c853", width=1, dash="dot"),
    ))
    fig_price.add_trace(go.Scatter(
        x=df_ticker["Week"],
        y=df_ticker["Low"],
        mode="lines",
        name="Low",
        line=dict(color="#ff1744", width=1, dash="dot"),
        fill="tonexty",
        fillcolor="rgba(41,121,255,0.05)",
    ))
    fig_price.update_layout(
        title=f"{selected_ticker} — 52-Week Price History",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=10, r=10, t=40, b=10),
        height=350,
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        xaxis=dict(tickangle=-45, tickfont=dict(size=9)),
    )
    st.plotly_chart(fig_price, use_container_width=True)

    fig_pct = px.bar(
        df_ticker.dropna(subset=["% Change"]),
        x="Week",
        y="% Change",
        color="Direction",
        color_discrete_map={"Up": "#00c853", "Down": "#ff1744"},
        title=f"{selected_ticker} — Weekly % Change",
    )
    fig_pct.add_hline(y=0, line_dash="dash", line_color="#888")
    fig_pct.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=10, r=10, t=40, b=10),
        height=280,
        xaxis=dict(tickangle=-45, tickfont=dict(size=9)),
    )
    st.plotly_chart(fig_pct, use_container_width=True)

# ─────────────────────────────────────────────
# FORWARD RETURN HEATMAP
# ─────────────────────────────────────────────
st.markdown("## 🔭 Forward Return Heatmap — Latest Week")
st.caption("Average forward returns for stocks in the latest week, grouped by performance bucket.")

forward_cols = [
    "% Close 1 Week", "% Close 2 Weeks", "% Close 3 Weeks",
    "% Close 4 Weeks", "% Close 8 Weeks", "% Close 12 Weeks",
]
available_fwd = [c for c in forward_cols if c in df_latest.columns]

if available_fwd:
    df_heat = df_latest.dropna(subset=["% Change"]).copy()
    df_heat["Bucket"] = pd.cut(
        df_heat["% Change"],
        bins=[-999, -10, -5, -2, 0, 2, 5, 10, 999],
        labels=["<-10%", "-10 to -5%", "-5 to -2%", "-2 to 0%", "0 to 2%", "2 to 5%", "5 to 10%", ">10%"],
    )
    heat_data = df_heat.groupby("Bucket", observed=True)[available_fwd].mean().round(2)

    fig_heat = px.imshow(
        heat_data,
        color_continuous_scale="RdYlGn",
        color_continuous_midpoint=0,
        text_auto=True,
        title="Avg forward % return by current-week performance bucket",
        aspect="auto",
    )
    fig_heat.update_layout(
        margin=dict(l=10, r=10, t=40, b=10),
        height=350,
        coloraxis_colorbar=dict(title="% Return"),
    )
    st.plotly_chart(fig_heat, use_container_width=True)
else:
    st.info("Forward return columns not available yet — they appear after the scraper has run for several weeks.")

# ─────────────────────────────────────────────
# FULL DATA TABLE
# ─────────────────────────────────────────────
st.markdown("---")
st.markdown("## 📋 Full Data Table — Latest Week")
st.caption(f"Showing {len(filtered)} stocks after filters. Week: {latest_week}")

display_cols = [
    "Rank", "Ticker", "Market Cap (B)", "Close", "Open", "High", "Low",
    "Volume", "% Change", "Direction",
]
display_cols = [c for c in display_cols if c in filtered.columns]

st.dataframe(
    filtered[display_cols].style.format({
        "% Change": "{:+.2f}%",
        "Close": "${:.2f}",
        "Open": "${:.2f}",
        "High": "${:.2f}",
        "Low": "${:.2f}",
        "Market Cap (B)": "${:.1f}B",
        "Volume": "{:,.0f}",
    }).background_gradient(subset=["% Change"], cmap="RdYlGn", vmin=-10, vmax=10),
    use_container_width=True,
    height=500,
)

st.download_button(
    label="⬇️ Download filtered data as CSV",
    data=filtered.to_csv(index=False),
    file_name=f"rankings_{latest_week}.csv",
    mime="text/csv",
)

# ─────────────────────────────────────────────
# HISTORICAL TABLE
# ─────────────────────────────────────────────
with st.expander("📅 View full 52-week history table"):
    hist_ticker = st.selectbox("Pick a ticker to view history", all_tickers, key="hist_ticker")
    df_hist = df[df["Ticker"] == hist_ticker].sort_values("Week", ascending=False)
    st.dataframe(df_hist.reset_index(drop=True), use_container_width=True, height=400)
