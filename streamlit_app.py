"""Stonks — real-time crypto dashboard.

Flow: Binance WebSocket -> producer.py -> Kafka (ticker / kline) -> Kafka Connect
Redis sinks -> this dashboard.

  * ticker data  -> Redis hashes   ``ticker:<SYMBOL>``      (overwritten every tick)
  * kline data   -> Redis streams  ``kline:<SYMBOL>:<IV>``  (append-only, XADD)

The dashboard re-renders on a fragment timer (default 1s) so new ticks and candles
show up without a page reload.
"""

import os

import altair as alt
import pandas as pd
import redis
import streamlit as st

# ── Page setup ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Stonks · Live Market",
    page_icon="📈",
    layout="wide",
)

GREEN, RED = "#21c354", "#ff4b4b"
MAX_KLINE_POINTS = 600  # samples pulled per symbol/interval stream
MAX_CANDLES = 150  # candles drawn when the interval is dense (e.g. 1s)

TICKER_FIELDS = (
    "bid_price", "bid_qty", "ask_price", "ask_qty",
    "last_price", "pct_change", "avg_price",
    "open_price", "high_price", "low_price",
)

# Refresh choices -> value passed to st.fragment(run_every=...)
REFRESH_MODES = {"1s": "1s", "2s": "2s", "5s": "5s", "10s": "10s", "Off": None}

CANDLE_X = alt.X("time:T", title=None)
PRICE_Y = alt.Y("close:Q", title="price", scale=alt.Scale(zero=False))


# ── Redis access ──────────────────────────────────────────────────────────────
@st.cache_resource(show_spinner=False)
def _redis_client(host: str, port: int) -> redis.Redis:
    return redis.Redis(
        host=host,
        port=port,
        decode_responses=True,
        socket_connect_timeout=2,
        socket_timeout=2,
    )


@st.cache_resource(show_spinner=False)
def resolve_redis():
    """Find a reachable Redis: REDIS_HOST/REDIS_PORT first, then docker/localhost."""
    candidates = []
    if os.environ.get("REDIS_HOST"):
        candidates.append((os.environ["REDIS_HOST"], int(os.environ.get("REDIS_PORT", "6379"))))
    candidates += [("redis", 6379), ("localhost", 6379), ("127.0.0.1", 6379)]

    for host, port in dict.fromkeys(candidates):
        try:
            client = _redis_client(host, port)
            client.ping()
            return client, host, port
        except (redis.RedisError, OSError):
            continue
    return None, None, None


# ── Loaders ───────────────────────────────────────────────────────────────────
def load_tickers(client: redis.Redis) -> dict[str, dict]:
    """Read every ``ticker:<SYMBOL>`` hash into ``{symbol: record}``."""
    tickers: dict[str, dict] = {}
    for key in client.scan_iter(match="ticker:*"):
        try:
            raw = client.hgetall(key)
        except redis.RedisError:
            continue
        if not raw:
            continue

        record = {"key": key}
        for field in TICKER_FIELDS:
            try:
                record[field] = float(raw[field])
            except (KeyError, TypeError, ValueError):
                record[field] = None
        record["symbol"] = raw.get("symbol") or key.split(":")[-1]
        record["timestamp"] = pd.to_datetime(raw["timestamp"]) if raw.get("timestamp") else None
        tickers[record["symbol"]] = record
    return dict(sorted(tickers.items()))


def discover_kline_streams(client: redis.Redis) -> dict[str, list[str]]:
    """Map ``{symbol: [interval, ...]}`` from the ``kline:<SYMBOL>:<INTERVAL>`` keys."""
    found: dict[str, set[str]] = {}
    for key in client.scan_iter(match="kline:*"):
        parts = key.split(":")
        if len(parts) == 3:
            found.setdefault(parts[1], set()).add(parts[2])
    return {symbol: sorted(ivs) for symbol, ivs in sorted(found.items())}


def load_kline(client: redis.Redis, symbol: str, interval: str) -> pd.DataFrame:
    """Return the last ``MAX_KLINE_POINTS`` candles as a chronological DataFrame."""
    try:
        rows = client.xrevrange(f"kline:{symbol}:{interval}", count=MAX_KLINE_POINTS)
    except redis.RedisError:
        return pd.DataFrame()

    records = []
    for _, fields in rows:
        try:
            records.append({
                "time": pd.to_datetime(fields["timestamp"]),
                "open": float(fields["open_price"]),
                "high": float(fields["high_price"]),
                "low": float(fields["low_price"]),
                "close": float(fields["close_price"]),
            })
        except (KeyError, TypeError, ValueError):
            continue  # skip malformed entries rather than break the whole panel
    if not records:
        return pd.DataFrame()
    return pd.DataFrame(records[::-1]).sort_values("time")


# ── Formatting helpers ────────────────────────────────────────────────────────
def fmt_price(value: float | None) -> str:
    if value is None:
        return "—"
    if value >= 1000:
        return f"{value:,.2f}"
    if value >= 1:
        return f"{value:,.4f}".rstrip("0").rstrip(".")
    return f"{value:.6f}".rstrip("0").rstrip(".")


def spread_of(record: dict) -> float | None:
    if record.get("ask_price") is None or record.get("bid_price") is None:
        return None
    return record["ask_price"] - record["bid_price"]


# ── Chart builders ────────────────────────────────────────────────────────────
def comparison_chart(frames: dict[str, pd.DataFrame]) -> alt.Chart | None:
    """Line chart of close prices rebased to 100 so scales are comparable."""
    rows = []
    for symbol, frame in frames.items():
        if frame.empty:
            continue
        base = float(frame["close"].iloc[0])
        if base <= 0:
            continue
        for time, close in zip(frame["time"], frame["close"]):
            rows.append({"time": time, "symbol": symbol, "close": close, "index": close / base * 100.0})
    data = pd.DataFrame(rows)
    if data.empty:
        return None
    return (
        alt.Chart(data)
        .mark_line()
        .encode(
            x=alt.X("time:T", title=None),
            y=alt.Y("index:Q", title="price (window start = 100)", scale=alt.Scale(zero=False)),
            color=alt.Color("symbol:N", title=None),
            tooltip=[
                alt.Tooltip("symbol:N", title="Symbol"),
                alt.Tooltip("close:Q", title="Close", format=",.2f"),
                alt.Tooltip("index:Q", title="Index", format=".2f"),
                alt.Tooltip("time:T", title="Time"),
            ],
        )
        .properties(height=320)
        .interactive()
    )


def candlestick_chart(frame: pd.DataFrame, title: str) -> alt.Chart:
    """OHLC candles for a single symbol/interval."""
    data = frame.tail(MAX_CANDLES)
    base = alt.Chart(data)
    wick = base.mark_rule(color="#9aa0a6", strokeWidth=1).encode(
        x=CANDLE_X,
        y=alt.Y("low:Q", title="price", scale=alt.Scale(zero=False)),
        y2="high:Q",
    )
    body = base.mark_bar().encode(
        x=CANDLE_X,
        y=alt.Y("open:Q", scale=alt.Scale(zero=False)),
        y2="close:Q",
        color=alt.condition("datum.open <= datum.close", alt.value(GREEN), alt.value(RED)),
        tooltip=[
            alt.Tooltip("time:T", title="Time"),
            alt.Tooltip("open:Q", title="Open", format=",.2f"),
            alt.Tooltip("high:Q", title="High", format=",.2f"),
            alt.Tooltip("low:Q", title="Low", format=",.2f"),
            alt.Tooltip("close:Q", title="Close", format=",.2f"),
        ],
    )
    return (wick + body).properties(height=320, title=title)


def change_chart(tickers: dict[str, dict]) -> alt.Chart | None:
    """Bar chart of the 24h percent change reported by each ticker."""
    rows = [
        {"symbol": symbol, "pct": record["pct_change"] or 0.0}
        for symbol, record in tickers.items()
    ]
    if not rows:
        return None
    data = pd.DataFrame(rows)
    return (
        alt.Chart(data)
        .mark_bar()
        .encode(
            x=alt.X("symbol:N", sort=list(data["symbol"]), title=None),
            y=alt.Y("pct:Q", title="24h change (%)"),
            color=alt.condition("datum.pct >= 0", alt.value(GREEN), alt.value(RED)),
            tooltip=[
                alt.Tooltip("symbol:N", title="Symbol"),
                alt.Tooltip("pct:Q", title="24h %", format="+.3f"),
            ],
        )
        .properties(height=320)
    )


def tickers_frame(tickers: dict[str, dict]) -> pd.DataFrame:
    rows = []
    for symbol, record in tickers.items():
        rows.append({
            "Symbol": symbol,
            "Last": record["last_price"],
            "Bid": record["bid_price"],
            "Ask": record["ask_price"],
            "Spread": spread_of(record),
            "24h %": record["pct_change"],
            "Open": record["open_price"],
            "High": record["high_price"],
            "Low": record["low_price"],
            "Avg": record["avg_price"],
            "Bid Qty": record["bid_qty"],
            "Ask Qty": record["ask_qty"],
            "Updated": record["timestamp"],
        })
    return pd.DataFrame(rows)


def window_summary(frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Per-symbol stats over the loaded candle window (count, high/low range, volatility)."""
    rows = []
    for symbol, frame in sorted(frames.items()):
        if frame.empty:
            continue
        returns = frame["close"].astype(float).pct_change().dropna()
        low, high = float(frame["low"].min()), float(frame["high"].max())
        rows.append({
            "Symbol": symbol,
            "Candles": int(len(frame)),
            "Window": f"{frame['time'].iloc[0]:%H:%M:%S}\u2013{frame['time'].iloc[-1]:%H:%M:%S}",
            "Low": low,
            "High": high,
            "Range": high - low,
            "Range %": ((high - low) / low * 100.0) if low else None,
            "Volatility %": float(returns.std() * 100.0) if len(returns) > 1 else None,
        })
    return pd.DataFrame(rows)


def volatility_chart(summary: pd.DataFrame) -> alt.Chart | None:
    """Horizontal bars of per-symbol volatility, ranked highest first."""
    if summary.empty or summary["Volatility %"].isna().all():
        return None
    data = summary.dropna(subset=["Volatility %"]).sort_values("Volatility %", ascending=False)
    return (
        alt.Chart(data)
        .mark_bar()
        .encode(
            y=alt.Y("Symbol:N", sort="-x", title=None),
            x=alt.X("Volatility %:Q", title="volatility (%) · stdev of returns"),
            color=alt.Color("Volatility %:Q", scale=alt.Scale(scheme="orangered"), legend=None),
            tooltip=[
                alt.Tooltip("Symbol:N", title="Symbol"),
                alt.Tooltip("Candles:Q", title="Candles"),
                alt.Tooltip("Range %:Q", title="Range %", format=".3f"),
                alt.Tooltip("Volatility %:Q", title="Volatility %", format=".4f"),
            ],
        )
        .properties(height=max(160, 60 * len(data)))
    )


# ── Sidebar (static part, must be outside the fragment to change run_every) ───
with st.sidebar:
    st.header("Dashboard settings")
    refresh_mode = st.select_slider(
        "Refresh",
        options=list(REFRESH_MODES),
        value="1s",
        key="_refresh",
        help="How often the dashboard re-reads Redis. Data updates without a page reload.",
    )
    st.caption("Flow: Binance → Kafka → Redis (hashes + streams) → this dashboard.")


# ── Dashboard (fragment re-runs on a timer = real-time updates) ───────────────
@st.fragment(run_every=REFRESH_MODES[refresh_mode])
def render_dashboard() -> None:
    client, host, port = resolve_redis()
    if client is None:
        st.error(
            "Could not reach Redis. Tried `redis`, `localhost` and `127.0.0.1` on port 6379 — "
            "set `REDIS_HOST` / `REDIS_PORT` to point somewhere else."
        )
        if st.button("Retry connection"):
            st.cache_resource.clear()
            st.rerun()
        return

    tickers = load_tickers(client)
    klines = discover_kline_streams(client)
    if not tickers and not klines:
        st.warning("No Redis data yet — waiting for the producer and Kafka Connect sinks…")
        st.caption(f"Redis `{host}:{port}` · no `ticker:*` hashes or `kline:*` streams found.")
        return

    # ── Data-dependent controls (inside the fragment so options stay fresh) ──
    all_symbols = sorted(set(tickers) | set(klines))
    if not st.session_state.get("symbols") or any(
        s not in all_symbols for s in st.session_state["symbols"]
    ):
        st.session_state["symbols"] = all_symbols

    all_intervals = sorted({iv for ivs in klines.values() for iv in ivs})
    preferred = "1m" if "1m" in all_intervals else (all_intervals[0] if all_intervals else None)
    if st.session_state.get("interval") not in all_intervals:
        st.session_state["interval"] = preferred

    with st.sidebar:
        st.divider()
        chosen = st.multiselect("Symbols", all_symbols, key="symbols")
        interval = st.selectbox("Kline interval", all_intervals, key="interval")
        st.button("🔄 Refresh now", key="_refresh_now")

    if not chosen:
        st.info("Select at least one symbol in the sidebar.")
        return

    # ── Freshness caption ────────────────────────────────────────────────
    newest_ticker = max(
        (r["timestamp"] for r in tickers.values() if r["timestamp"] is not None), default=None
    )
    frames = {
        symbol: load_kline(client, symbol, interval)
        for symbol in chosen
        if interval in klines.get(symbol, [])
    }
    newest_kline = max(
        (f["time"].max() for f in frames.values() if not f.empty), default=None
    )
    stamp = newest_ticker.strftime("%H:%M:%S UTC") if newest_ticker is not None else "—"
    kline_stamp = newest_kline.strftime("%H:%M:%S UTC") if newest_kline is not None else "—"
    st.caption(
        f"Redis `{host}:{port}` · {len(tickers)} ticker(s) · last tick {stamp} · "
        f"last candle ({interval}) {kline_stamp}"
        + ("" if refresh_mode == "Off" else f" · refreshing every {refresh_mode}")
    )

    # ── Metric cards, 3 per row ─────────────────────────────────────────
    for start in range(0, len(chosen), 3):
        cols = st.columns(3)
        for col, symbol in zip(cols, chosen[start:start + 3]):
            record = tickers.get(symbol)
            frame = frames.get(symbol)
            price = record["last_price"] if record else (
                float(frame["close"].iloc[-1]) if frame is not None and not frame.empty else None
            )
            change = record["pct_change"] if record else None
            with col:
                st.metric(
                    symbol,
                    fmt_price(price),
                    delta=f"{change:+.2f}%" if change is not None else None,
                    border=True,
                )
                if record:
                    st.caption(
                        f"24h H {fmt_price(record['high_price'])} · "
                        f"L {fmt_price(record['low_price'])} · "
                        f"spread {fmt_price(spread_of(record))}"
                    )

    # ── Charts ──────────────────────────────────────────────────────────
    chart_col, change_col = st.columns([2, 1], gap="medium")
    with chart_col:
        st.subheader(f"Close price · {interval}")
        chart = comparison_chart(frames)
        if chart is None:
            st.info(f"No `kline:*:{interval}` data for the selected symbols yet.")
        else:
            st.altair_chart(chart, width="stretch")
            st.caption("All series rebased to 100 at the left edge so different price scales compare.")
    with change_col:
        st.subheader("24h change")
        change = change_chart(tickers)
        if change is None:
            st.info("Waiting for ticker data…")
        else:
            st.altair_chart(change, width="stretch")

    # ── Window summary ──────────────────────────────────────────────────
    if frames:
        st.subheader(f"Window summary · {interval}")
        summary = window_summary(frames)
        if summary.empty:
            st.info("No candles loaded for the selected symbols.")
        else:
            table_col, vol_col = st.columns([3, 2], gap="medium")
            with table_col:
                st.dataframe(
                    summary,
                    hide_index=True,
                    width="stretch",
                    column_config={
                        "Symbol": st.column_config.TextColumn(width="small"),
                        "Candles": st.column_config.NumberColumn(
                            format="%d", help=f"Candles loaded (cap {MAX_KLINE_POINTS} per stream)"
                        ),
                        "Window": st.column_config.TextColumn(help="First–last candle time (UTC)"),
                        "Low": st.column_config.NumberColumn(format="%.2f"),
                        "High": st.column_config.NumberColumn(format="%.2f"),
                        "Range": st.column_config.NumberColumn(format="%.2f"),
                        "Range %": st.column_config.NumberColumn(format="%.2f%%"),
                        "Volatility %": st.column_config.NumberColumn(format="%.3f%%"),
                    },
                )
            with vol_col:
                st.markdown("**Volatility rank**")
                vol_chart = volatility_chart(summary)
                if vol_chart is None:
                    st.info("Not enough candles to rank volatility yet.")
                else:
                    st.altair_chart(vol_chart, width="stretch")
            st.caption(
                f"Up to {MAX_KLINE_POINTS} candles per stream · "
                "volatility = std dev of candle-to-candle close returns."
            )

    # ── Candlesticks ────────────────────────────────────────────────────
    st.subheader("Candlesticks")
    candle_symbol = st.selectbox("Symbol for candles", chosen, key="candle_symbol")
    candle_frame = frames.get(candle_symbol)
    if candle_frame is None or candle_frame.empty:
        st.info(f"No candles for {candle_symbol} at {interval}.")
    else:
        st.altair_chart(
            candlestick_chart(candle_frame, f"{candle_symbol} · {interval}"),
            width="stretch",
        )
        latest = candle_frame.iloc[-1]
        st.caption(
            f"{candle_symbol} @ {latest['time']:%H:%M:%S} UTC · "
            f"O {fmt_price(latest['open'])} · H {fmt_price(latest['high'])} · "
            f"L {fmt_price(latest['low'])} · C {fmt_price(latest['close'])}"
        )
        with st.expander("Raw candle data"):
            st.dataframe(candle_frame.tail(50), hide_index=True, width="stretch")

    # ── Ticker snapshot ─────────────────────────────────────────────────
    if tickers:
        st.subheader("Order book snapshot")
        st.dataframe(
            tickers_frame(tickers),
            hide_index=True,
            width="stretch",
            column_config={
                "Symbol": st.column_config.TextColumn(width="small"),
                "Last": st.column_config.NumberColumn(format="%.2f"),
                "Bid": st.column_config.NumberColumn(format="%.2f"),
                "Ask": st.column_config.NumberColumn(format="%.2f"),
                "Spread": st.column_config.NumberColumn(format="%.2f"),
                "24h %": st.column_config.NumberColumn(format="%+.2f%%"),
                "Open": st.column_config.NumberColumn(format="%.2f"),
                "High": st.column_config.NumberColumn(format="%.2f"),
                "Low": st.column_config.NumberColumn(format="%.2f"),
                "Avg": st.column_config.NumberColumn(format="%.2f"),
                "Bid Qty": st.column_config.NumberColumn(format="%.4f"),
                "Ask Qty": st.column_config.NumberColumn(format="%.4f"),
                "Updated": st.column_config.DatetimeColumn(format="HH:mm:ss"),
            },
        )


st.title("📈 Stonks — live market dashboard")
render_dashboard()
