"""Stonks — real-time crypto dashboard.

Flow: Binance WebSocket -> producer.py -> Kafka (ticker / kline) -> Kafka Connect
Redis sinks -> this dashboard.

  * ticker data  -> Redis hashes   ``ticker:<SYMBOL>``      (overwritten every tick)
  * kline data   -> Redis streams  ``kline:<SYMBOL>:<IV>``  (append-only, XADD)

Refresh strategy (keeps the page from repainting nonstop):

  * quotes/cards  -> fast fragment, retimed by the "Refresh (quotes)" control
  * charts/table  -> slower fragment, retimed by "Refresh (charts)"; chart specs are
    cached and rebuilt only when the underlying candles actually change
  * candle reads are incremental: only entries newer than the last seen stream id
    are fetched, with a periodic full reload to stay self-healing
"""

import os
import time

import altair as alt
import pandas as pd
import redis
import streamlit as st

# ── Page setup ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Fishtonks · Live Market",
    # page_icon="📈",
    layout="wide",
)

GREEN, RED = "#21c354", "#ff4b4b"
MAX_KLINE_POINTS = 600  # samples kept per symbol/interval stream
MAX_CANDLES = 150  # candles drawn when the interval is dense (e.g. 1s)
FULL_RELOAD_SECONDS = 30.0  # periodic full re-read so incremental reads self-heal

TICKER_FIELDS = (
    "bid_price", "bid_qty", "ask_price", "ask_qty",
    "last_price", "pct_change", "avg_price",
    "open_price", "high_price", "low_price",
)

# Refresh choices -> value passed to st.fragment(run_every=...)
REFRESH_MODES = {"1s": "1s", "2s": "2s", "5s": "5s", "10s": "10s", "Off": None}

KLINE_COLUMNS = ["time", "open", "high", "low", "close"]
CANDLE_X = alt.X("time:T", title=None)


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


# ── Stream helpers ────────────────────────────────────────────────────────────
def _entry_sort_key(entry_id: str) -> tuple[int, int]:
    ms, _, seq = entry_id.partition("-")
    return int(ms or 0), int(seq or 0)


def _newest_id(rows) -> str | None:
    return max((r[0] for r in rows), key=_entry_sort_key, default=None)


def _rows_to_frame(rows) -> pd.DataFrame:
    """One vectorised timestamp parse per batch instead of one per entry."""
    records = []
    for _, fields in rows:
        try:
            records.append((
                fields["timestamp"],
                float(fields["open_price"]),
                float(fields["high_price"]),
                float(fields["low_price"]),
                float(fields["close_price"]),
            ))
        except (KeyError, TypeError, ValueError):
            continue  # skip malformed entries rather than break the panel

    if not records:
        return pd.DataFrame(columns=KLINE_COLUMNS)

    frame = pd.DataFrame(records, columns=["timestamp", *KLINE_COLUMNS[1:]])
    frame["time"] = pd.to_datetime(frame["timestamp"], format="ISO8601")
    return frame.drop(columns=["timestamp"])


def load_kline(client: redis.Redis, symbol: str, interval: str) -> pd.DataFrame:
    """Candles for one symbol/interval, fetched incrementally between refreshes."""
    key = f"kline:{symbol}:{interval}"
    frames = st.session_state.setdefault("_kline_frames", {})
    meta = st.session_state.setdefault("_kline_meta", {})

    cached = frames.get(key)
    state = meta.get(key, {})
    last_id = state.get("last_id")
    stale = (time.monotonic() - state.get("full_at", 0.0)) > FULL_RELOAD_SECONDS

    try:
        if cached is None or cached.empty or not last_id or stale:
            rows = client.xrevrange(key, count=MAX_KLINE_POINTS)
            full = True
        else:
            rows = client.xrange(key, min=f"({last_id}", max="+")
            full = False
    except redis.RedisError:
        return cached if cached is not None else pd.DataFrame(columns=KLINE_COLUMNS)

    if full or cached is None or cached.empty:
        frame = _rows_to_frame(rows)
    elif rows:
        frame = pd.concat([cached, _rows_to_frame(rows)], ignore_index=True)
        frame = frame.drop_duplicates(subset="time", keep="last")
    else:
        frame = cached

    if frame is None or frame.empty:
        return pd.DataFrame(columns=KLINE_COLUMNS)

    frame = frame.sort_values("time").tail(MAX_KLINE_POINTS).reset_index(drop=True)
    frames[key] = frame
    meta[key] = {
        "last_id": _newest_id(rows) or last_id,
        "full_at": time.monotonic() if full else state.get("full_at", 0.0),
    }
    return frame


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


# ...existing code...
# ── Chart builders ────────────────────────────────────────────────────────────
COMPARE_ZOOM = "compare_zoom"   # named Altair param; Streamlit reports it back under this key
COMPARE_CHART_KEY = "compare_chart"


def compare_chart_key() -> str:
    """Widget key for the comparison chart, suffixed with the reset epoch.

    Streamlit derives the element id from ``key`` (plus the selection mode) and
    ignores the spec, then restores the saved Vega view state for any element id
    it has seen before. So a spec change does *not* reset pan/zoom — the frontend
    re-applies the old view state. Bumping the epoch yields an unseen element id,
    which is what actually forces a fresh unzoomed view.
    """
    return f"{COMPARE_CHART_KEY}_{st.session_state.get('_compare_epoch', 0)}"


def reset_compare_view() -> None:
    """Consume a pending "Reset view" click, before the chart is (re)created.

    The epoch feeds the widget key, so it may only be bumped on a run where the
    chart does not exist yet — hence the button -> flag -> rerun dance.
    """
    if not st.session_state.pop("_compare_reset_pending", False):
        return
    st.session_state["_compare_epoch"] = st.session_state.get("_compare_epoch", 0) + 1
    st.session_state.pop("_rebase_anchors", None)  # re-baseline to the current window


def rebase_anchor(symbol: str, frame: pd.DataFrame) -> float:
    """The price ``symbol``'s index series is anchored to (index 100).

    Latched on first sight and kept for the session. Do *not* use
    ``frame["close"].iloc[0]`` directly: the loaded window slides, so the first
    row is a different candle on every refresh and the whole history would be
    rescaled with it — which shows up as old points drifting up and down.
    """
    anchors = st.session_state.setdefault("_rebase_anchors", {})
    anchor = anchors.get(symbol)
    if anchor is None or anchor <= 0:
        anchor = float(frame["close"].iloc[0])
        anchors[symbol] = anchor
    return anchor


def comparison_chart(frames: dict[str, pd.DataFrame]) -> alt.Chart | None:
    """Line chart of close prices rebased to 100 so scales are comparable.

    Rendered through ``st.altair_chart`` on purpose: Streamlit re-sends the spec
    whenever a candle changes, which is what makes the chart update. (A fixed
    ``datasets`` name looks like it should be updated in place instead, but the
    frontend inserts a named dataset once and never replaces it — the chart then
    renders its first payload forever.)
    """
    rows = []
    for symbol, frame in frames.items():
        if frame.empty:
            continue
        anchor = rebase_anchor(symbol, frame)
        if anchor <= 0:
            continue
        for t, c in zip(frame["time"], frame["close"]):
            rows.append({"time": t, "symbol": symbol, "close": c, "index": c / anchor * 100.0})
    data = pd.DataFrame(rows)
    if data.empty:
        return None

    return (
        alt.Chart(data)
        .mark_line()
        .encode(
            x=alt.X("time:T", title=None),
            y=alt.Y(
                "index:Q",
                title="index (first loaded candle = 100)",
                scale=alt.Scale(zero=False),
            ),
            color=alt.Color("symbol:N", title=None),
            tooltip=[
                alt.Tooltip("symbol:N", title="Symbol"),
                alt.Tooltip("close:Q", title="Close", format=",.2f"),
                alt.Tooltip("index:Q", title="Index", format=".2f"),
                alt.Tooltip("time:T", title="Time"),
            ],
        )
        .add_params(alt.selection_interval(bind="scales", name=COMPARE_ZOOM))
        .properties(height=320)
    )
# ...existing code...


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


# ── Chart-spec cache (redraw only when the data actually changed) ─────────────
def chart_spec(cache_key: str, signature, build):
    """Rebuild a chart only when ``signature`` changes, else reuse the cached object.

    Altair derives the dataset name from the data, so an unchanged chart serialises to
    an identical Vega-Lite spec and Streamlit sends no update (no redraw).
    """
    cache = st.session_state.setdefault("_chart_specs", {})
    entry = cache.get(cache_key)
    if entry is None or entry[0] != signature:
        entry = (signature, build())
        cache[cache_key] = entry
    return entry[1]


def frames_signature(frames: dict[str, pd.DataFrame]):
    return tuple(sorted(
        (
            symbol,
            len(frame),
            str(frame["time"].iloc[-1]),
            round(float(frame["close"].iloc[-1]), 8),
            round(float(frame["low"].min()), 8),
            round(float(frame["high"].max()), 8),
        )
        for symbol, frame in frames.items()
        if not frame.empty
    ))


def tickers_signature(tickers: dict[str, dict]):
    return tuple(sorted(
        (symbol, round(record["pct_change"] or 0.0, 6))
        for symbol, record in tickers.items()
    ))


# ── Top level: connect, discover, build the sidebar ───────────────────────────
client, host, port = resolve_redis()
if client is None:
    st.error(
        "Could not reach Redis. Tried `redis`, `localhost` and `127.0.0.1` on port 6379 — "
        "set `REDIS_HOST` / `REDIS_PORT` to point somewhere else."
    )
    if st.button("Retry connection"):
        st.cache_resource.clear()
        st.rerun()
    st.stop()

tickers_boot = load_tickers(client)
klines_boot = discover_kline_streams(client)

with st.sidebar:
    st.header("Dashboard settings")
    refresh_mode = st.select_slider(
        "Refresh (quotes)",
        options=list(REFRESH_MODES),
        value="1s",
        key="_refresh",
        help="How often the quote cards and order book re-read Redis.",
    )
    chart_mode = st.select_slider(
        "Refresh (charts)",
        options=list(REFRESH_MODES),
        value="2s",
        key="_chart_refresh",
        help="Chart data is heavier to read and redraw, so it can update more slowly.",
    )
    st.divider()

    all_symbols = sorted(set(tickers_boot) | set(klines_boot))
    if not st.session_state.get("symbols") or any(
        s not in all_symbols for s in st.session_state["symbols"]
    ):
        st.session_state["symbols"] = all_symbols
    chosen = st.multiselect("Symbols", all_symbols, key="symbols")

    all_intervals = sorted({iv for ivs in klines_boot.values() for iv in ivs}, key=lambda x: x[1], reverse=True)
    preferred = "1m" if "1m" in all_intervals else (all_intervals[0] if all_intervals else None)
    if st.session_state.get("interval") not in all_intervals:
        st.session_state["interval"] = preferred
    interval = st.selectbox("Kline interval", all_intervals, key="interval")

    st.button("🔄 Refresh now", key="_refresh_now")
    st.caption("Flow: Binance → Kafka → Redis (hashes + streams) → this dashboard.")

st.title("Fishtonks — live market dashboard")

if not tickers_boot and not klines_boot:
    st.warning("No Redis data yet — waiting for the producer and Kafka Connect sinks…")
    st.caption(f"Redis `{host}:{port}` · no `ticker:*` hashes or `kline:*` streams found.")
    st.stop()

if not chosen:
    st.info("Select at least one symbol in the sidebar.")
    st.stop()


# ── Fast fragment: freshness + quote cards ────────────────────────────────────
FLASH_MS = 650      # how long a quote card stays tinted
FLASH_TTL = 0.9     # how long the rule survives on the page (one refresh cycle)
FLASH_TINTS = {"up": "33, 195, 84", "down": "255, 75, 75"}


def quote_container_key(symbol: str) -> str:
    """Key for the card's wrapper; Streamlit exposes it as ``st-key-<key>``."""
    return "quote-" + "".join(ch if ch.isalnum() else "-" for ch in symbol)


def flash_quote_moves(tickers: dict[str, dict]) -> None:
    """Tint the card of every symbol whose price moved since the last refresh.

    Styling only: Streamlit strips <script> out of markdown, so the move has to be
    expressed in CSS alone. Each move gets its *own* ``@keyframes`` name, because a
    re-render that leaves ``animation-name`` alone leaves a finished animation
    finished — a fresh name is the only thing that replays it. Runs that see no
    move emit no style element at all, so a quiet symbol never flashes.

    ``box-shadow: inset 0 0 0 9999px <tint>`` paints over the card's own
    background (theme-agnostic) and fades back to alpha 0.
    """
    now = time.monotonic()
    prices = st.session_state.setdefault("_quote_prices", {})
    flashes = st.session_state.setdefault("_quote_flashes", {})
    seq = st.session_state.get("_quote_flash_seq", 0)

    for symbol, record in tickers.items():
        price = record.get("last_price")
        if price is None:
            continue
        before = prices.get(symbol)
        if before is not None and price != before:
            seq += 1
            flashes[symbol] = ("up" if price > before else "down", seq, now)
        prices[symbol] = price
    st.session_state["_quote_flash_seq"] = seq

    rules = []
    for symbol, (direction, n, at) in list(flashes.items()):
        if now - at > FLASH_TTL:
            del flashes[symbol]
            continue
        tint = FLASH_TINTS[direction]
        name = f"stonks-flash-{n}"
        rules.append(
            f"@keyframes {name}{{"
            f"from{{box-shadow:inset 0 0 0 9999px rgba({tint},.30)}}"
            f"to{{box-shadow:inset 0 0 0 9999px rgba({tint},0)}}}}"
            f".st-key-{quote_container_key(symbol)} [data-testid=\"stMetric\"]"
            f"{{animation:{name} {FLASH_MS}ms ease-out}}"
        )
    # Emitted every run, empty or not. The cards below live in keyed containers,
    # and a fragment whose element count changes between runs makes Streamlit
    # re-insert those containers — which shows up as duplicate rows of cards.
    st.markdown(f"<style>{''.join(rules)}</style>", unsafe_allow_html=True)


@st.fragment(run_every=REFRESH_MODES[refresh_mode])
def render_quotes() -> None:
    tickers = load_tickers(client)
    newest = max(
        (r["timestamp"] for r in tickers.values() if r["timestamp"] is not None), default=None
    )
    stamp = newest.strftime("%H:%M:%S UTC") if newest is not None else "—"
    st.caption(
        f"Redis `{host}:{port}` · {len(tickers)} ticker(s) · last tick {stamp} · "
        f"quotes every {refresh_mode} · charts every {chart_mode}"
    )
    flash_quote_moves(tickers)

    for start in range(0, len(chosen), 3):
        cols = st.columns(3)
        for col, symbol in zip(cols, chosen[start:start + 3]):
            record = tickers.get(symbol)
            with col, st.container(key=quote_container_key(symbol)):
                st.metric(
                    symbol,
                    fmt_price(record["last_price"] if record else None),
                    delta=f"{record['pct_change']:+.2f}%" if record and record["pct_change"] is not None else None,
                    border=True,
                )
                if record:
                    st.caption(
                        f"24h H {fmt_price(record['high_price'])} · "
                        f"L {fmt_price(record['low_price'])} · "
                        f"spread {fmt_price(spread_of(record))}"
                    )


# ── Slow fragment: charts, window summary, candlesticks ───────────────────────
@st.fragment(run_every=REFRESH_MODES[chart_mode])
def render_klines() -> None:
    reset_compare_view()  # must run before the chart widget exists again
    frames = {
        symbol: load_kline(client, symbol, interval)
        for symbol in chosen
        if interval in klines_boot.get(symbol, [])
    }
    tickers = load_tickers(client)
    signature = frames_signature(frames)

    # ...existing code...
    chart_col, change_col = st.columns([2, 1], gap="medium")
    with chart_col:
        st.subheader(f"Close price · {interval}")
        spec = comparison_chart(frames)   # rebuilt every tick: it carries the data
        if spec is None:
            st.info(f"No `kline:*:{interval}` data for the selected symbols yet.")
        else:
            st.altair_chart(
                spec,
                width="stretch",
                key=compare_chart_key(),
                on_select="rerun",   # keeps the element id stable + saves the view state
            )
            st.caption(
                "Each series is indexed to its first loaded candle = 100, so different price "
                "scales compare. New candles are swapped in, so panning and zooming survive "
                "a refresh. The baseline is latched for the session."
            )
            if st.button("Reset view", key="_compare_reset"):
                st.session_state["_compare_reset_pending"] = True
                st.rerun()
    with change_col:
            # ...existing code...
        st.subheader("24h change")
        change = chart_spec("change", tickers_signature(tickers), lambda: change_chart(tickers))
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
                vol = chart_spec("volatility", signature, lambda: volatility_chart(summary))
                if vol is None:
                    st.info("Not enough candles to rank volatility yet.")
                else:
                    st.altair_chart(vol, width="stretch")
            st.caption(
                f"Up to {MAX_KLINE_POINTS} candles per stream · "
                "volatility = std dev of candle-to-candle close returns."
            )

    # ── Candlesticks ────────────────────────────────────────────────────
    st.subheader("Candlesticks")
    if st.session_state.get("candle_symbol") not in chosen:
        st.session_state["candle_symbol"] = chosen[0]
    candle_symbol = st.selectbox("Symbol for candles", chosen, key="candle_symbol")
    candle_frame = frames.get(candle_symbol)
    if candle_frame is None or candle_frame.empty:
        st.info(f"No candles for {candle_symbol} at {interval}.")
        return

    latest = candle_frame.iloc[-1]
    candle_signature = (candle_symbol, interval, len(candle_frame), str(latest["time"]), round(float(latest["close"]), 8))
    spec = chart_spec(
        "candles", candle_signature, lambda: candlestick_chart(candle_frame, f"{candle_symbol} · {interval}")
    )
    st.altair_chart(spec, width="stretch")
    st.caption(
        f"{candle_symbol} @ {latest['time']:%H:%M:%S} UTC · "
        f"O {fmt_price(latest['open'])} · H {fmt_price(latest['high'])} · "
        f"L {fmt_price(latest['low'])} · C {fmt_price(latest['close'])}"
    )
    with st.expander("Raw candle data"):
        st.dataframe(candle_frame.tail(50), hide_index=True, width="stretch")


# ── Order book snapshot ───────────────────────────────────────────────────────
@st.fragment(run_every=REFRESH_MODES[refresh_mode])
def render_book() -> None:
    tickers = load_tickers(client)
    if not tickers:
        st.info("Waiting for ticker data…")
        return
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
    st.subheader("Order book snapshot")
    st.dataframe(
        pd.DataFrame(rows),
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


render_quotes()
render_klines()
render_book()
