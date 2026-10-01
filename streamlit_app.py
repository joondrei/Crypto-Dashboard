# import os
# import threading
# import time
# from datetime import datetime, timezone

# import altair as alt
# import pandas as pd
# import redis
# import streamlit as st

# # ── Page setup ────────────────────────────────────────────────────────────────
# st.set_page_config(
#     page_title="Stonks · Live Tickers",
#     page_icon="📈",
#     layout="wide",
# )

# TICKER_FIELDS = (
#     "bid_price", "bid_qty", "ask_price", "ask_qty",
#     "last_price", "pct_change", "avg_price",
#     "open_price", "high_price", "low_price",
# )
# MAX_POINTS = 600  # price points kept per symbol in session history
# LIVE_POLL_FALLBACK = 0.5  # seconds, used when keyspace notifications are unavailable
# LIVE_HEARTBEAT = 30.0  # seconds, max wait between refreshes even with no events
# REFRESH_MODES = ["Live", "1s", "2s", "5s", "10s", "30s", "Off"]

# GREEN, RED = "#21c354", "#ff4b4b"


# # ── Redis access ──────────────────────────────────────────────────────────────
# @st.cache_resource(show_spinner=False)
# def _redis_client(host: str, port: int) -> redis.Redis:
#     return redis.Redis(
#         host=host,
#         port=port,
#         decode_responses=True,
#         socket_connect_timeout=2,
#         socket_timeout=2,
#     )


# def resolve_redis():
#     """Find a reachable Redis: REDIS_HOST/REDIS_PORT env vars first, then defaults.

#     Tries the docker-network hostname `redis` before localhost so the same code
#     works both inside the compose stack and when run locally via poetry.
#     """
#     candidates = []
#     if os.environ.get("REDIS_HOST"):
#         candidates.append((os.environ["REDIS_HOST"], int(os.environ.get("REDIS_PORT", "6379"))))
#     candidates += [("redis", 6379), ("localhost", 6379), ("127.0.0.1", 6379)]

#     for host, port in dict.fromkeys(candidates):
#         try:
#             client = _redis_client(host, port)
#             client.ping()
#             return client, host, port
#         except (redis.RedisError, OSError):
#             continue
#     return None, None, None


# class RedisChangeTracker:
#     """Wakes the dashboard the moment a matching Redis key is written.

#     Subscribes to keyspace notifications (`__keyspace@*__:*`) and exposes a
#     monotonically increasing event counter. Dashboard sessions remember the
#     counter value they last rendered and sleep until it increases.
#     """

#     CHANNEL = "__keyspace@*__:*"

#     def __init__(self, client: redis.Redis):
#         self._client = client
#         self._cond = threading.Condition()
#         self._counter = 0
#         self._stop = threading.Event()
#         self._live = False  # pubsub connected and receiving events
#         self._notified = False  # notifications enabled on the server
#         self._thread: threading.Thread | None = None

#     # -- lifecycle ------------------------------------------------------------
#     def start(self) -> "RedisChangeTracker":
#         self._enable_notifications()
#         self._thread = threading.Thread(target=self._listen, name="redis-change-tracker", daemon=True)
#         self._thread.start()
#         return self

#     def _enable_notifications(self) -> None:
#         """Turn on keyspace events at runtime (no redis.conf change needed)."""
#         try:
#             current = self._client.config_get("notify-keyspace-events").get("notify-keyspace-events", "")
#             wanted = "".join(sorted(set(current.upper()) | set("KEA")))
#             if wanted != current:
#                 self._client.config_set("notify-keyspace-events", wanted)
#             self._notified = True
#         except (redis.RedisError, OSError):
#             self._notified = False  # e.g. CONFIG disabled on managed Redis

#     def _listen(self) -> None:
#         while not self._stop.is_set():
#             try:
#                 with self._client.pubsub(ignore_subscribe_messages=True) as pubsub:
#                     pubsub.psubscribe(self.CHANNEL)
#                     with self._cond:
#                         self._live = True
#                     for message in pubsub.listen():
#                         if self._stop.is_set():
#                             break
#                         with self._cond:
#                             self._counter += 1
#                             self._cond.notify_all()
#             except (redis.RedisError, OSError):
#                 pass
#             with self._cond:
#                 self._live = False
#             if not self._stop.is_set():
#                 self._stop.wait(1.0)  # reconnect with a small backoff

#     # -- queries --------------------------------------------------------------
#     @property
#     def status(self) -> str:
#         """One of: 'events' (receiving keyspace events), 'connecting' (pubsub
#         starting up / retrying), or 'polling' (notifications unsupported)."""
#         if not self._notified:
#             return "polling"
#         return "events" if self._live else "connecting"

#     @property
#     def notifications_enabled(self) -> bool:
#         return self._notified

#     def current(self) -> int:
#         with self._cond:
#             return self._counter

#     def wait_for_change(self, snapshot: int, timeout: float) -> tuple[int, bool]:
#         """Block until the counter exceeds `snapshot` (or timeout). Returns (new, fired)."""
#         with self._cond:
#             fired = self._cond.wait_for(lambda: self._counter != snapshot, timeout=timeout)
#             return self._counter, fired


# @st.cache_resource(show_spinner=False)
# def _change_tracker(host: str, port: int) -> RedisChangeTracker:
#     return RedisChangeTracker(_redis_client(host, port)).start()


# def load_tickers(client: redis.Redis, pattern: str) -> dict[str, dict]:
#     """Read every hash matching `pattern` into a {symbol: record} dict."""
#     tickers: dict[str, dict] = {}
#     for key in client.scan_iter(match=pattern):
#         try:
#             raw = client.hgetall(key)
#         except redis.ResponseError:
#             continue  # not a hash (e.g. a string/stream key that happens to match)
#         if not raw:
#             continue
#         record = {"key": key}
#         for field in TICKER_FIELDS:
#             try:
#                 record[field] = float(raw[field])
#             except (KeyError, TypeError, ValueError):
#                 record[field] = None
#         record["timestamp"] = pd.to_datetime(raw["timestamp"]) if raw.get("timestamp") else None
#         record["symbol"] = raw.get("symbol") or key.split(":")[-2]
#         tickers[record["symbol"]] = record
#     return dict(sorted(tickers.items()))


# def update_history(tickers: dict[str, dict]) -> None:
#     """Append each *new* exchange tick to the in-session price history.

#     Points are keyed by the tick's own timestamp, so re-reading an unchanged
#     hash never adds a duplicate point.
#     """
#     history: dict[str, list] = st.session_state.setdefault("price_history", {})
#     for symbol, record in tickers.items():
#         if record["last_price"] is None:
#             continue
#         points = history.setdefault(symbol, [])
#         tick_ts = record["timestamp"] if record["timestamp"] is not None else pd.Timestamp.now(tz="UTC")
#         if points and points[-1][0] == tick_ts:
#             continue
#         points.append((tick_ts, record["last_price"]))
#         del points[:-MAX_POINTS]


# # ── Formatting helpers ────────────────────────────────────────────────────────
# def fmt_price(value: float | None) -> str:
#     if value is None:
#         return "—"
#     if value >= 1000:
#         return f"{value:,.2f}"
#     if value >= 1:
#         return f"{value:,.4f}".rstrip("0").rstrip(".")
#     return f"{value:.6f}".rstrip("0").rstrip(".")


# def spread_of(record: dict) -> float | None:
#     if record["ask_price"] is None or record["bid_price"] is None:
#         return None
#     return record["ask_price"] - record["bid_price"]


# # ── Chart builders ────────────────────────────────────────────────────────────
# def history_chart() -> alt.Chart | None:
#     """Line chart of prices observed while the page has been open, rebased to 100."""
#     rows = []
#     for symbol, points in st.session_state.get("price_history", {}).items():
#         if not points:
#             continue
#         base = points[0][1]
#         for ts, price in points:
#             rows.append({"time": ts, "symbol": symbol, "price": price, "indexed": price / base * 100})
#     df = pd.DataFrame(rows)
#     if df.empty or df.groupby("symbol").size().max() < 2:
#         return None
#     return alt.Chart(df).mark_line().encode(
#         x=alt.X("time:T", title=None),
#         y=alt.Y("indexed:Q", title="price (session start = 100)"),
#         color=alt.Color("symbol:N", title=None),
#         tooltip=[
#             alt.Tooltip("symbol:N", title="Symbol"),
#             alt.Tooltip("price:Q", title="Price", format=",.4f"),
#             alt.Tooltip("indexed:Q", title="Index", format=".2f"),
#         ],
#     ).interactive()


# def change_chart(tickers: dict[str, dict]) -> alt.Chart:
#     """Bar chart of the 24h percent change reported by each ticker."""
#     df = pd.DataFrame([
#         {"symbol": sym, "pct": rec["pct_change"] if rec["pct_change"] is not None else 0.0}
#         for sym, rec in tickers.items()
#     ])
#     return alt.Chart(df).mark_bar().encode(
#         x=alt.X("symbol:N", sort=list(df["symbol"]), title=None),
#         y=alt.Y("pct:Q", title="24h change (%)"),
#         color=alt.condition("datum.pct >= 0", alt.value(GREEN), alt.value(RED)),
#         tooltip=[
#             alt.Tooltip("symbol:N", title="Symbol"),
#             alt.Tooltip("pct:Q", title="24h %", format="+.3f"),
#         ],
#     )


# def tickers_frame(tickers: dict[str, dict]) -> pd.DataFrame:
#     rows = []
#     for sym, r in tickers.items():
#         rows.append({
#             "Symbol": sym,
#             "Last": r["last_price"],
#             "Bid": r["bid_price"],
#             "Ask": r["ask_price"],
#             "Spread": spread_of(r),
#             "24h %": r["pct_change"],
#             "Open": r["open_price"],
#             "High": r["high_price"],
#             "Low": r["low_price"],
#             "Avg": r["avg_price"],
#             "Bid Qty": r["bid_qty"],
#             "Ask Qty": r["ask_qty"],
#             "Updated": r["timestamp"],
#         })
#     return pd.DataFrame(rows)


# # ── Refresh strategies ────────────────────────────────────────────────────────
# def wait_for_next_refresh(mode: str) -> None:
#     """Block until the dashboard should re-render, then trigger a fragment rerun.

#     Live: sleeps until Redis emits a keyspace event (with a heartbeat cap so a
#     silent Redis still refreshes periodically and errors self-heal).
#     Fixed modes: sleeps the chosen interval. Off: returns without re-rendering.
#     """
#     if mode == "Off" or os.environ.get("STREAMLIT_DASHBOARD_NO_WAIT"):
#         return

#     if mode == "Live":
#         tracker = _change_tracker(*st.session_state["_redis_addr"])
#         if tracker.status == "events":
#             # Sleep until Redis reports a write to any key (heartbeat cap so a
#             # silent Redis still refreshes periodically and drops self-heal).
#             tracker.wait_for_change(tracker.current(), timeout=LIVE_HEARTBEAT)
#         else:
#             # Notifications unsupported, or pubsub still connecting: poll fast.
#             time.sleep(1.0 if tracker.status == "connecting" else LIVE_POLL_FALLBACK)
#         st.rerun(scope="fragment")

#     seconds = float(mode.rstrip("s"))
#     time.sleep(seconds)
#     st.rerun(scope="fragment")


# # ── Dashboard fragment ────────────────────────────────────────────────────────
# @st.fragment
# def dashboard() -> None:
#     client, host, port = resolve_redis()
#     if client is None:
#         st.error(
#             "Could not reach Redis. Tried `redis`, `localhost` and `127.0.0.1` on port 6379 — "
#             "set REDIS_HOST / REDIS_PORT to point somewhere else."
#         )
#         return
#     st.session_state["_redis_addr"] = (host, port)

#     tickers = load_tickers(client, pattern)
#     if not tickers:
#         st.warning(f"No Redis keys match `{pattern}`. Waiting for the producer to publish tickers…")
#         wait_for_next_refresh(st.session_state["_mode"])
#         return

#     update_history(tickers)

#     newest = max((r["timestamp"] for r in tickers.values() if r["timestamp"] is not None), default=None)
#     stamp = newest.strftime("%H:%M:%S UTC") if newest is not None else "—"
#     mode = st.session_state["_mode"]
#     if mode == "Live":
#         status = _change_tracker(host, port).status
#         status_note = {
#             "events": "listening for Redis changes",
#             "connecting": "connecting to change stream…",
#             "polling": f"notifications unavailable — polling every {LIVE_POLL_FALLBACK:g}s",
#         }[status]
#         mode_note = f" · live ({status_note})"
#     elif mode == "Off":
#         mode_note = " · paused"
#     else:
#         mode_note = f" · every {mode}"
#     st.caption(f"Redis `{host}:{port}` · {len(tickers)} ticker(s) · newest tick {stamp}{mode_note}")

#     # Metric cards, 3 per row
#     symbols = list(tickers)
#     for start in range(0, len(symbols), 3):
#         cols = st.columns(3)
#         for col, symbol in zip(cols, symbols[start:start + 3]):
#             record = tickers[symbol]
#             change = record["pct_change"]
#             with col:
#                 st.metric(
#                     symbol,
#                     fmt_price(record["last_price"]),
#                     delta=f"{change:+.2f}%" if change is not None else None,
#                     border=True,
#                 )
#                 st.caption(
#                     f"24h H {fmt_price(record['high_price'])} · "
#                     f"L {fmt_price(record['low_price'])} · "
#                     f"spread {fmt_price(spread_of(record))}"
#                 )

#     # Charts
#     chart_col, change_col = st.columns([2, 1], gap="medium")
#     with chart_col:
#         st.subheader("Price since page load")
#         chart = history_chart()
#         if chart is None:
#             st.info("Collecting price history… new ticks will appear here automatically.")
#         else:
#             st.altair_chart(chart, width="stretch")
#     with change_col:
#         st.subheader("24h change")
#         st.altair_chart(change_chart(tickers), width="stretch")

#     # Full snapshot table
#     st.subheader("Order book snapshot")
#     st.dataframe(
#         tickers_frame(tickers),
#         hide_index=True,
#         width="stretch",
#         column_config={
#             "Symbol": st.column_config.TextColumn(width="small"),
#             "Last": st.column_config.NumberColumn(format="%.2f"),
#             "Bid": st.column_config.NumberColumn(format="%.2f"),
#             "Ask": st.column_config.NumberColumn(format="%.2f"),
#             "Spread": st.column_config.NumberColumn(format="%.2f"),
#             "24h %": st.column_config.NumberColumn(format="%+.2f%%"),
#             "Open": st.column_config.NumberColumn(format="%.2f"),
#             "High": st.column_config.NumberColumn(format="%.2f"),
#             "Low": st.column_config.NumberColumn(format="%.2f"),
#             "Avg": st.column_config.NumberColumn(format="%.2f"),
#             "Bid Qty": st.column_config.NumberColumn(format="%.4f"),
#             "Ask Qty": st.column_config.NumberColumn(format="%.4f"),
#             "Updated": st.column_config.DatetimeColumn(format="HH:mm:ss"),
#         },
#     )

#     wait_for_next_refresh(mode)


# # ── Sidebar controls ──────────────────────────────────────────────────────────
# with st.sidebar:
#     st.header("Dashboard settings")
#     st.select_slider(
#         "Refresh",
#         options=REFRESH_MODES,
#         value="Live",
#         help="Live: re-renders the instant a matching Redis key changes (keyspace events). "
#              "Other options poll at a fixed rate.",
#         key="_mode",
#     )
#     if st.session_state.get("_mode") == "Live":
#         addr = st.session_state.get("_redis_addr")
#         if addr:
#             status = _change_tracker(*addr).status
#             st.caption({
#                 "events": "🟢 event-driven",
#                 "connecting": "🟡 connecting to change stream…",
#                 "polling": f"🟡 fallback polling ({LIVE_POLL_FALLBACK:g}s)",
#             }[status])
#     pattern = st.text_input("Redis key pattern", value="*ticker")
#     st.button("🔄 Refresh now", key="_refresh_now")
#     st.caption("Flow: Binance streams → Kafka → Redis hashes (`HSET`) → this dashboard.")

# st.title("📈 Stonks — live ticker dashboard")
# dashboard()
