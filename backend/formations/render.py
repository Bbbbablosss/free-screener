"""
Server-side render of a formation chart PNG — the blue-theme chart (candles +
level + info panel + Crypto watermark) that the acer levels_screener sends to
Telegram, reproduced on the VPS.

Why here and not on acer: the РФ→VPS HTTPS path stalls on uploads past a few tens
of KB, so the ~100 KB PNG can't be pushed from acer. Instead acer pushes the small
signal (level price + 24h metrics) and the VPS renders from its own charts.db
candles (binance_futures, kept fresh by the closed-bar stream).

Ported from levels_screener/chart.py. Simplified: draws the signal's horizontal
level (the common case — breakout/bounce/retest/potential_breakout); trendline
strategies render without the diagonal for now. Heavy deps (matplotlib/mplfinance/
pandas/numpy) are imported lazily on first render to keep the web's base RSS down.
"""
from __future__ import annotations

import io
import logging
import threading
from pathlib import Path

logger = logging.getLogger(__name__)

CHART_BARS = 300
_ASSETS = Path(__file__).resolve().parent / "assets"
_LOGO_PATH = _ASSETS / "logo.png"
_CJK_PATH = _ASSETS / "NotoSansSC-CJK.ttf"

THEME = {
    "page_bg": "#010d1a", "chart_bg": "#010d1a", "grid": "#091824",
    "up": "#0A9D61", "down": "#DA2647", "accent": "#5A99DD",
    "text": "#a2b4c8", "muted": "#4a6a8a", "level": "#2e5880",
    "vol": "#16344f", "warn": "#DA2647", "good": "#0A9D61",
}
STRATEGY_NAMES = {
    "potential_breakout": "Potential Level Breakout",
    "breakout": "Level Breakout", "bounce": "Level Bounce",
    "retest": "Level Retest", "trendline_bounce": "Trendline Bounce",
    "trendline_breakout": "Trendline Breakout", "consolidation": "Consolidation",
}
# touch tolerance per TF (from levels_screener config.TF_PARAMS) — where the level
# ray starts (first candle that touches the level within the window).
TOUCH = {"1m": 0.0015, "15m": 0.0035, "1h": 0.0050}

T = THEME
_init_done = False
_logo_cache = None
_cjk_name = None


def _init_mpl():
    global _init_done, _cjk_name
    if _init_done:
        return
    import matplotlib
    matplotlib.use("Agg")
    matplotlib.rcParams["timezone"] = "UTC"
    # Dark DEFAULTS so any panel that (very rarely) misses explicit styling is created
    # dark, not matplotlib's default WHITE. matplotlib applies axes.facecolor at axes
    # CREATION time, so with a dark default there is no white window for a race/edge
    # case to slip through → kills the intermittent white-chart / white-volume PNG for
    # good. Safe: formations is the process's ONLY matplotlib user.
    matplotlib.rcParams["figure.facecolor"] = T["page_bg"]
    matplotlib.rcParams["axes.facecolor"] = T["chart_bg"]
    matplotlib.rcParams["savefig.facecolor"] = T["page_bg"]
    matplotlib.rcParams["savefig.edgecolor"] = T["page_bg"]
    try:
        if _CJK_PATH.exists():
            from matplotlib import font_manager as fm
            fm.fontManager.addfont(str(_CJK_PATH))
            _cjk_name = fm.FontProperties(fname=str(_CJK_PATH)).get_name()
            matplotlib.rcParams["font.family"] = ["DejaVu Sans", _cjk_name]
            import logging as _l
            _l.getLogger("matplotlib.font_manager").setLevel(_l.ERROR)
    except Exception:
        _cjk_name = None
    _init_done = True


def _style():
    import mplfinance as mpf
    mc = mpf.make_marketcolors(
        up=T["up"], down=T["down"],
        edge={"up": T["up"], "down": T["down"]},
        wick={"up": T["up"], "down": T["down"]},
        volume={"up": T["vol"], "down": T["vol"]},
    )
    return mpf.make_mpf_style(
        marketcolors=mc, facecolor=T["chart_bg"], edgecolor=T["grid"],
        figcolor=T["page_bg"], gridcolor=T["grid"], gridstyle="-", y_on_right=False,
        rc={
            "font.family": ["DejaVu Sans"] + ([_cjk_name] if _cjk_name else []),
            "axes.labelcolor": T["muted"], "xtick.color": T["muted"],
            "ytick.color": T["muted"], "axes.edgecolor": T["grid"],
            "text.color": T["text"], "font.size": 10,
        },
    )


def _load_logo():
    global _logo_cache
    if _logo_cache is None:
        try:
            import matplotlib.image as mpimg
            arr = mpimg.imread(str(_LOGO_PATH)).astype(float).copy()
            if arr.max() > 1.5:
                arr /= 255.0
            if arr.shape[2] == 4:
                arr[..., 3] *= 0.05            # watermark opacity (faint, per user)
            _logo_cache = arr
        except Exception:
            _logo_cache = False
    return _logo_cache


def _fmt(v: float) -> str:
    if v >= 1000:
        return f"{v:,.1f}"
    if v >= 1:
        return f"{v:.4g}"
    return f"{v:.6g}"


def _fmt_money(v: float) -> str:
    for div, suf in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if v >= div:
            return f"${v / div:.2f}{suf}"
    return f"${v:.0f}"


def _price_tag(ax, price, *, fill, fg, strong=False):
    ax.text(1.004, price, f" {_fmt(price)} ", transform=ax.get_yaxis_transform(),
            ha="left", va="center", color=fg,
            fontsize=8.5 if strong else 8,
            fontweight="bold" if strong else "normal",
            bbox=dict(boxstyle="round,pad=0.2", fc=fill, ec="none"),
            clip_on=False, zorder=6)


def _header(ax, symbol, tf, metrics):
    """Single top row: ticker + TF pill on the LEFT, 24h metric blocks on the RIGHT
    (small uppercase label above the value), like the site's stat bar."""
    fig = ax.figure
    try:
        renderer = fig.canvas.get_renderer()
        inv = ax.transAxes.inverted()
    except Exception:
        renderer = None

    def _w(txt):  # rendered text width in axes-fraction units
        if renderer is None:
            return 0.12
        bb = txt.get_window_extent(renderer=renderer)
        return float(inv.transform((bb.x1, 0))[0] - inv.transform((bb.x0, 0))[0])

    # ticker (left) + TF pill right after it
    tk = ax.text(0.0, 1.062, symbol, transform=ax.transAxes, ha="left", va="center",
                 color="#eaf0f6", fontsize=19, fontweight="bold", zorder=5)
    ax.text(_w(tk) + 0.015, 1.062, tf, transform=ax.transAxes, ha="left", va="center",
            color=T["text"], fontsize=11.5, fontweight="bold", zorder=5,
            bbox=dict(boxstyle="round,pad=0.34", fc="#0b1a2a", ec=T["grid"], lw=0.8))

    # metric blocks on the right (label above value); rightmost = NATR
    blocks = []
    vol = metrics.get("turnover24h")
    if vol:
        blocks.append(("VOLUME 24H", _fmt_money(float(vol)), "#d6e2ee"))
    pcnt = metrics.get("price24hPcnt")
    if pcnt is not None:
        blocks.append(("CHANGE 24H", f"{float(pcnt):+.2f}%",
                       T["good"] if float(pcnt) >= 0 else T["warn"]))
    natr = metrics.get("natr")
    if natr is not None:
        blocks.append(("NATR 5M", f"{float(natr):.2f}", "#d6e2ee"))

    x = 1.0
    for label, value, vcol in reversed(blocks):
        vt = ax.text(x, 1.038, value, transform=ax.transAxes, ha="right", va="center",
                     color=vcol, fontsize=13, fontweight="bold", zorder=5)
        lt = ax.text(x, 1.092, label, transform=ax.transAxes, ha="right", va="center",
                     color=T["muted"], fontsize=8.5, fontweight="bold", zorder=5)
        x -= max(_w(vt), _w(lt)) + 0.04


# matplotlib's pyplot state machine + rcParams are process-global and NOT
# thread-safe. main.py renders via asyncio.to_thread(render_formation, ...), so
# concurrent formation ingests draw in parallel worker threads and race on the
# shared figure manager / facecolor rcParams (set by make_mpf_style rc=…). Very
# rarely a PNG gets saved before the dark facecolor is applied → a white-background
# chart. Serialize every render behind one lock: rendering is fast + CPU-bound so
# throughput is unaffected, and it stays off the event loop (still inside to_thread).
_render_lock = threading.Lock()


def render_formation(*, symbol, tf, strategy, direction, level=None,
                     line=None, metrics=None, candles) -> bytes | None:
    """candles = list of [ts_ms, o, h, l, c, v] (wire format).
    line = [[ts1_ms, price1], [ts2_ms, price2]] for trendline strategies (drawn
    as a ray extended to the right edge). Returns PNG bytes or None."""
    with _render_lock:
        return _render_formation_impl(
            symbol=symbol, tf=tf, strategy=strategy, direction=direction,
            level=level, line=line, metrics=metrics, candles=candles,
        )


def _render_formation_impl(*, symbol, tf, strategy, direction, level=None,
                           line=None, metrics=None, candles) -> bytes | None:
    if not candles or len(candles) < 20:
        return None
    _init_mpl()
    import numpy as np
    import pandas as pd
    import mplfinance as mpf
    from matplotlib.offsetbox import AnnotationBbox, OffsetImage
    from matplotlib.ticker import MaxNLocator
    import matplotlib.pyplot as plt

    rows = candles[-CHART_BARS:]
    ts = np.array([int(r[0]) for r in rows], dtype="int64")
    o = np.array([float(r[1]) for r in rows], dtype="float64")
    h = np.array([float(r[2]) for r in rows], dtype="float64")
    lo = np.array([float(r[3]) for r in rows], dtype="float64")
    cl = np.array([float(r[4]) for r in rows], dtype="float64")
    vol = np.array([float(r[5]) if len(r) > 5 and r[5] is not None else 0.0 for r in rows], dtype="float64")
    vis = len(rows)

    idx = pd.to_datetime(ts, unit="ms", utc=True)
    df = pd.DataFrame({"Open": o, "High": h, "Low": lo, "Close": cl, "Volume": vol}, index=idx)

    fig, axes = mpf.plot(
        df, type="candle", style=_style(), volume=True,
        ylabel="", ylabel_lower="", figratio=(16, 9), figscale=1.25,
        tight_layout=True, returnfig=True, xrotation=0,
        scale_padding={"left": 0.25, "right": 0.85, "top": 2.7, "bottom": 0.35},
        update_width_config=dict(candle_linewidth=0.7, candle_width=0.62),
    )
    ax = axes[0]
    fig.set_facecolor(T["page_bg"])   # belt-and-suspenders on the figure patch itself
    # Dark background on EVERY panel. mplfinance returns axes as (base, y-twin) pairs;
    # the base panels (even indices) get the chart bg, the y-twins (odd) go transparent so
    # they never paint a white patch over the panel. The VOLUME panel (axes[2]) was rendering
    # with matplotlib's default WHITE facecolor — the mpf style's facecolor didn't propagate
    # to the secondary volume axes → a white block under the volume bars.
    for i, a in enumerate(axes):
        a.set_facecolor(T["chart_bg"] if i % 2 == 0 else "none")
    for a in axes[2:]:
        a.tick_params(left=False, right=False, labelleft=False, labelright=False)
        a.set_ylabel("")
    ax.yaxis.set_major_locator(MaxNLocator(nbins=10, steps=[1, 2, 2.5, 5, 10]))
    pad = max(6, int(round(vis * 0.05)))
    right_edge = vis - 1 + pad
    ax.set_xlim(-0.5, right_edge)

    cur = float(cl[-1])

    # logo watermark
    logo = _load_logo()
    if logo is not False and logo is not None:
        oi = OffsetImage(logo, zoom=0.42)
        ab = AnnotationBbox(oi, (0.5, 0.52), xycoords="axes fraction",
                            frameon=False, zorder=0.4, box_alignment=(0.5, 0.5))
        ax.add_artist(ab)

    # signal level: solid ray from its first touch in the window → right edge
    if level is not None:
        try:
            lvl = float(level)
        except (TypeError, ValueError):
            lvl = None
        if lvl and lvl > 0:
            tol = TOUCH.get(tf, 0.005)
            m = ((np.abs(h - lvl) / lvl <= tol) | (np.abs(lo - lvl) / lvl <= tol))
            w = np.where(m)[0]
            x0 = int(w[0]) if len(w) else max(0, vis - int(vis * 0.6))
            ax.plot([x0, right_edge], [lvl, lvl], color=T["accent"], lw=1.7,
                    ls="-", alpha=0.98, zorder=3)
            _price_tag(ax, lvl, fill=T["accent"], fg="#06121f", strong=True)

    # trendline: the two (ts, price) points define the line in (time, price) space.
    # Convert ts → fractional candle-x (bar units from the first candle; the pivot
    # may be older than the window → x < 0, which is fine) and draw the VISIBLE
    # segment to the right edge at the line's true value (don't clamp the anchor's
    # price — that was the "crooked line" bug).
    if line and len(line) == 2:
        try:
            ts1, p1 = float(line[0][0]), float(line[0][1])
            ts2, p2 = float(line[1][0]), float(line[1][1])
            bar_ms = float(ts[1] - ts[0]) if len(ts) > 1 else 60000.0
            if bar_ms > 0 and p1 > 0 and p2 > 0:
                t0 = float(ts[0])
                x1 = (ts1 - t0) / bar_ms
                x2 = (ts2 - t0) / bar_ms
                if x2 != x1:
                    slope = (p2 - p1) / (x2 - x1)
                    xa = max(x1, 0.0)
                    ya = p1 + slope * (xa - x1)
                    yb = p1 + slope * (right_edge - x1)
                    ax.plot([xa, right_edge], [ya, yb], color=T["accent"],
                            lw=1.8, alpha=0.98, zorder=3)
        except (TypeError, ValueError, IndexError):
            pass

    # current price: dashed ray from last candle
    up = cl[-1] >= o[-1]
    cur_col = T["good"] if up else T["warn"]
    ax.plot([vis - 1, right_edge], [cur, cur], color=cur_col, lw=0.8,
            alpha=0.9, dashes=(4, 3), zorder=2.7)
    _price_tag(ax, cur, fill=cur_col, fg="#06121f", strong=True)

    # header row: ticker + TF (left) + 24h metrics (right) — no centered title/badge
    _header(ax, symbol, tf, metrics or {})

    fig.text(0.995, 0.012, "UTC", ha="right", va="bottom", fontsize=7.5,
             color=T["muted"], zorder=5)

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=130, facecolor=T["page_bg"],
                bbox_inches="tight", pad_inches=0.18)
    plt.close(fig)
    buf.seek(0)
    return buf.getvalue()
