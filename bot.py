#!/usr/bin/env python3
"""Discord price bot: watches channel messages for CAs / $tickers and
posts the price via the target webhook.

- CA (EVM 0x…hex / Solana base58)      -> DexScreener token API:
  price, 24h change, chain, liquidity, FDV/MC, 24h vol, txns, pair link.
- $TICKER or bare ALL-CAPS ticker      -> CoinGecko search + simple price:
  price, 24h change, market cap, 24h vol, rank.

Realtime idle job: Discord gateway websocket kept alive ~5.5 h (Actions
cron restarts the job every 5 h). Reading needs the bot in the guild with
message_content intent enabled; replies go through the channel webhook.

Exits 1 immediately on permanent auth failures (invalid token).
Env: DC_BOT_TOKEN, DC_PRICE_WEBHOOK, optional CHANNELS, WATCH_GUILD.
"""
import json, os, random, re, subprocess, sys, threading, time
from collections import OrderedDict

import requests
import websocket

BOT_TOKEN = os.environ.get("DC_BOT_TOKEN", "")
WEBHOOK = os.environ.get("DC_PRICE_WEBHOOK", "")
CHANNELS = {c for c in (os.environ.get("CHANNELS") or "").replace(" ", "").split(",") if c}
WATCH_GUILD = os.environ.get("WATCH_GUILD", "")
ONCE_SECONDS = int(os.environ.get("ONCE_SECONDS", "420"))

INTENTS = 32768 | 512          # message_content + guild_messages

_session = requests.Session()
_session.headers["User-Agent"] = "ca-ticker-price-bot/1.0"
POSTED = 0
STOP = False                   # permanent failure -> exit 1
HB_THREAD_ON = False

CA_RE = re.compile(r"\b(0x[a-fA-F0-9]{40})\b")
SOL_RE = re.compile(r"\b([1-9A-HJ-NP-Za-km-z]{32,44})\b")
# default: hanya $TICKER yang memicu lookup; kata ALL-CAPS polos bisa diaktifkan
# lewat secret/variable PLAIN_TICKERS=1
PLAIN_TICKERS = os.environ.get("PLAIN_TICKERS", "0") == "1"
TICK_RE = re.compile(r"[$]([A-Za-z][A-Za-z0-9]{1,15})\b")
TICK_PLAIN_RE = re.compile(r"\b([A-Z][A-Z0-9]{1,9})\b")

GECKO_NET = {"ethereum": "eth", "binance smart chain": "bsc", "smart chain": "bsc",
             "solana": "solana", "base": "base", "polygon pos": "polygon_pos",
             "arbitrum": "arbitrum", "optimism": "optimism", "avalanche": "avax",
             "robinhood": "robinhood"}

STOPWORDS = {
    "DD", "GM", "GN", "GZ", "OK", "MC", "FDV", "ATH", "ATL", "CA", "AMA",
    "POV", "NFT", "LP", "APY", "APR", "DCA", "NGMI", "WAGMI", "LFG", "OMG",
    "BRB", "IDK", "FYI", "BTW", "ASAP", "ETA", "TG", "WTF", "LOL", "GG",
    "OG", "DAO", "ICO", "IEO", "PSA", "SEC", "REMINDER",
}
DEDUPE = OrderedDict()          # message id -> timestamp


# ---------------------------------------------------------------- lookups

def dexscreener_ca(addr):
    """(best_pair, others) atau None jika token tak terdaftar."""
    r = _session.get(f"https://api.dexscreener.com/latest/dex/tokens/{addr}",
                     timeout=(10, 30))
    if r.status_code != 200:
        raise RuntimeError(f"dexscreener HTTP {r.status_code}")
    pairs = (r.json() or {}).get("pairs") or []
    if not pairs:
        return None
    best = max(pairs, key=lambda p: (p.get("liquidity") or {}).get("usd") or 0)
    return best, len(pairs) - 1


def gecko_ca(addr, chain):
    """Fallback GeckoTerminal; membutuhkan harga atau None (raiss 429 -> None)."""
    net = GECKO_NET.get(chain)
    if not net:
        return None
    r = _session.get(f"https://api.geckoterminal.com/api/v2/networks/{net}/tokens/{addr}",
                     timeout=(10, 30))
    if r.status_code != 200:
        return None
    attr = ((r.json() or {}).get("data") or {}).get("attributes") or {}
    price = attr.get("price_usd")
    if not price:
        return None
    return {"priceUsd": price, "symbol": attr.get("symbol") or "?",
            "name": attr.get("name") or "Token", "network_attr": attr.get("address"),
            "net": net}


def gecko_ticker(tick):
    s = _session.get(f"https://api.coingecko.com/api/v3/search?query={tick}",
                     timeout=(10, 30))
    if s.status_code != 200:
        raise RuntimeError(f"gecko search HTTP {s.status_code}")
    coins = (s.json() or {}).get("coins") or []
    hit = next((c for c in coins
                if (c.get("symbol") or "").lower() == tick.lower()), None)
    if not hit:
        return None
    cid = hit["id"]
    p = _session.get(
        f"https://api.coingecko.com/api/v3/simple/price?ids={cid}"
        f"&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true"
        f"&include_24hr_change=true", timeout=(10, 30))
    if p.status_code != 200:
        raise RuntimeError(f"gecko price HTTP {p.status_code}")
    data = (p.json() or {}).get(cid) or {}
    if not data.get("usd"):
        return None
    return {**data, "name": hit.get("name"), "symbol": tick,
            "rank": hit.get("market_cap_rank")}


# ---------------------------------------------------------------- discord

def deliver(body):
    global POSTED
    data = json.dumps(body).encode()
    for _ in range(3):
        try:
            r = _session.post(WEBHOOK, data=data,
                              headers={"Content-Type": "application/json"},
                              timeout=30)
        except Exception as e:
            print(f"webhook error: {e}", file=sys.stderr)
            time.sleep(3)
            continue
        if r.status_code in (200, 204):
            POSTED += 1
            return True
        if (r.status_code == 429 or 500 <= r.status_code < 600):
            ra = r.headers.get("Retry-After") or "3"
            time.sleep(min(60, _num(ra, 3)) + 1)
            continue
        print(f"webhook failed: HTTP {r.status_code}: {r.text[:160]}",
              file=sys.stderr)
        return False
    return False


def _num(v, d):
    try:
        return int(float(v))
    except Exception:
        return d


def fmt_usd(v):
    if v is None:
        return "-"
    try:
        v = float(v)
        if abs(v) >= 1000:
            return f"${v:,.0f}"
        if v >= 0.01:
            return f"${v:,.4f}"
        return f"${v:.12f}".rstrip("0")
    except Exception:
        return str(v)


def fmt_big(v):
    if v is None:
        return "-"
    try:
        v = float(v)
        for unit in ("", "K", "M", "B", "T", "Q"):
            if abs(v) < 1000:
                return f"${v:,.1f}{unit}".replace(".0", "") if unit else f"${v:,.0f}"
            v /= 1000
    except Exception:
        pass
    return str(v)


def answer_ca(chain, addr, best, others, mention):
    pc = (best.get("priceChange") or {})
    if isinstance(pc, dict):
        pc = pc.get("h24")       # DexScreener: {"m5","h1","h6","h24"}
    pc = float(pc) if pc not in (None, "", {}) else None
    color = 0x2E7D32 if (pc or 0) >= 0 else 0xB71C1C
    bt = best.get("baseToken") or {}
    liq = (best.get("liquidity") or {}).get("usd")
    vol = (best.get("volume") or {}).get("h24")
    txns = (best.get("txns") or {}).get("h24") or {}
    fields = [
        {"name": "Harga", "value": f"**{fmt_usd(best.get('priceUsd'))}**", "inline": True},
        {"name": "24 jam", "value": f"{pc:+.2f}%" if pc is not None else "-", "inline": True},
        {"name": "Chain", "value": f"`{chain}`", "inline": True},
        {"name": "Likuiditas", "value": fmt_big(liq), "inline": True},
        {"name": "FDV / MC", "value": f"{fmt_big(best.get('fdv'))} / {fmt_big(best.get('marketCap'))}",
         "inline": True},
        {"name": "Vol 24 jam", "value": fmt_big(vol), "inline": True},
    ]
    if txns:
        fields.append({"name": "Txns 24 jam",
                       "value": f"buy {txns.get('buys', '-')} · sell {txns.get('sells', '-')}",
                       "inline": False})
    if others:
        fields.append({"name": "Pasar lain", "value": f"+{others} pair(s) lain di DexScreener",
                       "inline": False})
    deliver({"username": "Harga Live", "allowed_mentions": {"parse": []},
             "content": mention or None,
             "embeds": [{"title": f"{bt.get('name', 'Token')} (${bt.get('symbol', '?')})",
                         "description": f"CA `{addr}`",
                         "color": color, "fields": fields,
                         "url": f"https://dexscreener.com/{chain}/{addr}",
                         "footer": {"text": f"via DexScreener · {best.get('dexId', 'DEX')}"}}]})


def answer_gecko(d, mention):
    pc = d.get("usd_24h_change")
    fields = [
        {"name": "Harga", "value": f"**{fmt_usd(d.get('usd'))}**", "inline": True},
        {"name": "24 jam", "value": f"{pc:+.2f}%" if pc is not None else "-", "inline": True},
        {"name": "MC", "value": fmt_big(d.get("usd_market_cap")), "inline": True},
        {"name": "Vol 24 jam", "value": fmt_big(d.get("usd_24h_vol")), "inline": True},
        {"name": "Ranking", "value": f"#{d.get('rank', '?')}", "inline": True},
    ]
    deliver({"username": "Harga Live", "allowed_mentions": {"parse": []},
             "content": mention or None,
             "embeds": [{"title": f"{d.get('name')} ({d.get('symbol')})",
                         "color": 0x1565C0, "fields": fields,
                         "footer": {"text": "via CoinGecko"}}]})


def not_found(mention):
    deliver({"username": "Harga Live", "allowed_mentions": {"parse": []},
             "content": f"{mention} → tidak ketemu. Kirim CA lengkap atau `$TICKER` yang valid"
             })


def answer_gecko_ca(chain, addr, g, mention):
    deliver({"username": "Harga Live", "allowed_mentions": {"parse": []},
             "content": mention or None,
             "embeds": [{"title": f"{g['name']} (${g['symbol']})",
                         "description": f"CA `{addr}`",
                         "color": 0x1565C0,
                         "fields": [
                             {"name": "Harga", "value": f"**{fmt_usd(g['priceUsd'])}**",
                              "inline": True},
                             {"name": "Sumber", "value": "GeckoTerminal", "inline": True},
                             {"name": "Chain", "value": f"`{g['net']}`", "inline": True}],
                         "url": f"https://www.geckoterminal.com/{g['net']}/tokens/{addr}",
                         "footer": {"text": "via GeckoTerminal"}}]})


def ca_lookup(addr):
    """Return (kind, data) atau ('none', None). kind='dex' (DexScreener) atau
    'gecko' (fallback)."""
    chain = "ethereum" if addr.startswith("0x") else "solana"
    try:
        res = dexscreener_ca(addr)
        if res:
            return chain, addr, res
    except Exception as e:
        print(f"dexscreener gagal ({addr}): {e}", file=sys.stderr)
    try:
        g = gecko_ca(addr, chain)
        if g:
            return chain, addr, {"gecko": g}
    except Exception as e:
        print(f"gecko gagal ({addr}): {e}", file=sys.stderr)
    return chain, addr, None


def say_once_per(key, seconds, content):
    """Anti-spam: kirim pesan sekali per `seconds` untuk `key` yang sama."""
    now = time.time()
    last = MISS_TS.get(key, 0)
    if now - last >= seconds:
        MISS_TS[key] = now
        deliver({"username": "Harga Live", "allowed_mentions": {"parse": []},
                 "content": content})


MISS_TS = {}


def handle_message(d):
    """Accept MESSAGE_CREATE packet fields dict; answer with price."""
    chan, gid = str(d.get("channel_id") or ""), d.get("guild_id")
    if CHANNELS and chan not in CHANNELS:
        return
    if WATCH_GUILD and str(gid or "") != str(WATCH_GUILD):
        return
    mid = str(d.get("id") or "")
    content = (d.get("content") or "").strip()
    if not mid or not content or mid in DEDUPE:
        return
    DEDUPE[mid] = time.time()
    while len(DEDUPE) > 500:
        DEDUPE.popitem(last=False)
    mention = f"<@{(d.get('author') or {}).get('id', '')}>"

    try:
        addrs = set(CA_RE.findall(content))
        evms = "|".join(addrs)
        for cand in set(SOL_RE.findall(content)):
            # anti false-positive: substring alamat EVM bukan alamat Solana
            if cand not in evms:
                addrs.add(cand)
        for addr in list(addrs)[:3]:
            chain, addr, res = ca_lookup(addr)
            if res is None:
                say_once_per(addr, 60, f"{mention} → data harga buat `{addr[:18]}…` "
                                       "sedang padat (API sibuk), coba kirim ulang dalam beberapa saat.")
            elif "gecko" in res:
                answer_gecko_ca(chain, addr, res["gecko"], mention)
            else:
                best, others = res
                answer_ca(chain, addr, best, others, mention)
        tickers = set()
        for m in TICK_RE.finditer(content):
            t = m.group(1)
            if t and t.upper() not in STOPWORDS and len(t) >= 2:
                tickers.add(t.upper())
        if PLAIN_TICKERS:
            for m in TICK_PLAIN_RE.finditer(content):
                t = m.group(1)
                if t and t.upper() not in STOPWORDS and len(t) >= 3:
                    tickers.add(t.upper())
        for tick in list(tickers)[:3]:
            res = gecko_ticker(tick)
            if res:
                answer_gecko(res, mention)
            # ticker tidak dikenal = diam (anti spam); hanya CA miss yang diberi tahu
    except Exception as e:
        print(f"lookup error: {e}", file=sys.stderr)


def ready_banner():
    # banner hanya SEKALI selamanya (flag di state.json yang di-commit),
    # bukan tiap gateway READY / reconnect / restart job
    HERE = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(HERE, "state.json")
    state = {}
    try:
        state = json.load(open(path))
    except Exception:
        pass
    if state.get("banner_sent"):
        return
    if not deliver({"username": "Harga Live",
                    "content": "🟢 **Price bot live** — siap jawab harga saat CA / `$TICKER` ditulis"}):
        return
    state["banner_sent"] = True
    try:
        with open(path, "w") as f:
            json.dump(state, f, indent=1, sort_keys=True)
    except Exception as e:
        print(f"state write: {e}", file=sys.stderr)
        return
    subprocess.run(["git", "add", "state.json"], cwd=HERE)
    subprocess.run(["git", "commit", "-m", "banner sent [skip ci]"],
                   cwd=HERE, capture_output=True)
    head = subprocess.run(["git", "rev-parse", "--abbrev-ref", "HEAD"],
                          cwd=HERE, capture_output=True, text=True).stdout.strip()
    if head:
        subprocess.run(["git", "push", "origin", head], cwd=HERE,
                       capture_output=True)


# ---------------------------------------------------------------- gateway

def process_raw(ws, raw):
    global STOP, HB_THREAD_ON
    try:
        m = json.loads(raw) if isinstance(raw, (str, bytes)) else raw
    except Exception:
        return
    op, t, d = m.get("op"), m.get("t"), m.get("d") or {}

    if op == 10:                                            # Hello
        interval = (d.get("heartbeat_interval") or 41250) / 1000
        if not HB_THREAD_ON:
            HB_THREAD_ON = True
            threading.Thread(target=_heartbeat, args=(ws, interval),
                             daemon=True).start()
        _send(ws, {"op": 2, "d": {"token": BOT_TOKEN, "intents": INTENTS,
                                  "properties": {"os": "linux", "browser": "bot",
                                                 "device": "bot"}}})
    elif op == 11:                                          # Heartbeat ACK
        pass
    elif op == 9:                                           # Invalid session
        print("invalid session (resuming not implemented) — reidentifying",
              file=sys.stderr)
        time.sleep(2)
        _send(ws, {"op": 2, "d": {"token": BOT_TOKEN, "intents": INTENTS,
                                  "properties": {"os": "linux", "browser": "bot",
                                                 "device": "bot"}}})
    elif t == "READY":
        user = (d.get("user") or {}).get("username")
        print(f"gateway READY as {user}", flush=True)
        ready_banner()
    elif t == "MESSAGE_CREATE":
        handle_message(d)
    elif t and t.startswith("GATEWAY_") or (m.get("code") and op is None):
        # close frames arrive as raw code objects
        pass


def _send(ws, payload):
    try:
        ws.send(json.dumps(payload))
    except Exception as e:
        print(f"send failed: {e}", file=sys.stderr)


def _heartbeat(ws, interval):
    """Discord gateway heartbeat loop (op 1)."""
    while True:
        try:
            _send(ws, {"op": 1, "d": None})
        except Exception:
            break
        time.sleep(interval)


def on_error(_ws, e):
    msg = str(e)
    # websocket-client raises WebSocketException('Handshake status ...') etc.
    if "4004" in msg or "authentication" in msg.lower():
        print("auth failure (invalid bot token) — stopping", file=sys.stderr)
        return _mark_stop()


def _mark_stop():
    global STOP
    STOP = True


def main():
    if not BOT_TOKEN or not WEBHOOK:
        print("DC_BOT_TOKEN / DC_PRICE_WEBHOOK not set", file=sys.stderr)
        return 2
    lifetime = None
    if "--loop" in sys.argv:
        i = sys.argv.index("--loop")
        lifetime = (float(sys.argv[i + 1]) if len(sys.argv) > i + 1
                    and sys.argv[i + 1].replace(".", "", 1).isdigit() else 330.0) * 60
    deadline = time.time() + (lifetime or ONCE_SECONDS)

    try:
        gw = _session.get("https://discord.com/api/v10/gateway",
                          timeout=15).json()["url"]
    except Exception as e:
        print(f"gateway fetch failed: {e}", file=sys.stderr)
        return 1

    ws = websocket.WebSocketApp(
        gw,
        on_message=process_raw,
        on_error=on_error,
    )

    def unexpected(_req, resp):
        print(f"gateway rejected: HTTP {resp.status_code}", file=sys.stderr)
        if resp.status_code in (401, 403):
            _mark_stop()
        ws.keep_running = False

    def watchdog():
        while time.time() < deadline:
            time.sleep(10)
        ws.close()

    ws.unexpected_response = unexpected
    threading.Thread(target=watchdog, daemon=True).start()

    retry = 1.0
    while time.time() < deadline:
        try:
            ws.run_forever(ping_interval=25, ping_timeout=10)
        except Exception as e:
            print(f"run_forever: {e}", file=sys.stderr)
        if STOP or time.time() >= deadline:
            break
        wait = retry + random.random() * 0.5
        retry = min(retry * 2, 60.0)
        print(f"gateway closed — retrying in {wait:.1f}s", file=sys.stderr)
        time.sleep(wait)
    print(f"done: {POSTED} relays delivered")
    return 1 if STOP else 0


if __name__ == "__main__":
    sys.exit(main())
