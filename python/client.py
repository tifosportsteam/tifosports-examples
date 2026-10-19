"""TifoSports WebSocket sample client (Python 3.9+, websockets).

    pip install -r requirements.txt
    LOF_KEY=your-api-key LOF_APP_KEY=socket-app-key python client.py

It logs in, joins your plan's channel, keeps every event in memory (REST's
objects, kept current by the socket), prints each message on one line, and
recovers through REST when a message is missed. The socket speaks the Pusher
protocol; this client talks it directly (connect, log in, subscribe, answer
pings), so it needs no Pusher library. Settings (environment):

    LOF_KEY       your API key (dashboard)
    LOF_APP_KEY   the socket app key ("Connect with" on the docs page)
    LOF_API       REST base, default https://api.tifosports.com/v1
    LOF_WS_HOST   socket host, default ws.tifosports.com
    LOF_WS_PORT   default 443 (80 without TLS)
    LOF_WS_TLS    true / false, default true
    LOF_CHANNEL   the channel, default private-feed.pro
    LOF_LOAD      1: load every event over REST first (default); 0: socket only
    LOF_SECONDS   stop after this many seconds and print a summary; 0 runs forever
"""

import asyncio
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

import websockets

API = os.environ.get("LOF_API", "https://api.tifosports.com/v1").rstrip("/")
KEY = os.environ.get("LOF_KEY", "")
APP_KEY = os.environ.get("LOF_APP_KEY", "")
TLS = os.environ.get("LOF_WS_TLS", "true") == "true"
WS_HOST = os.environ.get("LOF_WS_HOST", "ws.tifosports.com")
WS_PORT = int(os.environ.get("LOF_WS_PORT", "443" if TLS else "80"))
CHANNEL = os.environ.get("LOF_CHANNEL", "private-feed.pro")
LOAD = os.environ.get("LOF_LOAD", "1") == "1"
# The event states the first load asks for (a recovery asks `since`, every state).
FIRST_LOAD = ("upcoming", "live", "suspended")
SECONDS = float(os.environ.get("LOF_SECONDS", "0"))

if not KEY or not APP_KEY:
    sys.exit("Set LOF_KEY (your API key) and LOF_APP_KEY (the socket app key from the docs page).")

# ------------------------------------------------------------------ the picture

events = {}  # event id => REST's event object, markets inside
applied = set()  # settlement ids applied once
state = {"last_seq": None, "last_at": None, "last_settlement": None, "feed": "unknown"}
counts = {}


def count(name):
    counts[name] = counts.get(name, 0) + 1


def find_market(event, market_id):
    return next((m for m in event.get("markets") or [] if m["id"] == market_id), None)


def find_outcome(event, outcome_id):
    for market in event.get("markets") or []:
        for outcome in market["outcomes"]:
            if outcome["id"] == outcome_id:
                return outcome
    return None


def merge_event(incoming):
    """REST answers with `since` carry only the changed outcomes inside their markets: merge them."""
    held = events.get(incoming["id"])
    if held is None or "markets" not in incoming:
        if held is not None:
            incoming = {**held, **incoming, "markets": held.get("markets", [])}
        events[incoming["id"]] = incoming
        return
    for field, value in incoming.items():
        if field != "markets":
            held[field] = value
    for market in incoming["markets"]:
        mine = find_market(held, market["id"])
        if mine is None:
            held.setdefault("markets", []).append(market)
            continue
        for outcome in market["outcomes"]:
            for i, current in enumerate(mine["outcomes"]):
                if current["id"] == outcome["id"]:
                    if outcome.get("updated_at", "") >= current.get("updated_at", ""):
                        mine["outcomes"][i] = outcome
                    break
            else:
                mine["outcomes"].append(outcome)


def settle(row):
    if row["id"] in applied:  # apply each settlement id once
        return
    applied.add(row["id"])
    state["last_settlement"] = row["id"]
    event = events.get(row["event_id"])
    outcome = find_outcome(event, row["outcome_id"]) if event else None
    if outcome is not None:
        outcome["result"] = row["result"]
        if row["result"] != "pending":
            outcome["status"] = "settled"
    # Your book: grade the bets on row["outcome_id"] with row["result"] (pending = withdraw the grade).


def apply(name, data):
    """One message, applied the way asyncapi.yaml says."""
    event_id = data.get("event_id", (data.get("event") or {}).get("id"))
    event = events.get(event_id) if event_id is not None else None
    if name == "event.created":
        events[data["event"]["id"]] = data["event"]
    elif name == "event.updated" and event is not None:
        for field in ("start_time", "state", "score", "clock"):
            event[field] = data.get(field)
    elif name == "market.created" and event is not None:
        markets = event.setdefault("markets", [])
        for i, market in enumerate(markets):
            if market["id"] == data["market"]["id"]:
                markets[i] = data["market"]
                break
        else:
            markets.append(data["market"])
    elif name == "market.updated" and event is not None:
        market = find_market(event, data["event_market_id"])
        for change in data["outcomes"] if market else []:
            for outcome in market["outcomes"]:
                if outcome["id"] == change["id"]:
                    outcome["price"], outcome["status"] = change.get("price"), change["status"]
    elif name == "stats.updated" and event is not None:
        event["stats"] = data["stats"]
    elif name == "settlements":
        for row in data["settlements"]:
            settle(row)
    elif name == "feed.alive":
        state["feed"] = data["status"]  # stale: suspend live markets; down: suspend everything
    if event_id is not None and event is None and name != "event.created":
        count("unknown")


def describe(name, data):
    if name == "event.created":
        e = data["event"]
        return f"{(e.get('home') or {}).get('name')} v {(e.get('away') or {}).get('name')}, {e['state']}, {len(e.get('markets') or [])} markets"
    if name == "event.updated":
        score = data.get("score")
        return f"{data['state']} {str(score['home']) + ':' + str(score['away']) if score else ''} {(data.get('clock') or {}).get('period') or ''}"
    if name == "market.created":
        m = data["market"]
        return f"{m['name']}{' ' + str(m['line']) if m.get('line') is not None else ''}, {len(m['outcomes'])} outcomes"
    if name == "market.updated":
        return ", ".join(f"{o['id']} {o.get('price') or '-'} {o['status']}" for o in data["outcomes"])
    if name == "settlements":
        return f"{len(data['settlements'])} results"
    if name == "feed.alive":
        return data["status"]
    return "stats" if name == "stats.updated" else ""


# ------------------------------------------------------------------------- REST


def get(path):
    request = urllib.request.Request(API + path, headers={"Authorization": f"Bearer {KEY}", "Accept": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        if error.code == 429:
            # Over the request limit: wait what Retry-After says, then the same call again.
            wait = max(1, int(error.headers.get("Retry-After") or 1))
            print(f"-- rate limited, waiting {wait} s", flush=True)
            time.sleep(wait)
            return get(path)
        raise RuntimeError(f"GET {path}: {error.code} {error.read().decode()}") from None


def pages(path):
    """Every page; `meta` of the first page (its `seq` is read before the data)."""
    rows, first, cursor = [], None, None
    while True:
        page = get(path + (f"&cursor={urllib.parse.quote(cursor)}" if cursor else ""))
        first = first or page.get("meta") or {}
        rows.extend(page["data"])
        cursor = (page.get("meta") or {}).get("next_cursor")
        if not cursor:
            return rows, first


def catch_up(reason):
    """The first load and every recovery: what changed since the last message, then the results."""
    print(f"-- catch-up ({reason})" + (f" since {state['last_at']}" if state["last_at"] else ""), flush=True)
    if state["last_at"]:
        rows, meta = pages(f"/events?markets=true&per_page=50&since={urllib.parse.quote(state['last_at'])}")
    else:
        # First load: the states a book shows, not the closed history. The first call's
        # meta.seq is read before any of the data, so it covers all three.
        rows, meta = [], None
        for event_state in FIRST_LOAD:
            more, first = pages(f"/events?markets=true&per_page=50&state={event_state}")
            rows.extend(more)
            meta = meta if meta is not None else first
    for row in rows:
        merge_event(row)
    if state["last_settlement"] is not None:
        results, _ = pages(f"/settlements?since={state['last_settlement']}")
        for row in results:
            settle(row)
    if meta.get("seq") is not None:
        state["last_seq"] = meta["seq"]
    print(f"-- caught up: {len(rows)} events, continue after seq {state['last_seq']}", flush=True)


def login(socket_id):
    """The channel login: POST {API}/ws/auth with your key, as a form like any Pusher client."""
    body = urllib.parse.urlencode({"socket_id": socket_id, "channel_name": CHANNEL}).encode()
    request = urllib.request.Request(f"{API}/ws/auth", data=body, method="POST", headers={
        "Authorization": f"Bearer {KEY}", "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
    })
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        raise RuntimeError(f"login {error.code}: {error.read().decode()}") from None


# ----------------------------------------------------------------------- socket


class Feed:
    def __init__(self):
        self.recovering = None  # a task while a REST catch-up runs
        self.buffer = []  # messages that arrive meanwhile

    def recover(self, reason):
        if self.recovering is None:
            self.recovering = asyncio.ensure_future(self._recover(reason))

    async def _recover(self, reason):
        try:
            await asyncio.to_thread(catch_up, reason)
        except Exception as error:  # noqa: BLE001 - a sample: report and go on
            print(f"-- catch-up failed: {error}", flush=True)
        self.recovering = None
        waiting, self.buffer = self.buffer, []
        for name, data in waiting:
            self.receive(name, data)  # drops seq at or below meta.seq

    def receive(self, name, data):
        if self.recovering is not None:
            self.buffer.append((name, data))
            return
        count(name)
        last = state["last_seq"]
        if last is not None:
            if data["seq"] <= last:
                # Already applied, or the counter started again after our server restarted.
                if last - data["seq"] > 1000:
                    self.recover("seq started again")
                return
            if data["seq"] != last + 1:
                count("gaps")
                self.recover(f"missed {data['seq'] - last - 1} messages")
                self.buffer.append((name, data))  # replayed after the catch-up
                return
        event_id = data.get("event_id", (data.get("event") or {}).get("id"))
        print(f"{data['seq']} {data['at']} {name}{' ' + str(event_id) if event_id is not None else ''} {describe(name, data)}", flush=True)
        apply(name, data)
        state["last_seq"], state["last_at"] = data["seq"], data["at"]

    async def run(self):
        url = f"{'wss' if TLS else 'ws'}://{WS_HOST}:{WS_PORT}/app/{APP_KEY}?protocol=7&client=python-sample&version=1.0"
        first = True
        while True:
            try:
                async with websockets.connect(url, ping_interval=None, max_size=None) as ws:
                    print("-- connected", flush=True)
                    if not first and state["last_seq"] is not None:
                        self.recover("reconnected")  # the messages sent meanwhile are gone
                    first = False
                    await self.listen(ws)
            except (OSError, websockets.ConnectionClosed, asyncio.TimeoutError) as error:
                print(f"-- connection lost ({error.__class__.__name__}): reconnecting in 3 s", flush=True)
                await asyncio.sleep(3)

    async def listen(self, ws):
        while True:
            # No message for 30 s means no feed.alive either: treat the feed as down, reconnect.
            frame = json.loads(await asyncio.wait_for(ws.recv(), timeout=30))
            name, raw = frame.get("event"), frame.get("data")
            data = json.loads(raw) if isinstance(raw, str) else (raw or {})
            if name == "pusher:connection_established":
                auth = await asyncio.to_thread(login, data["socket_id"])
                await ws.send(json.dumps({"event": "pusher:subscribe", "data": {"channel": CHANNEL, **auth}}))
            elif name == "pusher_internal:subscription_succeeded":
                print(f"-- joined {CHANNEL}", flush=True)
            elif name == "pusher:ping":
                await ws.send(json.dumps({"event": "pusher:pong", "data": {}}))
            elif name == "pusher:error" or name == "pusher:subscription_error":
                sys.exit(f"-- could not join {CHANNEL}: {data}")
            elif name and not name.startswith("pusher"):
                self.receive(name, data)


async def main():
    feed = Feed()
    if LOAD:
        feed.recover("first load")
    task = asyncio.ensure_future(feed.run())
    if SECONDS <= 0:
        await task
        return
    await asyncio.sleep(SECONDS)
    task.cancel()
    outcomes = sum(len(m["outcomes"]) for e in events.values() for m in e.get("markets") or [])
    print(f"-- summary after {SECONDS:g} s: {json.dumps(counts)}; holding {len(events)} events, {outcomes} outcomes; "
          f"feed {state['feed']}; last seq {state['last_seq']}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
