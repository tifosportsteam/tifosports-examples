// TifoSports WebSocket sample client (Node 18+, pusher-js 8).
//
//   npm install
//   LOF_KEY=your-api-key LOF_APP_KEY=socket-app-key node client.mjs
//
// It logs in, joins your plan's channel, keeps every event in memory (REST's
// objects, kept current by the socket), prints each message on one line, and
// recovers through REST when a message is missed. Settings (environment):
//
//   LOF_KEY       your API key (dashboard)
//   LOF_APP_KEY   the socket app key ("Connect with" on the docs page)
//   LOF_API       REST base, default https://api.tifosports.com/v1
//   LOF_WS_HOST   socket host, default ws.tifosports.com
//   LOF_WS_PORT   default 443 (80 without TLS)
//   LOF_WS_TLS    true / false, default true
//   LOF_CHANNEL   the channel, default private-feed.pro
//   LOF_LOAD      1: load every event over REST first (default); 0: socket only
//   LOF_SECONDS   stop after this many seconds and print a summary; 0 runs forever

import Pusher from 'pusher-js';

const API = (process.env.LOF_API ?? 'https://api.tifosports.com/v1').replace(/\/$/, '');
const KEY = process.env.LOF_KEY ?? '';
const APP_KEY = process.env.LOF_APP_KEY ?? '';
const TLS = (process.env.LOF_WS_TLS ?? 'true') === 'true';
const WS_HOST = process.env.LOF_WS_HOST ?? 'ws.tifosports.com';
const WS_PORT = Number(process.env.LOF_WS_PORT ?? (TLS ? 443 : 80));
const CHANNEL = process.env.LOF_CHANNEL ?? 'private-feed.pro';
const LOAD = (process.env.LOF_LOAD ?? '1') === '1';
const SECONDS = Number(process.env.LOF_SECONDS ?? 0);
// The event states the first load asks for (a recovery asks `since`, every state).
const FIRST_LOAD = ['upcoming', 'live', 'suspended'];

if (!KEY || !APP_KEY) {
  console.error('Set LOF_KEY (your API key) and LOF_APP_KEY (the socket app key from the docs page).');
  process.exit(1);
}

// ---------------------------------------------------------------- the picture

const events = new Map(); // event id => REST's event object, markets inside
const applied = new Set(); // settlement ids applied once
let lastSeq = null; // the last message applied
let lastAt = null; // its `at`: where a REST catch-up starts
let lastSettlement = null; // the last settlement id: where a settlements catch-up starts
let feedStatus = 'unknown';
let recovering = null; // a promise while a REST catch-up runs
let buffer = []; // messages that arrive meanwhile
const counts = {};

function findMarket(event, marketId) {
  return (event.markets ?? []).find((m) => m.id === marketId);
}

function findOutcome(event, outcomeId) {
  for (const market of event.markets ?? []) {
    const outcome = market.outcomes.find((o) => o.id === outcomeId);
    if (outcome) return outcome;
  }
  return null;
}

// REST answers with `since` carry only the changed outcomes inside their markets: merge them.
function mergeEvent(incoming) {
  const held = events.get(incoming.id);
  if (!held || !incoming.markets) {
    events.set(incoming.id, held ? { ...held, ...incoming, markets: held.markets } : incoming);
    return;
  }
  const { markets, ...fields } = incoming;
  Object.assign(held, fields);
  for (const market of markets) {
    const mine = findMarket(held, market.id);
    if (!mine) {
      held.markets.push(market);
      continue;
    }
    for (const outcome of market.outcomes) {
      const i = mine.outcomes.findIndex((o) => o.id === outcome.id);
      if (i === -1) mine.outcomes.push(outcome);
      else if (outcome.updated_at >= mine.outcomes[i].updated_at) mine.outcomes[i] = outcome;
    }
  }
}

function settle(row) {
  if (applied.has(row.id)) return; // apply each settlement id once
  applied.add(row.id);
  lastSettlement = row.id;
  const event = events.get(row.event_id);
  const outcome = event && findOutcome(event, row.outcome_id);
  if (outcome) {
    outcome.result = row.result;
    if (row.result !== 'pending') outcome.status = 'settled';
  }
  // Your book: grade the bets on row.outcome_id with row.result (pending = withdraw the grade).
}

// One message, applied the way asyncapi.yaml says.
function apply(name, data) {
  const id = data.event_id ?? data.event?.id;
  const event = id !== undefined ? events.get(id) : undefined;
  switch (name) {
    case 'event.created':
      events.set(data.event.id, data.event);
      break;
    case 'event.updated':
      if (event) Object.assign(event, { start_time: data.start_time, state: data.state, score: data.score, clock: data.clock });
      break;
    case 'market.created': {
      if (!event) break;
      const i = event.markets.findIndex((m) => m.id === data.market.id);
      if (i === -1) event.markets.push(data.market);
      else event.markets[i] = data.market;
      break;
    }
    case 'market.updated': {
      const market = event && findMarket(event, data.event_market_id);
      if (!market) break;
      for (const change of data.outcomes) {
        const outcome = market.outcomes.find((o) => o.id === change.id);
        if (outcome) Object.assign(outcome, { price: change.price, status: change.status });
      }
      break;
    }
    case 'stats.updated':
      if (event) event.stats = data.stats;
      break;
    case 'settlements':
      data.settlements.forEach(settle);
      break;
    case 'feed.alive':
      feedStatus = data.status; // stale: suspend live markets; down: suspend everything
      break;
  }
  if (id !== undefined && !event && name !== 'event.created') counts.unknown = (counts.unknown ?? 0) + 1;
}

function print(name, data) {
  const id = data.event_id ?? data.event?.id;
  const what = {
    'event.created': () => `${data.event.home?.name} v ${data.event.away?.name}, ${data.event.state}, ${data.event.markets?.length ?? 0} markets`,
    'event.updated': () => `${data.state} ${data.score ? data.score.home + ':' + data.score.away : ''} ${data.clock?.period ?? ''}`,
    'market.created': () => `${data.market.name}${data.market.line !== null ? ' ' + data.market.line : ''}, ${data.market.outcomes.length} outcomes`,
    'market.updated': () => data.outcomes.map((o) => `${o.id} ${o.price ?? '-'} ${o.status}`).join(', '),
    'stats.updated': () => 'stats',
    settlements: () => `${data.settlements.length} results`,
    'feed.alive': () => data.status,
  }[name];
  console.log(`${data.seq} ${data.at} ${name}${id !== undefined ? ' ' + id : ''} ${what ? what() : ''}`);
}

// ---------------------------------------------------------------------- REST

async function get(path) {
  const response = await fetch(API + path, { headers: { Authorization: `Bearer ${KEY}`, Accept: 'application/json' } });
  if (response.status === 429) {
    // Over the request limit: wait what Retry-After says, then the same call again.
    const wait = Math.max(1, Number(response.headers.get('Retry-After')) || 1);
    console.log(`-- rate limited, waiting ${wait} s`);
    await new Promise((resolve) => setTimeout(resolve, wait * 1000));
    return get(path);
  }
  if (!response.ok) throw new Error(`GET ${path}: ${response.status} ${await response.text()}`);
  return response.json();
}

// Every page; `meta` of the first page (its `seq` is read before the data).
async function pages(path) {
  const rows = [];
  let first = null;
  let cursor = null;
  do {
    const page = await get(path + (cursor ? `&cursor=${encodeURIComponent(cursor)}` : ''));
    first ??= page.meta;
    rows.push(...page.data);
    cursor = page.meta?.next_cursor ?? null;
  } while (cursor);
  return { rows, meta: first ?? {} };
}

// The first load and every recovery: what changed since the last message, then the results.
async function catchUp(reason) {
  console.log(`-- catch-up (${reason})${lastAt ? ' since ' + lastAt : ''}`);
  let rows = [];
  let meta = null;
  if (lastAt) {
    ({ rows, meta } = await pages(`/events?markets=true&per_page=50&since=${encodeURIComponent(lastAt)}`));
  } else {
    // First load: the states a book shows, not the closed history. The first call's
    // meta.seq is read before any of the data, so it covers all three.
    for (const state of FIRST_LOAD) {
      const page = await pages(`/events?markets=true&per_page=50&state=${state}`);
      rows.push(...page.rows);
      meta ??= page.meta;
    }
  }
  rows.forEach(mergeEvent);
  if (lastSettlement !== null) {
    const results = await pages(`/settlements?since=${lastSettlement}`);
    results.rows.forEach(settle);
  }
  lastSeq = meta.seq ?? lastSeq;
  console.log(`-- caught up: ${rows.length} events, continue after seq ${lastSeq}`);
}

function recover(reason) {
  if (recovering) return;
  recovering = catchUp(reason)
    .catch((error) => console.error('-- catch-up failed:', error.message))
    .finally(() => {
      recovering = null;
      const waiting = buffer;
      buffer = [];
      waiting.forEach(([name, data]) => receive(name, data)); // drops seq at or below meta.seq
    });
}

// -------------------------------------------------------------------- socket

function receive(name, data) {
  if (recovering) {
    buffer.push([name, data]);
    return;
  }
  counts[name] = (counts[name] ?? 0) + 1;
  if (lastSeq !== null) {
    if (data.seq <= lastSeq) {
      // Already applied, or the counter started again after our server restarted.
      if (lastSeq - data.seq > 1000) recover('seq started again');
      return;
    }
    if (data.seq !== lastSeq + 1) {
      counts.gaps = (counts.gaps ?? 0) + 1;
      recover(`missed ${data.seq - lastSeq - 1} messages`);
      buffer.push([name, data]); // replayed after the catch-up
      return;
    }
  }
  print(name, data);
  apply(name, data);
  lastSeq = data.seq;
  lastAt = data.at;
}

const pusher = new Pusher(APP_KEY, {
  cluster: '',
  wsHost: WS_HOST,
  wsPort: WS_PORT,
  wssPort: WS_PORT,
  forceTLS: TLS,
  enabledTransports: ['ws', 'wss'],
  channelAuthorization: {
    // The channel login: POST {API}/ws/auth with your key, as a form like any Pusher client.
    customHandler: async ({ socketId, channelName }, callback) => {
      try {
        const response = await fetch(`${API}/ws/auth`, {
          method: 'POST',
          headers: { Authorization: `Bearer ${KEY}`, Accept: 'application/json', 'Content-Type': 'application/x-www-form-urlencoded' },
          body: new URLSearchParams({ socket_id: socketId, channel_name: channelName }),
        });
        if (!response.ok) throw new Error(`login ${response.status}: ${await response.text()}`);
        callback(null, await response.json());
      } catch (error) {
        callback(error, null);
      }
    },
  },
});

let watchdog = null;
const resetWatchdog = () => {
  clearTimeout(watchdog);
  // No feed.alive for 30 s: treat the feed as down, suspend everything, reconnect.
  watchdog = setTimeout(() => {
    console.log('-- no feed.alive for 30 s: reconnecting');
    pusher.disconnect();
    pusher.connect();
  }, 30_000);
};

let dropped = false;
pusher.connection.bind('state_change', ({ previous, current }) => {
  console.log(`-- connection ${previous} -> ${current}`);
  if (current === 'unavailable' || current === 'disconnected') dropped = true;
  // After a drop, the messages sent meanwhile are gone: catch up through REST.
  if (current === 'connected' && dropped) {
    dropped = false;
    if (lastSeq !== null) recover('reconnected');
  }
});

const channel = pusher.subscribe(CHANNEL);
channel.bind('pusher:subscription_succeeded', () => console.log(`-- joined ${CHANNEL}`));
channel.bind('pusher:subscription_error', (error) => {
  console.error(`-- could not join ${CHANNEL}:`, error?.error ?? error);
  process.exit(1);
});
channel.bind_global((name, data) => {
  if (name.startsWith('pusher:')) return;
  if (name === 'feed.alive') resetWatchdog();
  receive(name, data);
});

if (LOAD) recover('first load');
resetWatchdog();

if (SECONDS > 0) {
  setTimeout(() => {
    let outcomes = 0;
    for (const event of events.values()) for (const market of event.markets ?? []) outcomes += market.outcomes.length;
    console.log(`-- summary after ${SECONDS} s: ${JSON.stringify(counts)}; holding ${events.size} events, ${outcomes} outcomes; feed ${feedStatus}; last seq ${lastSeq}`);
    pusher.disconnect();
    process.exit(0);
  }, SECONDS * 1000);
}
