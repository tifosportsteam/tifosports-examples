# TifoSports API examples

Sample clients for the [TifoSports](https://www.tifosports.com/?utm_source=github) sportsbook feed: fixtures, markets with pre-match and live odds, scores and bet settlement in one self-serve API, over REST and a WebSocket (Pusher protocol).

Each client logs in, joins your plan's channel, keeps every event in memory (the REST objects, kept current by the socket), prints each message on one line, and recovers through REST when a message is missed.

| Folder | Language | Needs |
|---|---|---|
| [`js/`](js) | Node.js 18+ | `pusher-js` (`npm install`) |
| [`python/`](python) | Python 3.9+ | `websockets` (`pip install -r requirements.txt`) |
| [`php/`](php) | PHP 8.1+ | no packages (openssl for TLS) |

## Quick start

1. Sign up on [tifosports.com](https://www.tifosports.com/signup?utm_source=github): 14 days of full Pro, free, from your key's first API call.
2. Copy your API key from the dashboard, and the socket app key from the docs ("Connect with").
3. Run a client:

```bash
cd js && npm install
LOF_KEY=your-api-key LOF_APP_KEY=socket-app-key node client.mjs
```

```bash
cd python && pip install -r requirements.txt
LOF_KEY=your-api-key LOF_APP_KEY=socket-app-key python client.py
```

```bash
cd php
LOF_KEY=your-api-key LOF_APP_KEY=socket-app-key php client.php
```

Every setting (hosts, channel, a time limit) is an environment variable listed at the top of each file.

## Docs

- [Documentation](https://www.tifosports.com/docs?utm_source=github): quick start, REST and WebSocket guides, recipes for sportsbooks
- [OpenAPI contract](https://www.tifosports.com/docs/openapi.yaml) (REST, OpenAPI 3.1: import into Postman or a code generator)
- [AsyncAPI contract](https://www.tifosports.com/docs/asyncapi.yaml) (the WebSocket, every message with an example)
- [`llms.txt`](https://www.tifosports.com/llms.txt) for AI coding agents
- [Pricing](https://www.tifosports.com/pricing?utm_source=github): $750 a month or $7,500 a year, no contract

## Licence

MIT, see [LICENSE](LICENSE).
