<?php

/*
 * TifoSports WebSocket sample client (PHP 8.1+, no packages: openssl for TLS).
 *
 *   LOF_KEY=your-api-key LOF_APP_KEY=socket-app-key php client.php
 *
 * It logs in, joins your plan's channel, keeps every event in memory (REST's
 * objects, kept current by the socket), prints each message on one line, and
 * recovers through REST when a message is missed. The socket speaks the Pusher
 * protocol over a plain WebSocket; this file talks both itself, so it needs no
 * library. In your own code a WebSocket package (ratchet/pawl, phrity/websocket)
 * can replace the WebSocketClient class at the bottom. Settings (environment):
 *
 *   LOF_KEY       your API key (dashboard)
 *   LOF_APP_KEY   the socket app key ("Connect with" on the docs page)
 *   LOF_API       REST base, default https://api.tifosports.com/v1
 *   LOF_WS_HOST   socket host, default ws.tifosports.com
 *   LOF_WS_PORT   default 443 (80 without TLS)
 *   LOF_WS_TLS    true / false, default true
 *   LOF_CHANNEL   the channel, default private-feed.pro
 *   LOF_LOAD      1: load every event over REST first (default); 0: socket only
 *   LOF_SECONDS   stop after this many seconds and print a summary; 0 runs forever
 */

declare(strict_types=1);

$env = fn (string $name, string $default = ''): string => getenv($name) !== false ? (string) getenv($name) : $default;

define('API', rtrim($env('LOF_API', 'https://api.tifosports.com/v1'), '/'));
define('KEY', $env('LOF_KEY'));
define('APP_KEY', $env('LOF_APP_KEY'));
define('TLS', $env('LOF_WS_TLS', 'true') === 'true');
define('WS_HOST', $env('LOF_WS_HOST', 'ws.tifosports.com'));
define('WS_PORT', (int) $env('LOF_WS_PORT', TLS ? '443' : '80'));
define('CHANNEL', $env('LOF_CHANNEL', 'private-feed.pro'));
define('LOAD', $env('LOF_LOAD', '1') === '1');
define('SECONDS', (float) $env('LOF_SECONDS', '0'));
// The event states the first load asks for (a recovery asks `since`, every state).
const FIRST_LOAD = ['upcoming', 'live', 'suspended'];

if (KEY === '' || APP_KEY === '') {
    fwrite(STDERR, "Set LOF_KEY (your API key) and LOF_APP_KEY (the socket app key from the docs page).\n");
    exit(1);
}

// ------------------------------------------------------------------ the picture

final class Picture
{
    /** @var array<int, array<string, mixed>> event id => REST's event object, markets inside */
    public array $events = [];

    /** @var array<int|string, true> settlement ids applied once */
    public array $applied = [];

    public ?int $lastSeq = null;       // the last message applied

    public ?string $lastAt = null;     // its `at`: where a REST catch-up starts

    public int|string|null $lastSettlement = null; // where a settlements catch-up starts

    public string $feed = 'unknown';

    /** @var array<string, int> */
    public array $counts = [];

    /** REST answers with `since` carry only the changed outcomes inside their markets: merge them. */
    public function merge(array $incoming): void
    {
        $id = $incoming['id'];
        $held = $this->events[$id] ?? null;
        if ($held === null || ! isset($incoming['markets'])) {
            $this->events[$id] = $held === null ? $incoming : array_merge($held, $incoming, ['markets' => $held['markets'] ?? []]);

            return;
        }
        foreach ($incoming as $field => $value) {
            if ($field !== 'markets') {
                $held[$field] = $value;
            }
        }
        foreach ($incoming['markets'] as $market) {
            $m = $this->marketIndex($held, $market['id']);
            if ($m === null) {
                $held['markets'][] = $market;

                continue;
            }
            foreach ($market['outcomes'] as $outcome) {
                $o = $this->outcomeIndex($held['markets'][$m], $outcome['id']);
                if ($o === null) {
                    $held['markets'][$m]['outcomes'][] = $outcome;
                } elseif (($outcome['updated_at'] ?? '') >= ($held['markets'][$m]['outcomes'][$o]['updated_at'] ?? '')) {
                    $held['markets'][$m]['outcomes'][$o] = $outcome;
                }
            }
        }
        $this->events[$id] = $held;
    }

    public function settle(array $row): void
    {
        if (isset($this->applied[$row['id']])) {
            return; // apply each settlement id once
        }
        $this->applied[$row['id']] = true;
        $this->lastSettlement = $row['id'];
        $eventId = $row['event_id'];
        foreach ($this->events[$eventId]['markets'] ?? [] as $m => $market) {
            $o = $this->outcomeIndex($market, $row['outcome_id']);
            if ($o !== null) {
                $this->events[$eventId]['markets'][$m]['outcomes'][$o]['result'] = $row['result'];
                if ($row['result'] !== 'pending') {
                    $this->events[$eventId]['markets'][$m]['outcomes'][$o]['status'] = 'settled';
                }
            }
        }
        // Your book: grade the bets on $row['outcome_id'] with $row['result'] (pending = withdraw the grade).
    }

    /** One message, applied the way asyncapi.yaml says. */
    public function apply(string $name, array $data): void
    {
        $id = $data['event_id'] ?? $data['event']['id'] ?? null;
        $known = $id !== null && isset($this->events[$id]);
        switch ($name) {
            case 'event.created':
                $this->events[$data['event']['id']] = $data['event'];
                break;
            case 'event.updated':
                if ($known) {
                    foreach (['start_time', 'state', 'score', 'clock'] as $field) {
                        $this->events[$id][$field] = $data[$field] ?? null;
                    }
                }
                break;
            case 'market.created':
                if ($known) {
                    $m = $this->marketIndex($this->events[$id], $data['market']['id']);
                    if ($m === null) {
                        $this->events[$id]['markets'][] = $data['market'];
                    } else {
                        $this->events[$id]['markets'][$m] = $data['market'];
                    }
                }
                break;
            case 'market.updated':
                $m = $known ? $this->marketIndex($this->events[$id], $data['event_market_id']) : null;
                if ($m !== null) {
                    foreach ($data['outcomes'] as $change) {
                        $o = $this->outcomeIndex($this->events[$id]['markets'][$m], $change['id']);
                        if ($o !== null) {
                            $this->events[$id]['markets'][$m]['outcomes'][$o]['price'] = $change['price'] ?? null;
                            $this->events[$id]['markets'][$m]['outcomes'][$o]['status'] = $change['status'];
                        }
                    }
                }
                break;
            case 'stats.updated':
                if ($known) {
                    $this->events[$id]['stats'] = $data['stats'];
                }
                break;
            case 'settlements':
                foreach ($data['settlements'] as $row) {
                    $this->settle($row);
                }
                break;
            case 'feed.alive':
                $this->feed = $data['status']; // stale: suspend live markets; down: suspend everything
                break;
        }
        if ($id !== null && ! $known && $name !== 'event.created') {
            $this->count('unknown');
        }
    }

    public function count(string $name): void
    {
        $this->counts[$name] = ($this->counts[$name] ?? 0) + 1;
    }

    private function marketIndex(array $event, int|string $marketId): ?int
    {
        foreach ($event['markets'] ?? [] as $i => $market) {
            if ($market['id'] === $marketId) {
                return $i;
            }
        }

        return null;
    }

    private function outcomeIndex(array $market, int|string $outcomeId): ?int
    {
        foreach ($market['outcomes'] as $i => $outcome) {
            if ($outcome['id'] === $outcomeId) {
                return $i;
            }
        }

        return null;
    }
}

function describe(string $name, array $data): string
{
    return match ($name) {
        'event.created' => sprintf('%s v %s, %s, %d markets', $data['event']['home']['name'] ?? '?', $data['event']['away']['name'] ?? '?', $data['event']['state'], count($data['event']['markets'] ?? [])),
        'event.updated' => trim($data['state'].' '.(isset($data['score']) ? $data['score']['home'].':'.$data['score']['away'] : '').' '.($data['clock']['period'] ?? '')),
        'market.created' => $data['market']['name'].(isset($data['market']['line']) ? ' '.$data['market']['line'] : '').', '.count($data['market']['outcomes']).' outcomes',
        'market.updated' => implode(', ', array_map(fn ($o) => $o['id'].' '.($o['price'] ?? '-').' '.$o['status'], $data['outcomes'])),
        'settlements' => count($data['settlements']).' results',
        'feed.alive' => $data['status'],
        'stats.updated' => 'stats',
        default => '',
    };
}

// ------------------------------------------------------------------------- REST

function http(string $method, string $url, ?string $form = null): array
{
    $headers = ['Authorization: Bearer '.KEY, 'Accept: application/json'];
    if ($form !== null) {
        $headers[] = 'Content-Type: application/x-www-form-urlencoded';
    }
    $context = stream_context_create(['http' => [
        'method' => $method, 'header' => implode("\r\n", $headers), 'content' => $form ?? '',
        'timeout' => 60, 'ignore_errors' => true,
    ]]);
    // fopen + stream_get_meta_data reads the response headers on every PHP from 8.1 on
    // ($http_response_header is deprecated since 8.5).
    $stream = @fopen($url, 'r', false, $context);
    $body = $stream !== false ? stream_get_contents($stream) : false;
    $lines = $stream !== false ? (stream_get_meta_data($stream)['wrapper_data'] ?? []) : [];
    if ($stream !== false) {
        fclose($stream);
    }
    $status = (int) (preg_match('#^HTTP/\S+ (\d+)#', $lines[0] ?? '', $m) ? $m[1] : 0);
    if ($status === 429) {
        // Over the request limit: wait what Retry-After says, then the same call again.
        $wait = 1;
        foreach ($lines as $line) {
            if (preg_match('#^Retry-After:\s*(\d+)#i', $line, $r)) {
                $wait = max(1, (int) $r[1]);
            }
        }
        echo "-- rate limited, waiting {$wait} s\n";
        sleep($wait);

        return http($method, $url, $form);
    }
    if ($body === false || $status !== 200) {
        throw new RuntimeException("$method $url: $status ".($body ?: 'no answer'));
    }

    return json_decode($body, true, 512, JSON_THROW_ON_ERROR);
}

/** Every page; `meta` of the first page (its `seq` is read before the data). */
function pages(string $path): array
{
    $rows = [];
    $first = null;
    $cursor = null;
    do {
        $page = http('GET', API.$path.($cursor !== null ? '&cursor='.rawurlencode($cursor) : ''));
        $first ??= $page['meta'] ?? [];
        array_push($rows, ...$page['data']);
        $cursor = $page['meta']['next_cursor'] ?? null;
    } while ($cursor !== null);

    return [$rows, $first];
}

/** The first load and every recovery: what changed since the last message, then the results. */
function catchUp(Picture $p, string $reason): void
{
    echo "-- catch-up ($reason)".($p->lastAt !== null ? " since $p->lastAt" : '')."\n";
    try {
        if ($p->lastAt !== null) {
            [$rows, $meta] = pages('/events?markets=true&per_page=50&since='.rawurlencode($p->lastAt));
        } else {
            // First load: the states a book shows, not the closed history. The first call's
            // meta.seq is read before any of the data, so it covers all three.
            [$rows, $meta] = [[], null];
            foreach (FIRST_LOAD as $state) {
                [$more, $m] = pages('/events?markets=true&per_page=50&state='.$state);
                array_push($rows, ...$more);
                $meta ??= $m;
            }
        }
        foreach ($rows as $row) {
            $p->merge($row);
        }
        if ($p->lastSettlement !== null) {
            [$results] = pages('/settlements?since='.rawurlencode((string) $p->lastSettlement));
            foreach ($results as $row) {
                $p->settle($row);
            }
        }
        $p->lastSeq = $meta['seq'] ?? $p->lastSeq;
        echo '-- caught up: '.count($rows)." events, continue after seq $p->lastSeq\n";
    } catch (Throwable $e) {
        echo '-- catch-up failed: '.$e->getMessage()."\n";
    }
}

/**
 * One message off the channel. The socket keeps what arrives while a catch-up
 * runs, so this client recovers in line and simply reads on: messages at or
 * below the catch-up's meta.seq are dropped as already applied.
 */
function receive(Picture $p, string $name, array $data): void
{
    $p->count($name);
    if ($p->lastSeq !== null) {
        if ($data['seq'] <= $p->lastSeq) {
            // Already applied, or the counter started again after our server restarted.
            if ($p->lastSeq - $data['seq'] > 1000) {
                catchUp($p, 'seq started again');
            }

            return;
        }
        if ($data['seq'] !== $p->lastSeq + 1) {
            $p->count('gaps');
            catchUp($p, 'missed '.($data['seq'] - $p->lastSeq - 1).' messages');
            if ($data['seq'] <= $p->lastSeq) {
                return;
            }
        }
    }
    $id = $data['event_id'] ?? $data['event']['id'] ?? null;
    echo $data['seq'].' '.$data['at'].' '.$name.($id !== null ? " $id" : '').' '.describe($name, $data)."\n";
    $p->apply($name, $data);
    $p->lastSeq = $data['seq'];
    $p->lastAt = $data['at'];
}

// ----------------------------------------------------------------------- socket

/** A minimal WebSocket client (RFC 6455): text frames, ping / pong, close. */
final class WebSocketClient
{
    /** @var resource */
    private $stream;

    private string $buffer = '';

    public function __construct(string $host, int $port, bool $tls, string $path)
    {
        $stream = @stream_socket_client(($tls ? 'tls://' : 'tcp://')."$host:$port", $errno, $error, 30);
        if ($stream === false) {
            throw new RuntimeException("connect $host:$port: $error");
        }
        $this->stream = $stream;
        $key = base64_encode(random_bytes(16));
        fwrite($stream, "GET $path HTTP/1.1\r\nHost: $host\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
            ."Sec-WebSocket-Key: $key\r\nSec-WebSocket-Version: 13\r\n\r\n");
        $head = '';
        while (! str_contains($head, "\r\n\r\n")) {
            $chunk = fread($stream, 1024);
            if ($chunk === false || $chunk === '') {
                throw new RuntimeException('the socket closed during the handshake');
            }
            $head .= $chunk;
        }
        [$head, $this->buffer] = explode("\r\n\r\n", $head, 2);
        if (! preg_match('#^HTTP/1\.1 101#', $head)) {
            throw new RuntimeException('handshake refused: '.strtok($head, "\r\n"));
        }
    }

    public function send(array $message): void
    {
        $payload = json_encode($message, JSON_THROW_ON_ERROR);
        $this->frame(0x1, $payload);
    }

    /** The next text message, or null when nothing came within $timeout seconds. */
    public function receive(float $timeout): ?string
    {
        $text = '';
        $deadline = microtime(true) + $timeout;
        while (true) {
            $frame = $this->readFrame($deadline);
            if ($frame === null) {
                return null;
            }
            [$fin, $opcode, $payload] = $frame;
            if ($opcode === 0x9) {
                $this->frame(0xA, $payload); // a ping from the server: pong

                continue;
            }
            if ($opcode === 0x8) {
                throw new RuntimeException('the server closed the socket');
            }
            if ($opcode === 0x1 || $opcode === 0x0) {
                $text .= $payload;
                if ($fin) {
                    return $text;
                }
            }
        }
    }

    public function close(): void
    {
        @$this->frame(0x8, '');
        @fclose($this->stream);
    }

    private function frame(int $opcode, string $payload): void
    {
        $length = strlen($payload);
        $head = chr(0x80 | $opcode);
        $head .= match (true) {
            $length < 126 => chr(0x80 | $length),
            $length < 65536 => chr(0x80 | 126).pack('n', $length),
            default => chr(0x80 | 127).pack('J', $length),
        };
        $mask = random_bytes(4); // a client masks every frame
        $masked = $payload ^ str_repeat($mask, intdiv($length, 4) + 1);
        fwrite($this->stream, $head.$mask.substr($masked, 0, $length));
    }

    /** @return array{0: bool, 1: int, 2: string}|null */
    private function readFrame(float $deadline): ?array
    {
        if (($head = $this->read(2, $deadline)) === null) {
            return null;
        }
        $fin = (ord($head[0]) & 0x80) !== 0;
        $opcode = ord($head[0]) & 0x0F;
        $length = ord($head[1]) & 0x7F;
        if ($length === 126) {
            $length = unpack('n', $this->read(2, null))[1];
        } elseif ($length === 127) {
            $length = unpack('J', $this->read(8, null))[1];
        }

        return [$fin, $opcode, $length > 0 ? $this->read($length, null) : ''];
    }

    /** Exactly $bytes; null when the deadline passes before the first byte (null deadline: wait). */
    private function read(int $bytes, ?float $deadline): ?string
    {
        while (strlen($this->buffer) < $bytes) {
            $wait = $deadline === null ? 30.0 : $deadline - microtime(true);
            if ($wait <= 0 && $this->buffer === '') {
                return null;
            }
            $read = [$this->stream];
            $none = null;
            $ready = stream_select($read, $none, $none, (int) max(0, $wait), (int) (fmod(max(0, $wait), 1) * 1e6));
            if ($ready === 0 && $deadline !== null && $this->buffer === '') {
                return null;
            }
            $chunk = fread($this->stream, 65536);
            if ($chunk === false || ($chunk === '' && feof($this->stream))) {
                throw new RuntimeException('the socket closed');
            }
            $this->buffer .= $chunk;
        }
        $out = substr($this->buffer, 0, $bytes);
        $this->buffer = (string) substr($this->buffer, $bytes);

        return $out;
    }
}

// ------------------------------------------------------------------------- main

$p = new Picture();
$stopAt = SECONDS > 0 ? microtime(true) + SECONDS : null;
$first = true;
if (LOAD) {
    catchUp($p, 'first load');
}

while ($stopAt === null || microtime(true) < $stopAt) {
    try {
        $socket = new WebSocketClient(WS_HOST, WS_PORT, TLS, '/app/'.APP_KEY.'?protocol=7&client=php-sample&version=1.0');
        echo "-- connected\n";
        if (! $first && $p->lastSeq !== null) {
            catchUp($p, 'reconnected'); // the messages sent meanwhile are gone
        }
        $first = false;
        while ($stopAt === null || microtime(true) < $stopAt) {
            $wait = $stopAt === null ? 30.0 : min(30.0, max(0.1, $stopAt - microtime(true)));
            $raw = $socket->receive($wait);
            if ($raw === null) {
                if ($stopAt !== null && microtime(true) >= $stopAt) {
                    break;
                }
                // No message for 30 s means no feed.alive either: treat the feed as down, reconnect.
                throw new RuntimeException('no message for 30 s');
            }
            $frame = json_decode($raw, true, 512, JSON_THROW_ON_ERROR);
            $name = (string) ($frame['event'] ?? '');
            $data = is_string($frame['data'] ?? null) ? json_decode($frame['data'], true) : ($frame['data'] ?? []);
            if ($name === 'pusher:connection_established') {
                // The channel login: POST {API}/ws/auth with your key, as a form like any Pusher client.
                $auth = http('POST', API.'/ws/auth', http_build_query(['socket_id' => $data['socket_id'], 'channel_name' => CHANNEL]));
                $socket->send(['event' => 'pusher:subscribe', 'data' => ['channel' => CHANNEL] + $auth]);
            } elseif ($name === 'pusher_internal:subscription_succeeded') {
                echo '-- joined '.CHANNEL."\n";
            } elseif ($name === 'pusher:ping') {
                $socket->send(['event' => 'pusher:pong', 'data' => new stdClass()]);
            } elseif ($name === 'pusher:error' || $name === 'pusher:subscription_error') {
                fwrite(STDERR, '-- could not join '.CHANNEL.': '.json_encode($data)."\n");
                exit(1);
            } elseif ($name !== '' && ! str_starts_with($name, 'pusher')) {
                receive($p, $name, $data);
            }
        }
        $socket->close();
    } catch (Throwable $e) {
        echo '-- connection lost ('.$e->getMessage()."): reconnecting in 3 s\n";
        sleep(3);
    }
}

$outcomes = 0;
foreach ($p->events as $event) {
    foreach ($event['markets'] ?? [] as $market) {
        $outcomes += count($market['outcomes']);
    }
}
printf("-- summary after %s s: %s; holding %d events, %d outcomes; feed %s; last seq %s\n",
    SECONDS, json_encode($p->counts), count($p->events), $outcomes, $p->feed, $p->lastSeq ?? 'none');
