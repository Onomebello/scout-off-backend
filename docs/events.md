# Server-Sent Events (SSE) Event Stream

The backend pushes newly indexed contract events to authenticated clients over a
long-lived **Server-Sent Events** connection at `GET /api/events/stream`. This is
how frontends and integrations learn about `player_registered`,
`milestone_approved`, `scout_subscribed`, and the other platform events in
real time without polling.

The stream is backed by `EventBroadcaster` in `src/services/eventBroadcaster.ts`
and the route handler in `src/routes/events.ts`.

## Endpoint

| Method | Path                        | Auth          | Content-Type            |
| ------ | --------------------------- | ------------- | ----------------------- |
| `GET`  | `/api/events/stream`        | Bearer JWT    | `text/event-stream`     |

The stream is also mounted under the versioned prefixes:

- `/api/v1/events/stream`
- `/api/v2/events/stream`

All three paths behave identically (the v2 mount is currently the same handler
set as v1; this note will be updated once v1/v2 path mounting is made
consistent).

## Authentication

The stream uses the same `requireAuth` middleware as every other protected
route: send a `Bearer` JWT in the `Authorization` header (an `X-API-Key` header
is also accepted).

> **Browser caveat:** the native `EventSource` API cannot set request headers,
> so it cannot send a `Bearer` token directly. Use a headers-capable SSE client
> (e.g. the `eventsource` npm package, or an HTTP client that streams the
> response body) so you can attach the `Authorization` header. See the examples
> below.

Response codes:

| Status | Meaning                                                                 |
| ------ | ----------------------------------------------------------------------- |
| `200`  | Stream opened; frames start arriving                                    |
| `400`  | Invalid `eventType` filter (`{ success: false, error, code: "VALIDATION_ERROR", validEventTypes }`) |
| `401`  | Missing or invalid token (`{ success: false, error }`)                  |
| `403`  | Wallet is blocklisted — stream access revoked                           |
| `503`  | Connection limit reached (`SSE_MAX_CONNECTIONS`) — retry later          |

## Connecting

```js
// Node.js — headers-capable SSE client
const EventSource = require('eventsource');

const es = new EventSource('https://api.scoutoff.example/api/events/stream', {
  headers: { Authorization: `Bearer ${jwt}` },
});

es.addEventListener('connected', (e) => {
  console.log('stream open for wallet:', JSON.parse(e.data).wallet);
});

es.addEventListener('milestone_approved', (e) => {
  console.log('milestone approved:', e.data);
});

es.onerror = (err) => {
  console.error('stream error (will reconnect per EventSource spec):', err);
};
```

Browser `EventSource` example (only works if the token can be supplied by the
environment, e.g. via a service worker or a short-lived session cookie):

```js
const es = new EventSource('/api/events/stream');
es.onopen = () => console.log('stream open');
es.onmessage = (e) => console.log('event:', e.data);
```

On connect the server immediately sends an initial frame so the client knows
the stream is live:

```
event: connected
data: {"wallet":"GABCDEF..."}

```

## Filtering

All query parameters are optional and combinable. **Wallet-relevance filtering
is always applied** (see below); the query parameters only narrow the stream
further on top of it.

| Parameter   | Type   | Behaviour                                                                                                                              |
| ----------- | ------ | -------------------------------------------------------------------------------------------------------------------------------------- |
| `eventType` | string | Subscribe to one or more event types as a comma-separated list, e.g. `?eventType=milestone_approved,contact_unlocked`. Omitted = receive all event types that pass the relevance filter. Unknown values are rejected with `400`. |
| `playerId`  | string | Only deliver events whose payload contains this player identifier. Omitted = no additional player-level narrowing.                       |

Examples:

```text
# Only my milestone approvals
GET /api/events/stream?eventType=milestone_approved

# Multiple types (comma-separated)
GET /api/events/stream?eventType=milestone_approved,contact_unlocked

# Only events about one player (any type)
GET /api/events/stream?playerId=player-001

# Both
GET /api/events/stream?eventType=contact_unlocked&playerId=player-001
```

Filterable `eventType` values (validated against this exact list):

- `player_registered`
- `milestone_submitted`
- `milestone_approved`
- `scout_subscribed`
- `contact_unlocked`
- `trial_offer_logged`
- `fees_withdrawn`

> **Note:** the stream can also carry `player_deactivated`, `player_reactivated`,
> `trial_offer_accepted`, and `trial_offer_rejected` frames (they pass the
> relevance filter), but those types are **not** currently accepted as
> `eventType` filter values — requesting one returns `400` with the list of
> valid types in `validEventTypes`.

### Invalid `eventType`

If any requested value is not in the valid list above, the request is rejected
with `400` **before** the stream opens:

```json
{
  "success": false,
  "error": "Unknown eventType: 'milestone_aproved'",
  "code": "VALIDATION_ERROR",
  "validEventTypes": [
    "player_registered",
    "milestone_submitted",
    "milestone_approved",
    "scout_subscribed",
    "contact_unlocked",
    "trial_offer_logged",
    "fees_withdrawn"
  ]
}
```

## Frame format

Every event is a standard SSE frame:

```
event: <event_type>
data: {"type":"<event_type>","payload":{...}}

```

Concretely:

```
event: milestone_approved
data: {"type":"milestone_approved","payload":{"player_id":"player-001","wallet":"GABCDEF...","scout":"G123456..."}}

```

Other frames you may see:

| Frame type     | When                                            | Payload                                   |
| -------------- | ----------------------------------------------- | ----------------------------------------- |
| `connected`    | Once, immediately after the stream opens        | `{ "wallet": "<your wallet>" }`           |
| `session_ended`| The stream is being closed (see live auth below)| `{ "reason": "token_revoked" \| "wallet_blocklisted" }` |
| `: ping`       | Keep-alive comment every `SSE_KEEPALIVE_INTERVAL_MS` (default 15 s) | — (comment only, ignored by EventSource) |

The `data` field is JSON; parse it with `JSON.parse(e.data)`.

## Wallet-relevance rules

Every event is checked against the authenticated wallet before delivery — this
is the tenant-isolation boundary and **cannot be disabled or overridden with
query parameters**. The rules (from `isEventRelevantToWallet` in
`src/services/eventBroadcaster.ts`) are:

| Event type             | Delivered to the wallet when…                                                       |
| ---------------------- | ----------------------------------------------------------------------------------- |
| `milestone_approved`   | `payload.player_id`, `payload.wallet`, or `payload.scout` matches                    |
| `scout_subscribed`     | `payload.scout` or `payload.wallet` matches                                          |
| `contact_unlocked`     | `payload.scout` or `payload.wallet` matches                                          |
| `trial_offer_logged`   | `payload.scout` matches (scout) or `payload.player_id` matches (player)              |
| `trial_offer_accepted` | `payload.scout` matches (scout who offered) or `payload.player_id` matches (player)  |
| `trial_offer_rejected` | `payload.scout` matches (scout who offered) or `payload.player_id` matches (player)  |
| `player_registered`    | `payload.wallet` or `payload.player_id` matches                                      |
| `milestone_submitted`  | `payload.player_id` matches (player) or `payload.validator` matches (validator)      |
| `fees_withdrawn`       | `payload.recipient` or `payload.wallet` matches (admin)                               |
| `player_deactivated`   | `payload.player_id`, `payload.wallet`, or `payload.scout_wallet` matches              |
| `player_reactivated`   | `payload.player_id` or `payload.wallet` matches                                       |

In practice most clients only need `milestone_approved`, `scout_subscribed`, and
`contact_unlocked`, but all event types are handled so the stream is
self-documenting and future-proof.

### How filters interact with relevance

The two filter layers compose with **AND** semantics:

1. `isEventRelevantToWallet` — wallet isolation, always enforced.
2. `isEventMatchingFilter` — the optional `eventType`
