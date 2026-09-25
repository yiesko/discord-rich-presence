# Protocol: how games talk to rsRPC

rsRPC plays the part of the local Discord client. Games connect to it the
way they would connect to Discord, publish Rich Presence, and get
officially-shaped replies. Two transports are served:

- **IPC** — local sockets named `discord-ipc-0` … `discord-ipc-9`, found in
  `XDG_RUNTIME_DIR`, then `TMPDIR`, `TMP`, `TEMP`, and finally `/tmp`
  (with symlinks into the other candidate directories so clients find the
  socket wherever they look).
- **WebSocket** — loopback only, on the first free port in
  `6463`–`6472` (`--ws-port-start` / `--ws-port-end`).

Web clients (browsers, Vencord) do not use these transports; they connect
to the bridge instead — see [web-client.md](web-client.md).

## Ports at a glance

| What | Default | Flag |
|---|---|---|
| Game IPC sockets | `discord-ipc-0` … `discord-ipc-9` | — |
| Game websocket | 6463–6472 (first free) | `--ws-port-start` / `--ws-port-end` |
| Bridge (JSON) | 1337–1347 (first free) | `--bridge-port` / `--bridge-port-end` |
| Bridge (MessagePack) | 1338 (steps forward if taken) | `--msgpack-port` |

## IPC handshake

Frames are `[4-byte opcode][4-byte length][JSON payload]`, little-endian.
Opcodes: `0` handshake, `1` frame, `2` close, `3` ping, `4` pong.

The first frame must be `{"v": 1, "client_id": "..."}`:

| Problem | Reply |
|---|---|
| `v` is not `1` | close `4004` — "Invalid version" |
| `client_id` is empty | close `4000` — "Invalid client_id" |
| Payload larger than 1 MiB | close `1003` — "Payload too large" |
| Unknown opcode | close `1003` — "Unsupported packet type" |
| Frame is not valid JSON | error `4005` — "Invalid encoding" |

On any connection loss, whatever that connection had published is cleared,
so a crashed game never leaves a card on screen.

## WebSocket handshake

Connect to `ws://127.0.0.1:<port>/?v=1&encoding=json` (add
`&client_id=<app id>` as a fallback for commands that carry no application
id):

- `v` must be `1` and `encoding` must be `json`; anything else is closed
  with the normal close code before any reply.
- Browser origins must be Discord (`discord.com`, `canary.discord.com`,
  `ptb.discord.com`); a missing origin (native clients) passes. Anything
  else is closed.
- A valid client immediately receives `DISPATCH` / `READY`:

```json
{
  "cmd": "DISPATCH",
  "evt": "READY",
  "data": {
    "v": 1,
    "user": { "id": "1045800378228281345", "username": "arRPC", "...": "..." },
    "config": { "api_endpoint": "//discord.com/api", "cdn_host": "cdn.discordapp.com", "environment": "production" }
  },
  "nonce": null
}
```

The `user` object is rsRPC's identity — see
[web-client.md](web-client.md) for how to change it.

## Commands

Both transports share one command table (see
`crates/rsrpc-protocol/src/commands.rs`):

| Command | What happens |
|---|---|
| `SET_ACTIVITY` | Forwarded to the bridge; the game gets an arRPC-shaped confirmation echoing its own activity (plus `application_id`). Clears (`activity: null`) work the same way. |
| `SUBSCRIBE`, `UNSUBSCRIBE` | Acknowledged locally — there is no voice/guild backend to subscribe to. |
| `GET_USER` | Returns the current identity, or `null` when the id names somebody else (a missing id resolves to self). |
| `INVITE_BROWSER`, `GUILD_TEMPLATE_BROWSER`, `GIFT_CODE_BROWSER` | Forwarded to bridge clients, then ACKed. The websocket transport validates the code first (`4011` invalid invite, `4017` invalid guild template, `4016` invalid gift code). |
| `DEEP_LINK` | Forwarded, then ACKed. |
| `CONNECTIONS_CALLBACK` | Refused with an error (code `1000`, "CONNECTIONS_CALLBACK is not supported"). |

A `SET_ACTIVITY` with no `args` at all is treated as malformed input, not a
clear: it gets error `4005` ("Missing activity args") and changes nothing.

### Commands rsRPC cannot back

These answer with official-shaped errors instead of hanging:

| Commands | Code | Message |
|---|---|---|
| `AUTHORIZE`, `AUTHENTICATE` | `5000` | Authorization requires the real Discord client |
| Activity invites (`SEND_ACTIVITY_JOIN_INVITE`, `CLOSE_ACTIVITY_REQUEST`, `ACCEPT_ACTIVITY_INVITE`, `ACTIVITY_INVITE_USER`) | `5006` | No eligible activity: invites require the real Discord client |
| Voice, guilds, channels, overlay, store, capture (e.g. `GET_GUILD`, `SELECT_VOICE_CHANNEL`, `SET_VOICE_SETTINGS`, `OVERLAY`, …) | `1000` | requires the real Discord client |
| Anything else unknown | `1000` | Unknown command |

Why: OAuth needs the real client's modal and the app's `client_secret`,
and the other commands need live client state that only Discord itself
has. The README's known-limitations section explains what that means for
games that try to log in over RPC.

## Events

Only three events are ever dispatched to clients:

- `READY` — on connect (handshake above).
- `ERROR` — for refused commands.
- `CURRENT_USER_UPDATE` — when the identity changes (bridge `SET_USER` /
  `RESET_USER`); game clients see it on their next handshake.

## The bridge side

Everything a game publishes is forwarded to bridge clients (JSON on
1337–1347, MessagePack on 1338+), keyed by `socketId` — the pid for
game-SDK frames, the app id for process-detected cards. Details, including
control messages and delivery behavior, are in
[web-client.md](web-client.md).

## See also

- [process-detection.md](process-detection.md) — games that never connect
  at all (process scanning and the IPC-wins handoff).
- [cli-options.md](cli-options.md) — port and transport flags.
