# Pi harness on Channels (spike)

Experimental proof of concept. It serves the example `PiHarness` from
[`../harnesses/pi`](../harnesses/pi) through Channels, so the Web Channel browser client and
`npx agents tui` talk to a pi-durable agent. It is not well engineered and is
not meant to merge as is.

- `src/server.ts` is one Durable Object per room: `PiHarness` (imported from
  `../harnesses/pi/src/harness`), `Streams`, `Channels` with the Web Channel, and
  `WebSockets`. The Worker is a `ChannelGateway`.
- `src/harness-channels.ts` is the bridge. It is typed against
  `ConversationHarness`, the part of the shared `examples/next/harnesses`
  shape it uses (`submit`, `abort`, `wait`, `messages`,
  `session().events()`). `PiHarness` satisfies it structurally.
- `src/client.tsx` is the minimal example's browser client.

## How the two models line up

| Channels                              | Harness                                                                              |
| ------------------------------------- | ------------------------------------------------------------------------------------ |
| Inbound `message` event               | `harness.submit(text, { operationId: eventId })`                                     |
| Turn id                               | Operation id (the event id), so pi deduplicates redelivery                           |
| Turn `queued` / `running` / `settled` | pi `submission` events: `queued`, `placed`, `done` / `unanswered`                    |
| Response                              | One per pi run, fed from `session.events()` (`message_update`, `tool_execution_end`) |
| Transcript and snapshot               | `harness.messages()`, tool results folded into their call                            |
| `cancel`                              | `harness.abort({ operationId })`                                                     |
| `approval-response`, `tool-result`    | Ignored: pi-durable has neither yet                                                  |

What this surfaced:

- The harness owns the transcript and its ids. A user message gets a pi
  entry id, so the bridge maps entry ids back to the surface's message id
  (KV `user:<entryId>`) to avoid a duplicate in the snapshot.
- One pi run can answer several submissions (steers join it). Channels has
  one response per turn, so those turns share the run's response id.
- A pi run spans several assistant messages (one per tool round). Channels
  folds a response into one message, then replaces it with the published
  messages when the turn settles.
- Events are pi's own `AgentEvent`s. The shared harness shape does not
  standardise events, so the event mapping is the pi-specific part. A
  Channels response grammar is close to what a standard harness event
  stream could be.
- The event watch is in memory. `onStart` re-attaches it, but a run that
  was in flight across an eviction is not resumed into a response.

## Run it

```sh
pnpm install
pnpm start
```

The example uses the remote Workers AI binding. If your Wrangler login has
more than one account, set `CLOUDFLARE_ACCOUNT_ID`.

Open the printed URL, and add `#room` for a separate conversation. In
another terminal, with the dev server running:

```sh
pnpm tui [room]
```

Set `AGENT_ORIGIN` if the dev server is not on `http://localhost:5173`. The
browser and the TUI can share a room and see each other's turns.

Try `What time is it?`.
