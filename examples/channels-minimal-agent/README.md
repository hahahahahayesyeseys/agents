# Channels minimal agent

A small agent that owns its turns and transcript, served to browsers through Channels.

- `src/server.ts` is the agent: `Sessions` holds the transcript, `Tasks` runs each turn, and `Channels` with the Web Channel carries events in and responses out.
- `src/client.tsx` is a browser client built on `WebChannelClient` from `agents/channels/web/client`.

It shows a client tool (`getLocation`, run by the browser of the participant who asked), a tool that needs approval (`flipCoin`), queued messages, and stopping a turn.

## Run it

```sh
pnpm install
pnpm start
```

Open the printed URL. Add `#some-name` to the URL to use a separate conversation.

## Chat from a terminal

With the dev server running, in another terminal:

```sh
pnpm tui [room]
```

This runs `@ai-sdk/tui` over `WebChannelChatTransport` from `agents/channels/web/ai-sdk`. The TUI runs `getLocation` itself and asks for approvals with `y` / `n`. Set `AGENT_ORIGIN` if the dev server is not on `ws://localhost:5173`. The browser and the TUI can share a room. Each browser and each TUI is its own participant (the client names itself with `?as=`, which only a demo should trust), so each runs only the `getLocation` calls from its own messages and shows the rest as not its to run.
