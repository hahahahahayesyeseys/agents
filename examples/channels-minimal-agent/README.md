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
