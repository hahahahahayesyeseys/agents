import { env } from "cloudflare:workers";
import { describe, expect, it } from "vitest";
import { WEB_IDENTITY_HEADER, type ServerFrame } from "../../channels/web";

/** The headers the gateway sets on an upgrade it has resolved. */
function upgrade(participant: string) {
  return {
    Upgrade: "websocket",
    [WEB_IDENTITY_HEADER]: JSON.stringify({
      conversationId: "default",
      participant: { id: participant }
    })
  };
}

type Harness = DurableObjectStub<
  import("../capabilities/channels").ChannelsHarnessObject
>;

function harness(): Harness {
  return env.ChannelsHarnessObject.getByName(crypto.randomUUID());
}

type Client = {
  socket: WebSocket;
  next(): Promise<ServerFrame>;
  /** Frames already received and not yet read. */
  buffered(): ServerFrame[];
  send(frame: unknown): void;
};

async function connect(stub: Harness, participant = "alice"): Promise<Client> {
  const response = await stub.fetch("https://example.com/channels", {
    headers: upgrade(participant)
  });
  expect(response.status).toBe(101);
  const socket = response.webSocket as WebSocket;
  socket.accept();
  const frames: ServerFrame[] = [];
  const waiters: ((frame: ServerFrame) => void)[] = [];
  socket.addEventListener("message", (event) => {
    const frame = JSON.parse(event.data as string) as ServerFrame;
    // The agent's own WebSockets frames share the connection.
    if (!frame.type.startsWith("channels:")) return;
    const waiter = waiters.shift();
    if (waiter) waiter(frame);
    else frames.push(frame);
  });
  const client: Client = {
    socket,
    next: () =>
      frames.length > 0
        ? Promise.resolve(frames.shift() as ServerFrame)
        : new Promise((resolve) => waiters.push(resolve)),
    buffered: () => frames.splice(0),
    send: (frame) => socket.send(JSON.stringify(frame))
  };
  expect((await client.next()).type).toBe("channels:snapshot");
  return client;
}

function message(eventId: string, text: string) {
  return {
    type: "channels:event",
    event: {
      type: "message",
      eventId,
      message: { id: eventId, role: "user", parts: [{ type: "text", text }] }
    }
  };
}

const settle = () => new Promise((resolve) => setTimeout(resolve, 20));

describe("Channels over the Web Channel", () => {
  it("sends the snapshot with the connection's participant", async () => {
    const stub = harness();
    await stub.setSnapshot({
      messages: [{ id: "m1", role: "user", parts: [] }],
      turns: [{ turnId: "t1", startedBy: "e1", status: "queued" }]
    });
    const response = await stub.fetch("https://example.com/channels", {
      headers: upgrade("alice")
    });
    const socket = response.webSocket as WebSocket;
    socket.accept();
    const frame = await new Promise<ServerFrame>((resolve) =>
      socket.addEventListener("message", (event) => {
        const frame = JSON.parse(event.data as string) as ServerFrame;
        if (frame.type.startsWith("channels:")) resolve(frame);
      })
    );
    expect(frame).toEqual({
      type: "channels:snapshot",
      you: { id: "alice" },
      messages: [{ id: "m1", role: "user", parts: [] }],
      turns: [{ turnId: "t1", startedBy: "e1", status: "queued" }]
    });
  });

  // T38, T43
  it("stamps the connection's identity on inbound events, then acks", async () => {
    const stub = harness();
    const alice = await connect(stub, "alice");
    alice.send({
      type: "channels:event",
      event: {
        type: "cancel",
        eventId: "e1",
        turnId: "t1",
        participant: { id: "mallory" }
      }
    });
    expect(await alice.next()).toEqual({ type: "channels:ack", eventId: "e1" });
    const [received] = await stub.getReceived();
    expect(received.event).toEqual({
      type: "cancel",
      eventId: "e1",
      turnId: "t1"
    });
    expect(received.origin.participant).toEqual({ id: "alice" });
    expect(received.origin.conversationId).toBe("default");

    alice.send({
      type: "channels:event",
      event: { type: "message", eventId: "e2", message: { role: "user" } }
    });
    expect(await alice.next()).toEqual({
      type: "channels:ack",
      eventId: "e2",
      error: "Invalid frame: message.id"
    });
  });

  // Echo on accept; A3 (a rejected message is never shown)
  it("echoes an accepted message to other connections only", async () => {
    const stub = harness();
    const alice = await connect(stub, "alice");
    const bob = await connect(stub, "bob");

    alice.send(message("e1", "hi"));
    expect(await alice.next()).toEqual({ type: "channels:ack", eventId: "e1" });
    expect(await bob.next()).toEqual({
      type: "channels:messages",
      messages: [
        { id: "e1", role: "user", parts: [{ type: "text", text: "hi" }] }
      ]
    });

    await stub.setRejectEvents(true);
    alice.send(message("e2", "no"));
    expect(await alice.next()).toEqual({
      type: "channels:ack",
      eventId: "e2",
      error: "Event rejected"
    });
    await settle();
    expect(bob.buffered()).toEqual([]);
    expect(alice.buffered()).toEqual([]);
  });

  // T56
  it("delivers a repeated event to the agent once", async () => {
    const stub = harness();
    const alice = await connect(stub);
    const cancel = {
      type: "channels:event",
      event: { type: "cancel", eventId: "e1", turnId: "t1" }
    };
    alice.send(cancel);
    alice.send(cancel);
    await alice.next();
    await alice.next();
    expect(await stub.getReceived()).toHaveLength(1);
  });

  // T3, T12
  it("replays a response's prefix, then follows its live tail", async () => {
    const stub = harness();
    const alice = await connect(stub);
    const responseId = await stub.openResponse("t1");
    await stub.append(responseId, { type: "text-start", id: "a" });
    await stub.append(responseId, { type: "text-delta", id: "a", delta: "He" });

    alice.send({ type: "channels:subscribe", responseId });
    expect(await alice.next()).toEqual({
      type: "channels:chunks",
      responseId,
      from: 0,
      chunks: [
        { type: "text-start", id: "a" },
        { type: "text-delta", id: "a", delta: "He" }
      ]
    });
    expect(await alice.next()).toEqual({
      type: "channels:caught-up",
      responseId,
      cursor: 2
    });

    await stub.append(responseId, { type: "text-delta", id: "a", delta: "y" });
    expect(await alice.next()).toEqual({
      type: "channels:chunks",
      responseId,
      from: 2,
      chunks: [{ type: "text-delta", id: "a", delta: "y" }]
    });
    await stub.end(responseId);
    expect(await alice.next()).toEqual({
      type: "channels:end",
      responseId,
      state: "ended"
    });
  });

  // T1, T2, T14
  it("fails an append that breaks the grammar or the size limit", async () => {
    const stub = harness();
    const responseId = await stub.openResponse("t1");
    expect(
      await stub.append(responseId, { type: "text-delta", id: "a", delta: "x" })
    ).toBe("ResponseGrammarError");
    await stub.append(responseId, { type: "text-start", id: "a" });
    expect(
      await stub.append(responseId, { type: "reasoning-start", id: "a" })
    ).toBeUndefined();
    expect(await stub.append(responseId, { type: "text-start", id: "a" })).toBe(
      "ResponseGrammarError"
    );
    expect(
      await stub.append(responseId, {
        type: "text-delta",
        id: "a",
        delta: "x".repeat(2000)
      })
    ).toBe("StreamSerializationError");
    await stub.append(responseId, { type: "text-end", id: "a" });
    expect(
      await stub.append(responseId, { type: "text-delta", id: "a", delta: "x" })
    ).toBe("ResponseGrammarError");
    await stub.end(responseId);
    expect(await stub.readChunks(responseId)).toEqual([
      { type: "text-start", id: "a" },
      { type: "reasoning-start", id: "a" },
      { type: "text-end", id: "a" }
    ]);
  });

  // T15, T16
  it("marks a response left streaming as interrupted when the object wakes", async () => {
    const stub = harness();
    const alice = await connect(stub);
    const responseId = await stub.openResponse("t1");
    await stub.append(responseId, { type: "text-start", id: "a" });

    await stub.wake();
    expect(await alice.next()).toEqual({
      type: "channels:end",
      responseId,
      state: "interrupted"
    });

    alice.send({ type: "channels:subscribe", responseId });
    expect((await alice.next()).type).toBe("channels:chunks");
    expect((await alice.next()).type).toBe("channels:caught-up");
    expect(await alice.next()).toEqual({
      type: "channels:end",
      responseId,
      state: "interrupted"
    });
  });

  it("sends a generic failure unless the agent gives the error", async () => {
    const stub = harness();
    const alice = await connect(stub);
    await stub.publishTurn({
      turnId: "t1",
      startedBy: "e1",
      status: "settled",
      outcome: "failed",
      messageIds: []
    });
    expect(await alice.next()).toEqual({
      type: "channels:turn",
      turn: {
        turnId: "t1",
        startedBy: "e1",
        status: "settled",
        outcome: "failed",
        messageIds: [],
        error: "The turn failed."
      }
    });
  });

  // T60
  it("deletes every response on reset and pushes an empty snapshot", async () => {
    const stub = harness();
    const alice = await connect(stub);
    const live = await stub.openResponse("t1");
    const ended = await stub.openResponse("t1");
    await stub.end(ended);

    await stub.reset();
    expect(await alice.next()).toEqual({
      type: "channels:snapshot",
      you: { id: "alice" },
      messages: [],
      turns: []
    });
    expect(await stub.responseState(live)).toBeNull();
    expect(await stub.responseState(ended)).toBeNull();
    expect(await stub.append(live, { type: "text-start", id: "a" })).toBe(
      "StreamClosedError"
    );

    alice.send({ type: "channels:subscribe", responseId: live });
    expect(await alice.next()).toEqual({
      type: "channels:end",
      responseId: live,
      state: "not-found"
    });
  });
});
