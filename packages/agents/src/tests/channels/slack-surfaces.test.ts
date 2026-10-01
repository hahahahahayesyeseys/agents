import { env } from "cloudflare:workers";
import { describe, expect, it } from "vitest";
import { WEB_IDENTITY_HEADER } from "../../channels/web";

type Harness = DurableObjectStub<
  import("../capabilities/channels").ChannelsHarnessObject
>;

const THREAD = {
  channelKey: "slack",
  version: 1,
  address: { channelId: "CTEAM", threadTs: "1700000000.1" },
  label: "Slack · CTEAM"
} as const;

function harness(): Harness {
  return env.ChannelsHarnessObject.getByName(crypto.randomUUID());
}

/** A Slack thread joins the conversation by sending it a message. */
async function join(stub: Harness, eventId = "s1"): Promise<void> {
  await stub.receive(
    {
      type: "message",
      eventId,
      message: {
        id: eventId,
        role: "user",
        parts: [{ type: "text", text: "hello from Slack" }]
      }
    },
    { conversationId: "default", participant: { id: "U1" }, surface: THREAD }
  );
}

/** Wait until Slack has been called with `method`, then return every call. */
async function slackCallsUntil(stub: Harness, method: string) {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    const calls = await stub.getSlackCalls();
    if (calls.some((call) => call.method === method)) return calls;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`Slack was never called with ${method}`);
}

/** The text a native stream showed, in order. */
function streamed(calls: { method: string; body: Record<string, unknown> }[]) {
  return calls
    .flatMap((call) =>
      Array.isArray(call.body.chunks)
        ? (call.body.chunks as { text?: string }[])
        : []
    )
    .map((chunk) => chunk.text ?? "")
    .join("");
}

/** Run a turn whose response says `text`. */
async function answer(
  stub: Harness,
  turn: { turnId: string; startedBy: string },
  text: string
): Promise<void> {
  const responseId = await stub.openResponse(turn.turnId);
  await stub.publishTurn({ ...turn, status: "running", responseId });
  await stub.append(responseId, { type: "text-start", id: "a" });
  await stub.append(responseId, { type: "text-delta", id: "a", delta: text });
  await stub.end(responseId);
}

describe("Slack surfaces", () => {
  it("streams each turn into a thread that joined", async () => {
    const stub = harness();
    await join(stub);
    await answer(stub, { turnId: "t1", startedBy: "s1" }, "Hi there");

    const calls = await slackCallsUntil(stub, "chat.stopStream");
    expect(calls[0]).toMatchObject({
      method: "chat.startStream",
      body: { channel: "CTEAM", thread_ts: "1700000000.1" }
    });
    // A message from the thread itself is not quoted back to it.
    expect(streamed(calls)).toBe("Hi there");
  });

  it("quotes a message from another surface above its answer", async () => {
    const stub = harness();
    await join(stub);
    const upgrade = await stub.fetch("https://example.com/channels", {
      headers: {
        Upgrade: "websocket",
        [WEB_IDENTITY_HEADER]: JSON.stringify({
          conversationId: "default",
          participant: { id: "alice", name: "Alice" }
        })
      }
    });
    const socket = upgrade.webSocket as WebSocket;
    socket.accept();
    const acked = new Promise<void>((resolve) =>
      socket.addEventListener("message", (event) => {
        if (String(event.data).includes("channels:ack")) resolve();
      })
    );
    socket.send(
      JSON.stringify({
        type: "channels:event",
        event: {
          type: "message",
          eventId: "w1",
          message: {
            id: "w1",
            role: "user",
            parts: [{ type: "text", text: "from the browser" }]
          }
        }
      })
    );
    await acked;

    await answer(stub, { turnId: "t1", startedBy: "w1" }, "Answer");
    const calls = await slackCallsUntil(stub, "chat.stopStream");
    expect(streamed(calls)).toBe("> Alice: from the browser\n\nAnswer");
  });

  it("asks for an approval with buttons, then marks it answered", async () => {
    const stub = harness();
    await join(stub);
    const tool = {
      type: "tool" as const,
      toolCallId: "c1",
      toolName: "deploy",
      input: { env: "prod" }
    };
    await stub.publishMessages([
      {
        id: "m1",
        role: "assistant",
        parts: [
          { ...tool, state: "approval-requested", approval: { id: "ap1" } }
        ]
      }
    ]);
    const asked = await slackCallsUntil(stub, "chat.postMessage");
    const post = asked.find((call) => call.method === "chat.postMessage");
    expect(JSON.stringify(post?.body.blocks)).toContain("ap1");

    await stub.publishMessages([
      {
        id: "m1",
        role: "assistant",
        parts: [
          {
            ...tool,
            state: "approval-responded",
            approval: { id: "ap1", approved: true }
          }
        ]
      }
    ]);
    const calls = await slackCallsUntil(stub, "chat.update");
    expect(calls.find((call) => call.method === "chat.update")).toMatchObject({
      body: { channel: "CTEAM", text: "Approved: *deploy*" }
    });
  });

  it("finishes a stream when its response is interrupted", async () => {
    const stub = harness();
    await join(stub);
    const responseId = await stub.openResponse("t1");
    await stub.publishTurn({
      turnId: "t1",
      startedBy: "s1",
      status: "running",
      responseId
    });
    await stub.append(responseId, { type: "text-start", id: "a" });
    await stub.append(responseId, {
      type: "text-delta",
      id: "a",
      delta: "Par"
    });
    await slackCallsUntil(stub, "chat.appendStream");

    await stub.wake();
    const calls = await slackCallsUntil(stub, "chat.stopStream");
    expect(streamed(calls)).toContain("Par");
  });

  it("tells a thread when a turn fails", async () => {
    const stub = harness();
    await join(stub);
    await stub.publishTurn({
      turnId: "t1",
      startedBy: "s1",
      status: "settled",
      outcome: "failed",
      messageIds: []
    });
    const calls = await slackCallsUntil(stub, "chat.stopStream");
    expect(streamed(calls)).toBe("The turn failed.");
  });
});
