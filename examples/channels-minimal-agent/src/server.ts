import { DurableObject } from "cloudflare:workers";
import { ChannelGateway, Channels, type GatewayEvent } from "agents/channels";
import type {
  EventOrigin,
  ResponseWriter,
  InboundEvent,
  TurnOutcome,
  TurnStatus
} from "agents/channels";
import {
  answerToolCall,
  awaitsInput,
  toResponseChunks,
  toTranscriptMessage,
  toUIMessage
} from "agents/channels/ai-sdk";
import { WebChannel } from "agents/channels/web";
import { Lifecycle } from "agents/lifecycle";
import { Sessions } from "agents/sessions";
import { Streams } from "agents/streams";
import { Tasks, type TaskStep } from "agents/tasks";
import { WebSockets } from "agents/websockets";
import {
  convertToModelMessages,
  stepCountIs,
  streamText,
  tool,
  toUIMessageStream
} from "ai";
import type { UIMessage } from "ai";
import { createWorkersAI } from "workers-ai-provider";
import { z } from "zod";

const tools = {
  // No execute: the browser of the participant who asked runs it.
  getLocation: tool({
    description: "Get the user's location",
    inputSchema: z.object({})
  }),
  // Harmless, so the example can show an approval without side effects.
  flipCoin: tool({
    description: "Flip a coin, once the user approves",
    inputSchema: z.object({}),
    needsApproval: true,
    execute: async () => (Math.random() < 0.5 ? "Heads" : "Tails")
  })
};

/** Saved messages remember who may run their client tool calls. */
type Message = UIMessage<{ owner?: string }>;

/** The published status, plus what the agent keeps to itself. */
type Turn = TurnStatus & {
  conversationId: string;
  /** The participant who started the turn. */
  owner: string;
  runId?: string;
};

/** One conversation per object. One turn runs at a time; messages queue. */
export class MinimalAgent extends DurableObject<Env> {
  readonly sessions = new Sessions();
  readonly transcript = this.sessions.session();
  readonly streams = new Streams();
  readonly websockets = new WebSockets();
  readonly tasks = new Tasks({
    definitions: {
      "turn@v1": (input: { turnId: string }, step: TaskStep) =>
        step.do("attempt", ({ signal }) => this.#runTurn(input.turnId, signal))
    }
  });
  readonly channels = new Channels({
    streams: this.streams,
    channels: {
      web: new WebChannel({ websockets: this.websockets })
    },
    snapshot: async () => ({
      messages: (await this.#messages()).map(toTranscript),
      turns: this.#turns().filter(isOpen)
    }),
    onEvent: (event, origin) => this.#onEvent(event, origin)
  });
  readonly lifecycle = Lifecycle.install(this)
    .use(this.sessions)
    .use(this.streams)
    .use(this.tasks)
    .use(this.channels)
    .use(this.websockets);

  /** Inbound events the gateway routes here. */
  receive(event: GatewayEvent, origin: EventOrigin) {
    return this.channels.receive(event, origin);
  }

  async #onEvent(event: InboundEvent, origin: EventOrigin) {
    if (event.type === "message") {
      await this.transcript.appendMessage(toUIMessage(event.message));
      return this.#start({
        turnId: crypto.randomUUID(),
        startedBy: event.eventId,
        status: "queued",
        conversationId: origin.conversationId,
        owner: origin.participant.id
      });
    }
    const turn = this.#turns().find((t) => t.turnId === event.turnId);
    if (!turn || !isOpen(turn)) return;
    if (event.type === "cancel") {
      if (turn.runId) await this.tasks.cancel(turn.runId);
      if (turn.status !== "running") await this.#settle(turn, "aborted");
      return;
    }
    // Only a turn awaiting input continues, and only the owner runs tools.
    if (turn.status !== "settled") return;
    if (event.type === "tool-result" && origin.participant.id !== turn.owner) {
      return;
    }
    const answered = answerToolCall(await this.#messages(), event);
    if (!answered) return; // The first answer wins.
    await this.transcript.updateMessage(answered);
    await this.#start({ ...turn, status: "queued" });
  }

  // Each attempt starts from the saved transcript. Unsaved output is lost.
  #runTurn = serially(async (turnId: string, signal: AbortSignal) => {
    const queued = this.#turns().find((t) => t.turnId === turnId);
    if (!queued || queued.status === "settled") return; // Cancelled.
    const { conversationId } = queued;
    const saved = await this.#messages();
    const last = saved.at(-1);
    const response = await this.channels.openResponse(conversationId, turnId);
    const turn: Turn = {
      ...queued,
      status: "running",
      responseId: response.id,
      extends: last?.role === "assistant" ? last.id : undefined
    };
    await this.#save(turn);

    try {
      await this.#attempt(turn, saved, response, signal);
    } catch (error) {
      // A failure outside the response still settles the turn, so no
      // client is left showing it running.
      console.error(`Turn "${turnId}" failed`, error);
      try {
        response.end(); // Unless the response already ended.
      } catch {}
      await this.#settle(turn, signal.aborted ? "aborted" : "failed");
    }
  });

  async #attempt(
    turn: Turn,
    saved: Message[],
    response: ResponseWriter,
    signal: AbortSignal
  ) {
    const { conversationId, owner } = turn;
    let message: Message | undefined;
    const result = streamText({
      model: createWorkersAI({ binding: this.env.AI })(
        "@cf/moonshotai/kimi-k2.7-code"
      ),
      messages: await convertToModelMessages(saved),
      tools,
      stopWhen: stepCountIs(5),
      abortSignal: signal
    });
    const stream = toUIMessageStream<typeof tools, Message>({
      stream: result.stream,
      tools,
      originalMessages: saved,
      generateMessageId: () => crypto.randomUUID(),
      onEnd: (finished) => {
        message = { ...finished.responseMessage, metadata: { owner } };
      }
    });
    let failed = false;
    await response
      .pipe(toResponseChunks(stream, { tools, owner }))
      .catch(() => (failed = true));

    if (message) {
      await this.transcript.upsertMessage(message);
      await this.channels.publishMessages(conversationId, [
        toTranscript(message)
      ]);
    }
    if (signal.aborted) return this.#settle(turn, "aborted", message);
    if (failed) return this.#settle(turn, "failed", message);
    const waiting = message && awaitsInput(message);
    await this.#settle(turn, waiting ? "awaiting-input" : "completed", message);
  }

  async #start(turn: Turn) {
    const receipt = await this.tasks.run("turn@v1", { turnId: turn.turnId });
    await this.#save({ ...turn, runId: receipt.runId });
  }

  #settle(turn: Turn, outcome: TurnOutcome, message?: Message) {
    const messageIds = message ? [message.id] : [];
    return this.#save({ ...turn, status: "settled", outcome, messageIds });
  }

  async #save(turn: Turn) {
    this.ctx.storage.kv.put(`turn:${turn.turnId}`, turn);
    await this.channels.publishTurn(turn.conversationId, turn);
  }

  #turns() {
    const turns = this.ctx.storage.kv.list<Turn>({ prefix: "turn:" });
    return Array.from(turns, ([, turn]) => turn);
  }

  async #messages() {
    return (await this.transcript.getHistory()) as Message[];
  }
}

function isOpen(turn: TurnStatus) {
  return turn.status !== "settled" || turn.outcome === "awaiting-input";
}

function toTranscript(message: Message) {
  const owner = message.metadata?.owner;
  return toTranscriptMessage(message, { tools, owner });
}

/** Runs calls one at a time, in the order they were made. */
function serially<A extends unknown[]>(fn: (...args: A) => Promise<void>) {
  let queue = Promise.resolve();
  return (...args: A) => {
    const run = queue.then(() => fn(...args));
    queue = run.catch(() => {});
    return run;
  };
}

export default {
  fetch: async (request: Request, env: Env) =>
    (await new ChannelGateway({
      channels: {},
      agent: (room) => env.MinimalAgent.getByName(room),
      // Demo only: the client names itself. A real app resolves the
      // participant from a session it can verify.
      web: (request) => {
        const url = new URL(request.url);
        const room = /^\/channels\/([^/]+)$/.exec(url.pathname)?.[1];
        const id = url.searchParams.get("as") ?? "anonymous";
        return room ? { route: room, participant: { id } } : undefined;
      }
    }).fetch(request)) ?? new Response("Not found", { status: 404 })
} satisfies ExportedHandler<Env>;
