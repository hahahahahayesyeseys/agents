// Imported from their modules, not "../lifecycle", which loads
// cloudflare:workers and would stop agents/channels loading outside Workers.
import { LifecycleCapability } from "../lifecycle/capability";
import type { Streams } from "../streams";
import type {
  ConversationAgent,
  ConversationSnapshot,
  EventOrigin,
  InboundEvent,
  ResponseChunk,
  TranscriptMessage,
  TurnStatus
} from "./protocol";
import type { ChannelMessageSurface } from "./surface";
import { responseWriter, type ResponseWriter } from "./response";

const TAG_PREFIX = "channels:";
const GENERIC_FAILURE = "The turn failed.";
// How many recent event ids Channels remembers so a repeated event, such as
// one a client resends after reconnecting, reaches the agent once. Past this
// the oldest is forgotten, and a repeat that old is delivered again.
const SEEN_EVENTS_LIMIT = 1000;

/**
 * An inbound event as the gateway sends it. An approval response from a
 * webhook names no turn; Channels finds it from the conversation snapshot.
 */
export type GatewayEvent =
  | InboundEvent
  | Omit<Extract<InboundEvent, { type: "approval-response" }>, "turnId">;

/** How a response finished, as a reader sees it. */
export type ResponseEnding = "ended" | "interrupted" | "not-found";

/** A change Channels pushes to every channel showing a conversation. */
export type ConversationUpdate =
  | { type: "turn"; turn: TurnStatus }
  | {
      type: "messages";
      messages: TranscriptMessage[];
      /** Set when the messages arrived as an inbound event. */
      origin?: EventOrigin;
      /** The inbound event's id, set with `origin`. */
      eventId?: string;
    }
  | { type: "reset" }
  | { type: "response-end"; responseId: string; ending: ResponseEnding };

export type ResponseReadOptions = {
  from?: number;
  signal?: AbortSignal;
  onChunks(from: number, chunks: ResponseChunk[]): void;
  /** Everything recorded so far has been read; what follows is live. */
  onCaughtUp(cursor: number): void;
};

/** Durable state a channel keeps in the agent, private to that channel. */
export type ChannelState = {
  get<T>(key: string): T | undefined;
  put(key: string, value: unknown): void;
  delete(key: string): void;
};

/** What Channels gives a mounted channel. */
export type ChannelsHost = {
  readonly channelKey: string;
  /** This channel's surfaces that have joined the conversation. */
  surfaces(conversationId: string): Promise<ChannelMessageSurface[]>;
  readonly state: ChannelState;
  snapshot(conversationId: string): Promise<ConversationSnapshot>;
  /**
   * Deliver an event to the agent. Resolves once the agent accepted it and
   * rejects if the agent threw.
   */
  dispatch(event: InboundEvent, origin: EventOrigin): Promise<void>;
  readResponse(
    conversationId: string,
    responseId: string,
    options: ResponseReadOptions
  ): Promise<ResponseEnding>;
};

/** An interface that shows conversations, such as the Web Channel. */
export interface ConversationChannel {
  /** Called once, when Channels is constructed. */
  mount(host: ChannelsHost): void;
  publish(
    conversationId: string,
    update: ConversationUpdate
  ): void | Promise<void>;
}

export type ChannelsOptions = ConversationAgent & {
  /** Install it before Channels. */
  streams: Streams;
  channels: Record<string, ConversationChannel>;
};

/**
 * Carries inbound events to the agent, and responses, turn statuses and
 * transcript updates back to every channel showing the conversation.
 */
export class Channels extends LifecycleCapability {
  readonly #options: ChannelsOptions;
  readonly #channels: [string, ConversationChannel][];
  readonly #queues = new Map<string, Promise<unknown>>();
  /** Recent events by key, each with the outcome repeats receive. */
  readonly #seen = new Map<string, Promise<void>>();

  constructor(options: ChannelsOptions) {
    super("channels");
    this.#options = options;
    this.#channels = Object.entries(options.channels);
    for (const [channelKey, channel] of this.#channels) {
      channel.mount(this.#host(channelKey));
    }
  }

  /** Mark responses left streaming by an earlier instance as interrupted. */
  async onStart(): Promise<void> {
    const streaming = await this.#options.streams.list({
      state: "streaming",
      // Streams lists 100 by default. A loop until none are left would drop
      // the cap, but `list()` matches tags exactly, not by prefix, so streams
      // other capabilities left streaming would come back on every pass.
      limit: 1000
    });
    for (const status of streaming) {
      const conversationId = conversationOf(status.tag);
      if (conversationId === undefined) continue;
      const writer = await this.#options.streams.open(status.streamId);
      writer.error("interrupted");
      await this.#publish(conversationId, {
        type: "response-end",
        responseId: status.streamId,
        ending: "interrupted"
      });
    }
  }

  /**
   * Deliver an inbound event from the gateway, through the same path as
   * the agent's own channels. The agent calls this from its RPC method.
   */
  async receive(event: GatewayEvent, origin: EventOrigin): Promise<void> {
    // The surface joins the conversation, so every turn reaches it.
    const { surface } = origin;
    this.lifecycle.storage.kv.put(
      surfaceKey(origin.conversationId, surface.channelKey, surface.address),
      surface
    );
    await this.#dispatch(
      event.type !== "approval-response" || "turnId" in event
        ? event
        : await this.#withTurn(event, origin),
      origin
    );
  }

  /** Open a new response for a turn. Each attempt and continuation opens one. */
  async openResponse(
    conversationId: string,
    turnId: string
  ): Promise<ResponseWriter> {
    const writer = await this.#options.streams.open(crypto.randomUUID(), {
      tag: TAG_PREFIX + conversationId,
      metadata: { turnId }
    });
    return responseWriter(writer);
  }

  /**
   * Push a turn's status to every channel showing the conversation. Fields
   * beyond `TurnStatus` are dropped, so the agent can pass its turn record.
   */
  async publishTurn(conversationId: string, turn: TurnStatus): Promise<void> {
    await this.#publish(conversationId, { type: "turn", turn: statusOf(turn) });
  }

  /** Publish messages the agent saved. Each replaces any message with its id. */
  async publishMessages(
    conversationId: string,
    messages: TranscriptMessage[]
  ): Promise<void> {
    await this.#publish(conversationId, { type: "messages", messages });
  }

  /**
   * Call after the agent has reset its own transcript and turns. Deletes the
   * conversation's responses, stopping live producers, then pushes an empty
   * snapshot.
   */
  async reset(conversationId: string): Promise<void> {
    const streams = this.#options.streams;
    const tag = TAG_PREFIX + conversationId;
    for (;;) {
      const batch = await streams.list({ tag });
      if (batch.length === 0) break;
      for (const { streamId, state } of batch) {
        if (state === "streaming") (await streams.open(streamId)).close();
        await streams.delete(streamId);
      }
    }
    // Events accepted before the reset may be sent again to rebuild it.
    const prefix = JSON.stringify([conversationId]).slice(0, -1) + ",";
    for (const key of this.#seen.keys()) {
      if (key.startsWith(prefix)) this.#seen.delete(key);
    }
    await this.#publish(conversationId, { type: "reset" });
  }

  async #publish(
    conversationId: string,
    update: ConversationUpdate
  ): Promise<void> {
    await Promise.all(
      this.#channels.map(async ([channelKey, channel]) => {
        try {
          await channel.publish(conversationId, update);
        } catch (error) {
          console.error(`Channel "${channelKey}" failed to publish`, error);
        }
      })
    );
  }

  #host(channelKey: string): ChannelsHost {
    const kv = () => this.lifecycle.storage.kv;
    const stateKey = (key: string) =>
      `${TAG_PREFIX}state:${JSON.stringify([channelKey, key])}`;
    return {
      channelKey,
      surfaces: async (conversationId) => {
        const prefix = surfaceKey(conversationId, channelKey);
        return [...kv().list<ChannelMessageSurface>({ prefix })].map(
          ([, surface]) => surface
        );
      },
      state: {
        get: (key) => kv().get(stateKey(key)),
        put: (key, value) => kv().put(stateKey(key), value),
        delete: (key) => void kv().delete(stateKey(key))
      },
      snapshot: async (conversationId) => {
        const snapshot = await this.#runInHost(() =>
          this.#options.snapshot(conversationId)
        );
        return { ...snapshot, turns: snapshot.turns.map(statusOf) };
      },
      dispatch: (event, origin) => this.#dispatch(event, origin),
      readResponse: (conversationId, responseId, options) =>
        this.#readResponse(conversationId, responseId, options)
    };
  }

  /** Events take effect in arrival order, one at a time per conversation. */
  #dispatch(event: InboundEvent, origin: EventOrigin): Promise<void> {
    const key = JSON.stringify([origin.conversationId, event.eventId]);
    const seen = this.#seen.get(key);
    if (seen) return seen;

    const previous = this.#queues.get(origin.conversationId);
    const run = async () => {
      try {
        await this.#runInHost(() => this.#options.onEvent(event, origin));
      } catch (error) {
        // A rejected event may be sent again.
        this.#seen.delete(key);
        throw error;
      }
      if (event.type === "message") {
        await this.#publish(origin.conversationId, {
          type: "messages",
          messages: [event.message],
          origin,
          eventId: event.eventId
        });
      }
    };
    const result = previous ? previous.then(run, run) : run();
    this.#seen.set(key, result);
    if (this.#seen.size > SEEN_EVENTS_LIMIT) {
      this.#seen.delete(this.#seen.keys().next().value as string);
    }
    const settled = result.catch(() => {});
    this.#queues.set(origin.conversationId, settled);
    void settled.then(() => {
      if (this.#queues.get(origin.conversationId) === settled) {
        this.#queues.delete(origin.conversationId);
      }
    });
    return result;
  }

  /** Name the turn awaiting an approval that arrived without one. */
  async #withTurn(
    event: Exclude<GatewayEvent, InboundEvent>,
    origin: EventOrigin
  ): Promise<InboundEvent> {
    const { messages, turns } = await this.#runInHost(() =>
      this.#options.snapshot(origin.conversationId)
    );
    const message = messages.find((m) =>
      m.parts.some(
        (part) =>
          part.type === "tool" &&
          part.state === "approval-requested" &&
          part.approval?.id === event.approvalId
      )
    );
    const turn = turns.find(
      (t) =>
        t.status === "settled" &&
        message !== undefined &&
        t.messageIds.includes(message.id)
    );
    if (!turn) throw new Error("No turn is waiting for this approval");
    return { ...event, turnId: turn.turnId };
  }

  async #readResponse(
    conversationId: string,
    responseId: string,
    options: ResponseReadOptions
  ): Promise<ResponseEnding> {
    const streams = this.#options.streams;
    const status = await streams.status(responseId);
    if (status?.tag !== TAG_PREFIX + conversationId) return "not-found";
    let cursor = options.from ?? 0;
    for await (const batch of streams.readBatches(responseId, {
      from: cursor,
      signal: options.signal,
      onUpToDate: () => options.onCaughtUp(cursor)
    })) {
      options.onChunks(
        batch[0].seq,
        // SAFETY: Channels records only chunks that passed the grammar.
        batch.map((item) => item.chunk as unknown as ResponseChunk)
      );
      cursor = batch[batch.length - 1].seq + 1;
    }
    const ending = await streams.status(responseId);
    return ending?.state === "errored" ? "interrupted" : "ended";
  }

  async #runInHost<T>(fn: () => Promise<T>): Promise<T> {
    // SAFETY: runInHostContext resolves with fn's own result.
    return (await this.lifecycle.runInHostContext(fn)) as T;
  }
}

/**
 * Where a surface is recorded. Without an address, the prefix for all of a
 * channel's surfaces in the conversation.
 */
function surfaceKey(
  conversationId: string,
  channelKey: string,
  address?: unknown
): string {
  const scope = `${TAG_PREFIX}surface:${JSON.stringify([conversationId, channelKey])}:`;
  return address === undefined ? scope : scope + JSON.stringify(address);
}

/** A turn status with only its protocol fields. */
function statusOf(turn: TurnStatus): TurnStatus {
  const { turnId, startedBy } = turn;
  switch (turn.status) {
    case "queued":
      return { turnId, startedBy, status: "queued" };
    case "running": {
      const { responseId, extends: extended } = turn;
      return {
        turnId,
        startedBy,
        status: "running",
        responseId,
        ...(extended !== undefined && { extends: extended })
      };
    }
    case "settled": {
      const { messageIds } = turn;
      return turn.outcome === "failed"
        ? {
            turnId,
            startedBy,
            status: "settled",
            outcome: "failed",
            messageIds,
            error: turn.error ?? GENERIC_FAILURE
          }
        : {
            turnId,
            startedBy,
            status: "settled",
            outcome: turn.outcome,
            messageIds
          };
    }
  }
}

function conversationOf(tag: string | undefined): string | undefined {
  return tag?.startsWith(TAG_PREFIX) ? tag.slice(TAG_PREFIX.length) : undefined;
}
