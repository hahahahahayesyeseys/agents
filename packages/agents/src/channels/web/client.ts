import { applyChunks } from "../apply-chunks";
import type {
  InboundEvent,
  Participant,
  ResponseChunk,
  TranscriptMessage,
  TurnStatus
} from "../protocol";
import type { ClientFrame, ServerFrame } from "./protocol";

export type WebChannelClientState = {
  connected: boolean;
  you?: Participant;
  /** The transcript, with the output of running turns applied. */
  messages: TranscriptMessage[];
  /** Turns that are queued, running or awaiting input. */
  turns: TurnStatus[];
};

/** An inbound event; the client fills in `eventId` when it is left out. */
export type ClientEvent = InboundEvent extends infer E
  ? E extends { eventId: string }
    ? Omit<E, "eventId"> & { eventId?: string }
    : never
  : never;

/** Turn statuses and response output, frame by frame. */
export type WebChannelActivity =
  | { type: "turn"; turn: TurnStatus }
  | {
      type: "chunks";
      responseId: string;
      turnId: string;
      chunks: ResponseChunk[];
    }
  | {
      type: "end";
      responseId: string;
      turnId: string;
      state: "ended" | "interrupted" | "not-found";
    };

type Response = {
  turnId: string;
  extends?: string;
  chunks: ResponseChunk[];
  /** Applied to the transcript until the turn moves past it. */
  shown: boolean;
  ended: boolean;
};

/** A browser client for the Web Channel, with reconnects. */
export class WebChannelClient {
  readonly #url: string;
  #socket: WebSocket | undefined;
  #closed = false;
  #connected = false;
  #you: Participant | undefined;
  #transcript: TranscriptMessage[] = [];
  readonly #turns = new Map<string, TurnStatus>();
  readonly #responses = new Map<string, Response>();
  /** Events not yet acknowledged, sent again after each reconnect. */
  readonly #outbox = new Map<
    string,
    { event: InboundEvent; resolve(): void; reject(error: Error): void }
  >();
  readonly #listeners = new Set<(state: WebChannelClientState) => void>();
  readonly #activity = new Set<(activity: WebChannelActivity) => void>();
  #state: WebChannelClientState = { connected: false, messages: [], turns: [] };

  constructor(url: string | URL) {
    this.#url = String(url);
    this.#connect();
  }

  get state(): WebChannelClientState {
    return this.#state;
  }

  /** Calls the listener on every change. Returns the unsubscribe. */
  subscribe(listener: (state: WebChannelClientState) => void): () => void {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  }

  /**
   * Calls the listener with each turn status, response chunk and response
   * ending as it arrives, including chunks that arrive after the turn has
   * settled. Returns the unsubscribe.
   */
  onActivity(listener: (activity: WebChannelActivity) => void): () => void {
    this.#activity.add(listener);
    return () => this.#activity.delete(listener);
  }

  /** The chunks received so far for a response this client is reading. */
  chunksOf(responseId: string): readonly ResponseChunk[] | undefined {
    return this.#responses.get(responseId)?.chunks;
  }

  /**
   * Resolves once the agent accepts the event, and rejects if it refuses.
   * While disconnected the event waits, and it is sent again after a
   * reconnect until acknowledged; Channels drops repeats by `eventId`.
   */
  send(event: ClientEvent): Promise<void> {
    const eventId = event.eventId ?? crypto.randomUUID();
    // SAFETY: the same event, with its id filled in.
    const complete = { ...event, eventId } as InboundEvent;
    return new Promise((resolve, reject) => {
      const accepted = () => {
        // Other connections get the message from Channels; this one adds its
        // own once the agent has accepted it.
        if (complete.type === "message") this.#upsert([complete.message]);
        resolve();
      };
      this.#outbox.set(eventId, { event: complete, resolve: accepted, reject });
      if (this.#connected) {
        this.#send({ type: "channels:event", event: complete });
      }
    });
  }

  /** Closes the connection and rejects events still waiting for an ack. */
  close(): void {
    this.#closed = true;
    this.#socket?.close();
    for (const pending of this.#outbox.values()) {
      pending.reject(new Error("Closed"));
    }
    this.#outbox.clear();
  }

  #connect(): void {
    const socket = new WebSocket(this.#url);
    this.#socket = socket;
    socket.addEventListener("message", (event) => {
      if (typeof event.data !== "string") return;
      const frame = JSON.parse(event.data) as { type?: unknown };
      // Skip the agent's own frames on the shared connection.
      if (
        typeof frame.type === "string" &&
        frame.type.startsWith("channels:")
      ) {
        this.#receive(frame as ServerFrame);
      }
    });
    socket.addEventListener("close", () => {
      this.#connected = false;
      this.#update();
      if (!this.#closed) setTimeout(() => this.#connect(), 1000);
    });
  }

  #send(frame: ClientFrame): void {
    if (this.#socket?.readyState === WebSocket.OPEN) {
      this.#socket.send(JSON.stringify(frame));
    }
  }

  #receive(frame: ServerFrame): void {
    switch (frame.type) {
      case "channels:snapshot":
        this.#connected = true;
        this.#you = frame.you;
        this.#transcript = frame.messages;
        this.#turns.clear();
        this.#resume(frame.turns);
        for (const turn of frame.turns) this.#setTurn(turn);
        for (const { event } of this.#outbox.values()) {
          this.#send({ type: "channels:event", event });
        }
        break;
      case "channels:turn":
        this.#setTurn(frame.turn);
        this.#update();
        this.#emit({ type: "turn", turn: frame.turn });
        return;
      case "channels:messages":
        this.#upsert(frame.messages);
        return;
      case "channels:chunks": {
        const response = this.#responses.get(frame.responseId);
        // Skip chunks already received, and frames past a gap.
        const skip = (response?.chunks.length ?? 0) - frame.from;
        if (!response || skip < 0) return;
        const fresh = frame.chunks.slice(skip);
        if (fresh.length === 0) return;
        response.chunks.push(...fresh);
        this.#emit({
          type: "chunks",
          responseId: frame.responseId,
          turnId: response.turnId,
          chunks: fresh
        });
        if (!response.shown) return;
        break;
      }
      case "channels:ack": {
        const ack = this.#outbox.get(frame.eventId);
        this.#outbox.delete(frame.eventId);
        if (frame.error) ack?.reject(new Error(frame.error));
        else ack?.resolve();
        return;
      }
      case "channels:end": {
        const response = this.#responses.get(frame.responseId);
        if (!response) return;
        response.ended = true;
        // A missing response has nothing to show.
        if (!response.shown || frame.state === "not-found") {
          this.#responses.delete(frame.responseId);
        }
        this.#emit({
          type: "end",
          responseId: frame.responseId,
          turnId: response.turnId,
          state: frame.state
        });
        if (frame.state === "not-found") break;
        return;
      }
      case "channels:caught-up":
        return;
    }
    this.#update();
  }

  #upsert(messages: TranscriptMessage[]): void {
    for (const message of messages) {
      const index = this.#transcript.findIndex((m) => m.id === message.id);
      if (index === -1) this.#transcript.push(message);
      else this.#transcript[index] = message;
    }
    this.#update();
  }

  #emit(activity: WebChannelActivity): void {
    for (const listener of this.#activity) listener(activity);
  }

  /** After a reconnect, keep reading still-running responses where we were. */
  #resume(turns: TurnStatus[]): void {
    const running = new Set(
      turns.flatMap((turn) =>
        turn.status === "running" ? [turn.responseId] : []
      )
    );
    for (const [responseId, response] of this.#responses) {
      if (!running.has(responseId)) {
        this.#responses.delete(responseId);
        continue;
      }
      this.#send({
        type: "channels:subscribe",
        responseId,
        from: response.chunks.length
      });
    }
  }

  #setTurn(turn: TurnStatus): void {
    const responseId = turn.status === "running" ? turn.responseId : undefined;
    // A turn's earlier response is replaced by its next one, or by the
    // saved messages once it settles. It is kept until its end arrives.
    for (const [id, response] of this.#responses) {
      if (response.turnId === turn.turnId && id !== responseId) {
        response.shown = false;
        if (response.ended) this.#responses.delete(id);
      }
    }
    const open = turn.status !== "settled" || turn.outcome === "awaiting-input";
    if (open) this.#turns.set(turn.turnId, turn);
    else this.#turns.delete(turn.turnId);
    if (turn.status === "running" && !this.#responses.has(turn.responseId)) {
      this.#responses.set(turn.responseId, {
        turnId: turn.turnId,
        ...(turn.extends !== undefined && { extends: turn.extends }),
        chunks: [],
        shown: true,
        ended: false
      });
      this.#send({ type: "channels:subscribe", responseId: turn.responseId });
    }
  }

  #update(): void {
    const messages = [...this.#transcript];
    for (const [responseId, live] of this.#responses) {
      if (!live.shown) continue;
      const index = messages.findIndex((m) => m.id === live.extends);
      const base: TranscriptMessage =
        index === -1
          ? { id: responseId, role: "assistant", parts: [] }
          : messages[index];
      const message = applyChunks(base, live.chunks);
      if (index === -1) messages.push(message);
      else messages[index] = message;
    }
    this.#state = {
      connected: this.#connected,
      ...(this.#you && { you: this.#you }),
      messages,
      turns: [...this.#turns.values()]
    };
    for (const listener of this.#listeners) listener(this.#state);
  }
}
