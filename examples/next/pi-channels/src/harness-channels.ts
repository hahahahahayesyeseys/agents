import type { AssistantMessage } from "@earendil-works/pi-ai";
import type {
  AgentEvent,
  AgentEventStream,
  MessageChange,
  SubmissionRecord,
  UserInput
} from "@earendil-works/pi-durable";
import type {
  Channels,
  ConversationSnapshot,
  EventOrigin,
  InboundEvent,
  Json,
  MessagePart,
  ResponseChunk,
  ResponseWriter,
  ToolPart,
  TranscriptMessage,
  TurnStatus
} from "agents/channels";
import { projectEntry } from "../../harnesses/pi/src/harness/messages";
import type {
  PiMessage,
  PiOperationResult,
  PiReceipt,
  PiSubmitOptions
} from "../../harnesses/pi/src/harness/types";

/**
 * The harness shape shared by `examples/next/harnesses` (`submit`, `abort`,
 * `wait`, `messages`, `session(id).events()`), as far as this bridge uses
 * it. `PiHarness` satisfies it structurally. The events stay pi's own: the
 * shared shape does not standardise them yet, so `#onEvents` is the only
 * pi-specific part.
 */
export type ConversationHarness = {
  submit(input: UserInput, options?: PiSubmitOptions): Promise<PiReceipt>;
  abort(options?: { operationId?: string }): Promise<boolean>;
  wait(operationId: string): Promise<PiOperationResult>;
  messages(): Promise<PiMessage[]>;
  session(): { events(): Promise<AgentEventStream> };
};

type Turn = TurnStatus & { conversationId: string };

/**
 * Spike: serve one harness session as one Channels conversation.
 *
 * - An inbound message is a harness submission. Its event id is the
 *   operation id and the turn id, so redelivery is deduplicated by pi.
 * - The harness owns the transcript. The snapshot and every published
 *   message are projections of `harness.messages()`.
 * - One pi run is one Channels response, fed from `session.events()`.
 *   Submissions that join a run (steers) share its response.
 *
 * Turns and the user-message id map live in KV. The event watch is in
 * memory, so the host calls `attach()` from `onStart`.
 */
export class HarnessChannels {
  readonly #harness: ConversationHarness;
  readonly #channels: () => Channels;
  readonly #kv: SyncKvStorage;
  #watch: Promise<AgentEventStream> | undefined;
  #run: Run | undefined;
  readonly #settling = new Set<string>();
  /** pi submission id → operation id, from `submission` events. */
  readonly #operations = new Map<string, string>();

  constructor(options: {
    harness: ConversationHarness;
    channels: () => Channels;
    kv: SyncKvStorage;
  }) {
    this.#harness = options.harness;
    this.#channels = options.channels;
    this.#kv = options.kv;
  }

  // ── ConversationAgent ───────────────────────────────────────────────────

  async snapshot(conversationId: string): Promise<ConversationSnapshot> {
    this.#kv.put("conversation", conversationId);
    void this.attach();
    return {
      messages: await this.#transcript(),
      turns: this.#turns().filter(isOpen)
    };
  }

  async onEvent(event: InboundEvent, origin: EventOrigin): Promise<void> {
    this.#kv.put("conversation", origin.conversationId);
    await this.attach();
    if (event.type === "message") {
      const text = event.message.parts
        .map((part) => (part.type === "text" ? part.text : ""))
        .join("");
      // The surface's message id, which may differ from its event id.
      this.#kv.put(`message:${event.eventId}`, event.message.id);
      if (!this.#turn(event.eventId)) {
        await this.#save({
          turnId: event.eventId,
          startedBy: event.eventId,
          status: "queued",
          conversationId: origin.conversationId
        });
      }
      // pi deduplicates by operation id, so redelivery is harmless.
      await this.#harness.submit(text, { operationId: event.eventId });
      return;
    }
    if (event.type === "cancel") {
      await this.#harness.abort({ operationId: event.turnId });
      return;
    }
    // pi-durable has no approvals or client tools yet (see ../../harnesses/pi/NOTES.md).
  }

  /** Start following the harness's events. Idempotent. */
  attach(): Promise<AgentEventStream> {
    this.#watch ??= this.#harness
      .session()
      .events()
      .then((stream) => {
        stream.start((events) => this.#onEvents(events));
        void stream.closed.then(() => (this.#watch = undefined));
        return stream;
      })
      .catch((error: unknown) => {
        this.#watch = undefined;
        throw error;
      });
    return this.#watch;
  }

  // ── pi events → Channels ────────────────────────────────────────────────

  async #onEvents(events: readonly AgentEvent[]): Promise<void> {
    for (const event of events) {
      try {
        await this.#onEvent(event);
      } catch (error) {
        console.warn("pi → Channels", event.type, error);
      }
    }
  }

  async #onEvent(event: AgentEvent): Promise<void> {
    switch (event.type) {
      case "submission":
        return this.#onSubmission(event.record);
      case "run_start": {
        const turnIds = event.inputs.flatMap((id) => {
          const operation = this.#operations.get(String(id));
          return operation ? [operation] : [];
        });
        const first = turnIds[0] ?? this.#turns().find(isQueued)?.turnId;
        const conversationId = this.#conversation();
        if (!first || !conversationId) return;
        const response = await this.#channels().openResponse(
          conversationId,
          first
        );
        this.#run = new Run(response);
        for (const turnId of turnIds.length ? turnIds : [first]) {
          await this.#join(turnId);
        }
        return;
      }
      case "message_start":
        if (event.message.role === "assistant") this.#run?.nextMessage();
        return;
      case "message_update":
        this.#run?.apply(event.changes);
        return;
      case "message_end": {
        this.#run?.closeParts();
        const message = projectEntry(event.entry);
        if (message?.role !== "assistant") return;
        for (const part of message.parts) {
          if (part.type === "tool-call") {
            this.#run?.toolCall(part.id, part.name, part.arguments as Json);
          }
        }
        return;
      }
      case "tool_execution_end": {
        const result = event.entry && projectEntry(event.entry)?.parts[0];
        if (result?.type !== "tool-result") return;
        this.#run?.append(
          result.error
            ? {
                type: "tool-output-error",
                toolCallId: event.toolCallId,
                errorText: toolText(result.content)
              }
            : {
                type: "tool-output-available",
                toolCallId: event.toolCallId,
                output: toolOutput(result.content, result.details)
              }
        );
        return;
      }
      case "run_end": {
        const run = this.#run;
        this.#run = undefined;
        run?.end();
        await this.#publishTranscript();
        // Settle anything the run owned that pi has not reported yet.
        for (const id of event.inputs) {
          const operation = this.#operations.get(String(id));
          if (operation) void this.#settleFromWait(operation);
        }
        return;
      }
      default:
        return;
    }
  }

  async #onSubmission(record: SubmissionRecord): Promise<void> {
    if (record.type !== "input" || record.requestId === undefined) return;
    const turnId = record.requestId;
    this.#operations.set(String(record.id), turnId);
    if (record.status === "placed") {
      // Show the user's message under the id the surface sent it with.
      this.#kv.put(
        `user:${record.entry}`,
        this.#kv.get<string>(`message:${turnId}`) ?? turnId
      );
      if (this.#run) await this.#join(turnId); // a steer joins the run
      return;
    }
    if (record.status === "done") {
      return this.#settle(turnId, { outcome: "completed" });
    }
    if (record.status === "unanswered") {
      const aborted =
        record.reason === "aborted" || record.reason === "withdrawn";
      return this.#settle(
        turnId,
        aborted
          ? { outcome: "aborted" }
          : { outcome: "failed", error: `Not answered: ${record.reason}` }
      );
    }
  }

  async #settleFromWait(turnId: string): Promise<void> {
    const result = await this.#harness.wait(turnId);
    await this.#settle(
      turnId,
      result.status === "done"
        ? { outcome: "completed" }
        : result.reason === "aborted" || result.reason === "withdrawn"
          ? { outcome: "aborted" }
          : { outcome: "failed", error: `Not answered: ${result.reason}` }
    );
  }

  async #join(turnId: string): Promise<void> {
    const turn = this.#turn(turnId);
    if (!turn || turn.status !== "queued" || !this.#run) return;
    await this.#save({
      ...turn,
      status: "running",
      responseId: this.#run.response.id
    });
  }

  async #settle(
    turnId: string,
    end:
      | { outcome: "completed" | "aborted" }
      | { outcome: "failed"; error: string }
  ): Promise<void> {
    const turn = this.#turn(turnId);
    // pi's submission event and the run_end fallback can both get here.
    if (!turn || turn.status === "settled" || this.#settling.has(turnId)) {
      return;
    }
    this.#settling.add(turnId);
    const transcript = await this.#transcript();
    await this.#publish(transcript);
    // The turn's messages are what follows its user message. Not the run's
    // own list: pi often reports the outcome after the run has ended.
    const userId = this.#kv.get<string>(`message:${turnId}`) ?? turnId;
    const start = transcript.findIndex((m) => m.id === userId);
    const messageIds: string[] = [];
    for (const message of start === -1 ? [] : transcript.slice(start + 1)) {
      if (message.role === "user") break;
      messageIds.push(message.id);
    }
    await this.#save({
      turnId: turn.turnId,
      startedBy: turn.startedBy,
      conversationId: turn.conversationId,
      status: "settled",
      messageIds: [...messageIds],
      ...end
    });
  }

  // ── State ───────────────────────────────────────────────────────────────

  async #publishTranscript(): Promise<void> {
    await this.#publish(await this.#transcript());
  }

  async #publish(transcript: TranscriptMessage[]): Promise<void> {
    const conversationId = this.#conversation();
    if (!conversationId) return;
    await this.#channels().publishMessages(conversationId, transcript);
  }

  async #transcript(): Promise<TranscriptMessage[]> {
    return toTranscript(await this.#harness.messages(), (entryId) =>
      this.#kv.get<string>(`user:${entryId}`)
    );
  }

  async #save(turn: Turn): Promise<void> {
    this.#kv.put(`turn:${turn.turnId}`, turn);
    await this.#channels().publishTurn(turn.conversationId, turn);
  }

  #turn(turnId: string): Turn | undefined {
    return this.#kv.get<Turn>(`turn:${turnId}`);
  }

  #turns(): Turn[] {
    return Array.from(this.#kv.list<Turn>({ prefix: "turn:" }), ([, t]) => t);
  }

  #conversation(): string | undefined {
    return this.#kv.get<string>("conversation");
  }
}

/** One pi run streaming into one Channels response. */
class Run {
  /** Open text and reasoning parts of the live message, by content index. */
  #open = new Map<number, { kind: "text" | "reasoning"; id: string }>();
  #message = 0;
  readonly #calls = new Set<string>();

  constructor(readonly response: ResponseWriter) {}

  nextMessage(): void {
    this.closeParts();
    this.#message += 1;
  }

  apply(changes: readonly MessageChange[]): void {
    for (const change of changes) {
      switch (change.type) {
        case "text_start":
        case "thinking_start": {
          const kind = kindOf(change.block);
          const part = this.#start(change.contentIndex, kind);
          const text = blockText(change.block);
          if (text) {
            this.append({ type: `${kind}-delta`, id: part.id, delta: text });
          }
          break;
        }
        case "text_delta":
        case "thinking_delta": {
          const kind = change.type === "text_delta" ? "text" : "reasoning";
          const part = this.#start(change.contentIndex, kind);
          this.append({
            type: kind === "text" ? "text-delta" : "reasoning-delta",
            id: part.id,
            delta: change.delta
          });
          break;
        }
        case "block":
          if (change.block.type === "toolCall") {
            const { id, name, arguments: input } = change.block;
            this.toolCall(id, name, input as Json);
          } else this.#close(change.contentIndex);
          break;
        default:
          // toolcall_start/delta: the call is shown once its block completes.
          break;
      }
    }
  }

  /** Show a tool call once. pi may complete it in a block or only at message end. */
  toolCall(toolCallId: string, toolName: string, input: Json): void {
    if (this.#calls.has(toolCallId)) return;
    this.#calls.add(toolCallId);
    this.append({ type: "tool-input-start", toolCallId, toolName });
    this.append({ type: "tool-input-available", toolCallId, toolName, input });
  }

  closeParts(): void {
    for (const index of [...this.#open.keys()]) this.#close(index);
  }

  append(chunk: ResponseChunk): void {
    try {
      this.response.append(chunk);
    } catch (error) {
      console.warn("dropped chunk", chunk.type, error);
    }
  }

  end(): void {
    this.closeParts();
    this.response.end();
  }

  #start(index: number, kind: "text" | "reasoning") {
    const open = this.#open.get(index);
    if (open) return open;
    const part = { kind, id: `${this.#message}:${index}` };
    this.#open.set(index, part);
    this.append({ type: `${kind}-start`, id: part.id });
    return part;
  }

  #close(index: number): void {
    const part = this.#open.get(index);
    if (!part) return;
    this.#open.delete(index);
    this.append({ type: `${part.kind}-end`, id: part.id });
  }
}

function blockText(block: AssistantMessage["content"][number]): string {
  if (block.type === "text") return block.text;
  if (block.type === "thinking") return block.thinking;
  return "";
}

function kindOf(block: AssistantMessage["content"][number]) {
  return block.type === "thinking" ? "reasoning" : "text";
}

/** pi's projected messages as a Channels transcript. Tool results fold into their call. */
export function toTranscript(
  messages: readonly PiMessage[],
  userId: (entryId: string) => string | undefined
): TranscriptMessage[] {
  const out: TranscriptMessage[] = [];
  const calls = new Map<string, ToolPart>();
  for (const message of messages) {
    if (message.role === "tool") {
      for (const part of message.parts) {
        const call = part.type === "tool-result" && calls.get(part.id);
        if (!call || part.type !== "tool-result") continue;
        if (part.error) {
          call.state = "output-error";
          call.errorText = toolText(part.content);
        } else {
          call.state = "output-available";
          call.output = toolOutput(part.content, part.details);
        }
      }
      continue;
    }
    const parts: MessagePart[] = [];
    for (const part of message.parts) {
      switch (part.type) {
        case "text":
          parts.push({ type: "text", text: part.text });
          break;
        case "thinking":
          parts.push({ type: "reasoning", text: part.text });
          break;
        case "image":
          parts.push({
            type: "file",
            mediaType: part.mimeType,
            url: `data:${part.mimeType};base64,${part.data}`
          });
          break;
        case "tool-call": {
          const call: ToolPart = {
            type: "tool",
            toolCallId: part.id,
            toolName: part.name,
            state: "input-available",
            input: part.arguments as Json
          };
          calls.set(part.id, call);
          parts.push(call);
          break;
        }
      }
    }
    if (message.error)
      parts.push({ type: "text", text: `Error: ${message.error}` });
    if (message.role === "notice") {
      if (parts.length) out.push({ id: message.id, role: "system", parts });
      continue;
    }
    out.push({
      id:
        message.role === "user"
          ? (userId(message.id) ?? message.id)
          : message.id,
      role: message.role,
      parts
    });
  }
  return out;
}

function toolText(content: readonly { type: string; text?: string }[]): string {
  return content.map((part) => part.text ?? "").join("");
}

function toolOutput(
  content: readonly { type: string; text?: string }[],
  details: unknown
): Json {
  return (details ?? toolText(content)) as Json;
}

function isOpen(turn: TurnStatus): boolean {
  return turn.status !== "settled";
}

function isQueued(turn: TurnStatus): boolean {
  return turn.status === "queued";
}
