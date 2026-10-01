import { DurableObject } from "cloudflare:workers";
import {
  Channels,
  type ConversationSnapshot,
  type EventOrigin,
  type InboundEvent,
  type ResponseChunk,
  type ResponseWriter,
  type TranscriptMessage,
  type TurnStatus
} from "../../channels";
import { WebChannel } from "../../channels/web";
import { Lifecycle } from "../../lifecycle";
import { Streams } from "../../streams";
import { WebSockets } from "../../websockets";

/**
 * Channels with the Web Channel over a scripted agent. Tests drive the
 * agent's side through RPC and the participant's side through WebSockets.
 * Tests connect with the identity header the gateway would set.
 */
export class ChannelsHarnessObject extends DurableObject<Cloudflare.Env> {
  readonly received: { event: InboundEvent; origin: EventOrigin }[] = [];
  snapshotState: ConversationSnapshot = { messages: [], turns: [] };
  rejectEvents = false;
  readonly #responses = new Map<string, ResponseWriter>();
  readonly streams = new Streams({ maxChunkBytes: 1024 });
  readonly websockets = new WebSockets();
  readonly channels = new Channels({
    streams: this.streams,
    channels: {
      web: new WebChannel({ websockets: this.websockets })
    },
    snapshot: async () => this.snapshotState,
    onEvent: async (event, origin) => {
      if (this.rejectEvents) throw new Error("rejected");
      this.received.push({ event, origin });
    }
  });

  readonly lifecycle = Lifecycle.install(this)
    .use(this.streams)
    .use(this.channels)
    .use(this.websockets);

  getReceived() {
    return this.received;
  }

  setRejectEvents(reject: boolean): void {
    this.rejectEvents = reject;
  }

  setSnapshot(snapshot: ConversationSnapshot): void {
    this.snapshotState = snapshot;
  }

  async openResponse(turnId: string): Promise<string> {
    const response = await this.channels.openResponse("default", turnId);
    this.#responses.set(response.id, response);
    return response.id;
  }

  /** Returns the error name when the append fails. */
  append(responseId: string, chunk: ResponseChunk): string | undefined {
    try {
      this.#responses.get(responseId)?.append(chunk);
      return undefined;
    } catch (error) {
      return error instanceof Error ? error.name : String(error);
    }
  }

  end(responseId: string): void {
    this.#responses.get(responseId)?.end();
  }

  /** Forget the producer and run startup again, as a new instance would. */
  async wake(): Promise<void> {
    this.#responses.clear();
    await this.channels.onStart();
  }

  async readChunks(responseId: string): Promise<unknown[]> {
    const chunks: unknown[] = [];
    for await (const { chunk } of this.streams.read(responseId)) {
      chunks.push(chunk);
    }
    return chunks;
  }

  async responseState(responseId: string): Promise<string | null> {
    return (await this.streams.status(responseId))?.state ?? null;
  }

  publishTurn(turn: TurnStatus): Promise<void> {
    return this.channels.publishTurn("default", turn);
  }

  publishMessages(messages: TranscriptMessage[]): Promise<void> {
    return this.channels.publishMessages("default", messages);
  }

  reset(): Promise<void> {
    return this.channels.reset("default");
  }
}
