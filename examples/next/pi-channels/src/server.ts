import { DurableObject } from "cloudflare:workers";
import { Type } from "@earendil-works/pi-ai";
import { createRegistry, Harness } from "@earendil-works/pi-durable";
import { ChannelGateway, Channels, type GatewayEvent } from "agents/channels";
import type { EventOrigin } from "agents/channels";
import { WebChannel } from "agents/channels/web";
import { Lifecycle } from "agents/lifecycle";
import { Streams } from "agents/streams";
import { WebSockets } from "agents/websockets";
import { PiHarness } from "../../harnesses/pi/src/harness/pi-harness";
import { createModels } from "../../harnesses/pi/src/providers/models";
import { workersAI } from "../../harnesses/pi/src/providers/workers-ai";
import { HarnessChannels } from "./harness-channels";

const MODEL_ID = "@cf/moonshotai/kimi-k2.7-code";

function createAppRegistry() {
  const registry = createRegistry();
  registry.systemPrompt.section(
    "preamble",
    () =>
      "You are a concise assistant. Use current_time when asked about the time.",
    { tag: false }
  );
  registry.tools.add({
    name: "current_time",
    description: "Return the current UTC time.",
    parameters: Type.Object({}),
    replay: "safe",
    async execute() {
      const iso = new Date().toISOString();
      return { content: [{ type: "text", text: iso }], details: { iso } };
    }
  });
  return registry;
}

/** One Channels conversation per object, on the harness's root session. */
export class PiChannelsAgent extends DurableObject<Env> {
  readonly registry = createAppRegistry();
  readonly harness = new PiHarness({
    harness: ({ storage, context }) =>
      Harness.open(
        storage,
        {
          models: createModels({ providers: [workersAI(this.env.AI)] }),
          registry: this.registry,
          onReport: (error) => console.warn("pi report", error)
        },
        context
      ),
    defaults: {
      model: { provider: "cloudflare-workers-ai", modelId: MODEL_ID },
      thinkingLevel: "low"
    }
  });
  readonly bridge: HarnessChannels = new HarnessChannels({
    harness: this.harness,
    channels: (): Channels => this.channels,
    kv: this.ctx.storage.kv
  });
  readonly streams = new Streams();
  readonly websockets = new WebSockets();
  readonly channels: Channels = new Channels({
    streams: this.streams,
    channels: { web: new WebChannel({ websockets: this.websockets }) },
    snapshot: (conversationId) => this.bridge.snapshot(conversationId),
    onEvent: (event, origin) => this.bridge.onEvent(event, origin)
  });
  readonly lifecycle = Lifecycle.install(this)
    .use(this.harness)
    .use(this.streams)
    .use(this.channels)
    .use(this.websockets);

  async onStart(): Promise<void> {
    await this.bridge.attach();
  }

  receive(event: GatewayEvent, origin: EventOrigin) {
    return this.channels.receive(event, origin);
  }
}

export default {
  fetch: async (request: Request, env: Env) =>
    (await new ChannelGateway({
      channels: {},
      agent: (room) => env.PiChannelsAgent.getByName(room),
      // Demo only: the client names itself.
      web: (request) => {
        const url = new URL(request.url);
        const room = /^\/channels\/([^/]+)$/.exec(url.pathname)?.[1];
        const id = url.searchParams.get("as") ?? "anonymous";
        return room ? { route: room, participant: { id } } : undefined;
      }
    }).fetch(request)) ?? new Response("Not found", { status: 404 })
} satisfies ExportedHandler<Env>;
