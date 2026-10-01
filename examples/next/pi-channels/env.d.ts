/* eslint-disable */
interface __BaseEnv_Env {
  AI: Ai;
  PiChannelsAgent: DurableObjectNamespace<
    import("./src/server").PiChannelsAgent
  >;
}
declare namespace Cloudflare {
  interface GlobalProps {
    mainModule: typeof import("./src/server");
    durableNamespaces: "PiChannelsAgent";
  }
  interface Env extends __BaseEnv_Env {}
}
interface Env extends __BaseEnv_Env {}
