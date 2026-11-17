#!/usr/bin/env node
// TresorBot environment host.
//
// A single resident process that supervises every configured environment instance.
// Sun spawns it once and speaks line-delimited JSON-RPC 2.0 over stdin/stdout,
// the same framing the agent host uses.
//
// Why one process rather than one per instance: environments are counted in single
// digits, hold long-lived network connections and mostly idle, so a process each
// would trade tens of megabytes of RSS for isolation that per-adapter error
// containment already provides at the module boundary. The adapter API is
// deliberately blind to this choice, so it can be revisited without touching an
// extension.
//
// The host is part of Sun's runtime, not part of the catalogue: it is embedded in
// the Go binary so a built Sun works with no repository to clone and the host can
// never drift out of version with the protocol it speaks.
//
// Both directions carry requests here, which is the one structural difference
// from the agent host. Sun asks the host to start, stop and deliver; the host asks
// Sun to admit an event and to persist a cursor. Numeric ids belong to Sun,
// string ids to the host, so a reply is never mistaken for a request.
//
// Usage:
//   node environment-host.mjs              serve Sun on stdio
//   node environment-host.mjs --check      validate the catalogue and exit
//   node environment-host.mjs --list       print the catalogue as JSON

import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";
import { realpathSync } from "node:fs";

import { PROTOCOL_VERSION, ProtocolError, RecordWriter, readRecords } from "./protocol.mjs";
import { catalogueStamp, describeError, describeEnvironments, discover, loadEnvironment, watchEnvironments } from "./registry.mjs";

const HERE = dirname(fileURLToPath(import.meta.url));
const HOST_VERSION = "1.0.0";
const ENVIRONMENTS_DIR = process.env.TRESORBOT_ENVIRONMENTS_DIR || resolve(HERE, "..", "environments");
const MAX_INSTANCES = Number(process.env.TRESORBOT_MAX_INSTANCES || 200);
// A tool call holds a model turn open, so it is bounded more tightly than a
// shutdown. Sun applies its own ceiling; this one exists so the adapter is told
// it timed out rather than leaving the whole host unresponsive.
const TOOL_CALL_TIMEOUT_MS = 30000;

/**
 * The host's side of the JSON-RPC channel.
 *
 * One reader, one writer, and a promise per outstanding request. Ids are
 * prefixed so they can never collide with Sun's numeric ones.
 */
class Peer {
  constructor(writer) {
    this.writer = writer;
    this.pending = new Map();
    this.serial = 0;
  }

  request(method, params) {
    const id = `host:${++this.serial}`;
    this.writer.write({ id, method, params });
    return new Promise((resolvePromise, reject) => {
      this.pending.set(id, { resolve: resolvePromise, reject });
    });
  }

  /** Handles a reply from Sun to one of this host's requests. */
  settle(id, message) {
    const entry = this.pending.get(String(id));
    if (!entry) return false;
    this.pending.delete(String(id));
    if (message.error) {
      entry.reject(new ProtocolError(message.error.data ?? "request_failed", message.error.message ?? "request failed"));
    } else {
      entry.resolve(message.result ?? {});
    }
    return true;
  }

  rejectAll(reason) {
    for (const [, entry] of this.pending) entry.reject(new ProtocolError("host_shutdown", reason));
    this.pending.clear();
  }
}

/**
 * One running instance of one environment adapter.
 *
 * The instance owns the upstream connection; the host owns its lifecycle and
 * nothing else. Every call into the adapter is wrapped, because an adapter that
 * throws must not take down the other environments sharing this process.
 */
class RunningInstance {
  constructor({ name, environment, peer, settings, revision }) {
    this.name = name;
    this.environment = environment;
    this.peer = peer;
    this.settings = settings ?? {};
    this.revision = revision ?? 0;
    this.healthy = true;
    this.stopped = false;
    this.adapter = null;
  }

  /** Builds the context object an adapter is given. */
  context() {
    return {
      instance: this.name,
      environment: this.environment.name,
      settings: this.settings,
      log: (message, level = "info") => {
        this.peer.writer.notify("environment/log", {
          instance: this.name,
          level,
          message: String(message).slice(0, 4000),
        });
      },
      // Resolves only after Sun has durably admitted or rejected the event, which
      // is what lets an adapter advance its cursor without risking a lost event.
      emit: async (event) => {
        if (this.stopped) {
          throw new ProtocolError("instance_stopped", `instance ${this.name} stopped`);
        }
        return this.peer.request("environment/event", { instance: this.name, event });
      },
      // Sun stores cursors; the adapter owns their meaning.
      cursor: {
        get: async (key) => {
          const result = await this.peer.request("environment/cursor_get", { instance: this.name, key: String(key) });
          return result?.value ?? "";
        },
        set: async (key, value) => {
          await this.peer.request("environment/cursor_set", { instance: this.name, key: String(key), value: String(value) });
        },
      },
      // Delivery goes through the host, which owns retries; an adapter never
      // schedules its own.
      deliver: async (params) => this.adapter?.deliver?.(params) ?? { delivered: true },
    };
  }

  async start() {
    this.adapter = await this.environment.createInstance(this.context());
    await this.adapter.start(this.settings);
  }

  async reconfigure(settings, revision) {
    const previous = this.settings;
    this.settings = settings ?? {};
    this.revision = revision ?? this.revision;
    if (!this.adapter) return;
    if (typeof this.adapter.reconfigure === "function") {
      await this.adapter.reconfigure(this.settings);
      return;
    }
    // An adapter without reconfigure is restarted rather than silently keeping
    // stale settings, which would look as though the edit did nothing.
    this.log("settings changed; restarting because this environment has no reconfigure", "info", previous);
    await this.stop();
    this.stopped = false;
    await this.start();
  }

  log(message, level = "info") {
    this.peer.writer.notify("environment/log", { instance: this.name, level, message: String(message).slice(0, 4000) });
  }

  async stop() {
    this.stopped = true;
    if (this.adapter && typeof this.adapter.stop === "function") {
      // A stop that never settles would wedge shutdown, so it is bounded here
      // rather than trusted to the adapter.
      await withTimeout(Promise.resolve(this.adapter.stop()), 15000, `instance ${this.name} stop timed out`);
    }
    this.adapter = null;
  }
}

/** Races a promise against a timeout so one hung adapter cannot stall the host. */
function withTimeout(promise, ms, message) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new ProtocolError("timeout", message)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

/**
 * The host's method dispatch. Every adapter call is isolated: a throwing environment
 * becomes unhealthy, while its siblings keep running.
 */
class InputHost {
  constructor({ environmentsDir, peer, limit = MAX_INSTANCES, onExit = null }) {
    this.environmentsDir = environmentsDir;
    this.peer = peer;
    this.limit = limit;
    // Called after a graceful shutdown has finished. The shim supplies a real exit;
    // an embedding test supplies nothing, so importing this module stays inert.
    this.onExit = onExit;
    this.environments = new Map();
    this.problems = {};
    this.instances = new Map();
    // Set when Sun asks the host to stop. Making every later request fail is what
    // keeps an adapter that is mid-handshake from being answered by a host whose
    // owner is waiting for the process to exit.
    this.closing = false;
  }

  get writer() {
    return this.peer.writer;
  }

  catalogue() {
    return describeEnvironments([...this.environments.values()], this.problems);
  }

  async reload({ name } = {}) {
    const { environments, problems } = await discover(this.environmentsDir);
    if (name) {
      if (problems[name]) {
        this.problems[name] = problems[name];
      } else {
        delete this.problems[name];
        const found = environments.find((environment) => environment.name === name);
        if (found) this.environments.set(name, found);
      }
      return this.catalogue();
    }
    this.environments = new Map(environments.map((environment) => [environment.name, environment]));
    this.problems = problems;
    return this.catalogue();
  }

  async handle(method, params, id) {
    switch (method) {
      case "environment/hello": {
        if (params?.protocol !== PROTOCOL_VERSION) {
          this.writer.fail(id, -32600, `host speaks protocol ${PROTOCOL_VERSION}`, "protocol_mismatch");
          return;
        }
        await this.reload();
        this.writer.result(id, {
          protocol: PROTOCOL_VERSION,
          host_version: HOST_VERSION,
          environments: this.catalogue(),
          max_instances: this.limit,
        });
        return;
      }
      case "environment/reload": {
        const environments = await this.reload({ name: params?.name });
        this.writer.result(id, { environments });
        this.writer.notify("environment/environments_changed", { environments });
        return;
      }
      case "environment/config": {
        await this.configure(params, id);
        return;
      }
      case "environment/start": {
        await this.start(params?.instance, id);
        return;
      }
      case "environment/stop": {
        await this.stopInstance(params?.instance);
        this.writer.result(id, {});
        return;
      }
      case "environment/tools_list": {
        await this.toolsList(params, id);
        return;
      }
      case "environment/tools_call": {
        await this.toolsCall(params, id);
        return;
      }
      case "environment/webhook": {
        await this.webhook(params, id);
        return;
      }
      case "environment/deliver": {
        await this.deliver(params, id);
        return;
      }
      case "environment/shutdown": {
        // Answer first: Sun is waiting on this reply, and stopping every instance
        // can take as long as the slowest adapter's stop(). Exiting without ending
        // the process here would leave Sun to kill a host that had already agreed
        // to stop, which is indistinguishable from a wedged adapter.
        this.closing = true;
        this.writer.result(id, {});
        await this.shutdown();
        this.onExit?.();
        return;
      }
      default:
        this.writer.fail(id, -32601, `unknown method ${method}`, "unknown_method");
    }
  }

  /**
   * Configures one instance: confirms its adapter is loaded, creates the running
   * instance and applies settings. Starting is a separate call so Sun can create
   * every instance before any connects upstream.
   */
  async configure(params, id) {
    const name = params?.instance;
    if (!name) {
      this.writer.fail(id, -32602, "instance is required", "bad_request");
      return;
    }
    const environment = this.environments.get(params?.environment);
    if (!environment) {
      this.writer.fail(id, -32602, `environment ${JSON.stringify(params?.environment)} is not loaded`, "unknown_environment");
      return;
    }
    const existing = this.instances.get(name);
    if (existing) {
      await existing.reconfigure(params?.settings ?? {}, params?.revision ?? 0);
      this.writer.result(id, { configured: true, reconfigured: true });
      return;
    }
    if (this.instances.size >= this.limit) {
      this.writer.fail(id, -32000, `host is at its ${this.limit}-instance ceiling`, "overloaded");
      return;
    }
    this.instances.set(
      name,
      new RunningInstance({
        name,
        environment,
        peer: this.peer,
        settings: params?.settings ?? {},
        revision: params?.revision ?? 0,
      }),
    );
    this.writer.result(id, { configured: true });
  }

  async start(name, id) {
    const instance = this.instances.get(name);
    if (!instance) {
      this.writer.fail(id, -32602, `instance ${JSON.stringify(name)} is not configured`, "unknown_instance");
      return;
    }
    try {
      await instance.start();
      instance.healthy = true;
      this.writer.result(id, { started: true });
      this.writer.notify("environment/health", { instance: name, healthy: true });
    } catch (cause) {
      // A failed start is per-instance: the process and its siblings stay up.
      instance.healthy = false;
      const message = describeError(cause);
      this.writer.fail(id, -32603, message, "start_failed");
      this.writer.notify("environment/health", { instance: name, healthy: false, message });
    }
  }

  async stopInstance(name) {
    const instance = this.instances.get(name);
    if (!instance) return;
    this.instances.delete(name);
    try {
      await instance.stop();
    } catch (cause) {
      this.writer.notify("environment/log", { instance: name, level: "error", message: describeError(cause) });
    }
  }

  /**
   * Reports which tools one instance exposes.
   *
   * An adapter that does not implement listTools simply has none: that is not an
   * error, because declaring the `tools` capability is Sun's business, and an
   * adapter that later stops offering tools should degrade rather than fail.
   */
  async toolsList(params, id) {
    const instance = this.instances.get(params?.instance);
    if (!instance) {
      this.writer.fail(id, -32602, `instance ${JSON.stringify(params?.instance)} is not configured`, "unknown_instance");
      return;
    }
    if (!instance.environment.capabilities.includes("tools")) {
      this.writer.fail(id, -32603, `environment ${instance.environment.name} does not declare the tools capability`, "not_capable");
      return;
    }
    try {
      const listed = (await instance.adapter?.listTools?.()) ?? [];
      // Normalised here so Sun receives one shape even from a sloppy adapter: a
      // tool without a name is dropped rather than becoming an unnamed entry the
      // model could never call.
      const tools = [];
      for (const tool of Array.isArray(listed) ? listed : []) {
        if (!tool || typeof tool.name !== "string" || !tool.name) continue;
        tools.push({
          name: tool.name,
          description: String(tool.description ?? ""),
          parameters: tool.parameters && typeof tool.parameters === "object" ? tool.parameters : { type: "object", properties: {} },
        });
      }
      this.writer.result(id, { tools });
    } catch (cause) {
      this.writer.fail(id, -32603, describeError(cause), cause.reason ?? "tools_list_failed");
    }
  }

  /**
   * Runs one tool on one instance.
   *
   * The call is bounded here as well as in Sun: an adapter that hangs must fail
   * its own call with a reason, not hold a model turn open until the turn itself
   * times out, which would look to the model like the environment vanished.
   */
  async toolsCall(params, id) {
    const instance = this.instances.get(params?.instance);
    if (!instance) {
      this.writer.fail(id, -32602, `instance ${JSON.stringify(params?.instance)} is not configured`, "unknown_instance");
      return;
    }
    if (!instance.environment.capabilities.includes("tools")) {
      this.writer.fail(id, -32603, `environment ${instance.environment.name} does not declare the tools capability`, "not_capable");
      return;
    }
    const name = params?.name;
    if (typeof name !== "string" || !name) {
      this.writer.fail(id, -32602, "name is required", "bad_request");
      return;
    }
    if (typeof instance.adapter?.callTool !== "function") {
      this.writer.fail(id, -32603, `environment ${instance.environment.name} does not implement callTool`, "not_capable");
      return;
    }
    try {
      const result = await withTimeout(
        instance.adapter.callTool({
          name,
          arguments: params?.arguments ?? {},
          callId: String(params?.call_id ?? ""),
        }),
        TOOL_CALL_TIMEOUT_MS,
        `${name} did not answer within ${TOOL_CALL_TIMEOUT_MS}ms`,
      );
      // JSON-encoded rather than passed through: the Go side bounds size on a byte
      // count, and an adapter returning a value the record encoder cannot represent
      // must fail here rather than desynchronise the stream.
      this.writer.result(id, { result: result === undefined ? null : result });
    } catch (cause) {
      this.writer.fail(id, -32603, describeError(cause), cause.reason ?? "tool_failed");
    }
  }

  async webhook(params, id) {
    const instance = this.instances.get(params?.instance);
    if (!instance) {
      this.writer.fail(id, -32602, `instance ${JSON.stringify(params?.instance)} is not configured`, "unknown_instance");
      return;
    }
    try {
      const result = await instance.adapter?.handleWebhook?.({
        method: params?.method,
        path: params?.path,
        query: params?.query ?? "",
        headers: params?.headers ?? {},
        // Raw bytes: an upstream signature is computed over them, not over a
        // decoded string.
        body: Buffer.from(params?.body_base64 ?? "", "base64"),
      });
      if (!result) {
        this.writer.fail(id, -32603, `environment ${instance.environment.name} does not handle webhooks`, "not_capable");
        return;
      }
      this.writer.result(id, result);
    } catch (cause) {
      this.writer.fail(id, -32603, describeError(cause), cause.reason ?? "webhook_failed");
    }
  }

  /** Sun asks the host to deliver one result to the adapter that owns it. */
  async deliver(params, id) {
    const instance = this.instances.get(params?.instance);
    if (!instance) {
      this.writer.result(id, { delivered: false, retryable: false, code: "unknown_instance" });
      return;
    }
    if (typeof instance.adapter?.deliver !== "function") {
      // Nothing to deliver to. Reporting success would silently drop the reply;
      // reporting it as permanent lets Sun record the fact.
      this.writer.result(id, { delivered: false, retryable: false, code: "no_deliver" });
      return;
    }
    try {
      const result = await instance.adapter.deliver(params);
      this.writer.result(id, result ?? { delivered: true });
    } catch (cause) {
      this.writer.result(id, { delivered: false, retryable: true, code: cause.reason ?? "delivery_failed" });
    }
  }

  async shutdown() {
    for (const name of [...this.instances.keys()]) {
      await this.stopInstance(name);
    }
    this.peer.rejectAll("host is shutting down");
  }
}

/** Runs the stdio server. */
export async function serve({ environmentsDir = ENVIRONMENTS_DIR, input = process.stdin, output = process.stdout } = {}) {
  const writer = new RecordWriter(output);
  const peer = new Peer(writer);
  const host = new InputHost({
    environmentsDir,
    peer,
    onExit: () => {
      watcher.stop();
      output.end?.();
      process.exit(0);
    },
  });

  // The baseline is the catalogue as hello loaded it, so an install that lands
  // between startup and the first poll is reported rather than absorbed.
  const baseline = await catalogueStamp(environmentsDir).catch(() => null);
  const watcher = watchEnvironments(
    environmentsDir,
    async () => {
      if (host.closing) return;
      const environments = await host.reload();
      writer.notify("environment/environments_changed", { environments });
    },
    { baseline },
  );

  const close = async (code = 0) => {
    host.closing = true;
    watcher.stop();
    await host.shutdown();
    // Both streams are ended explicitly: a piped stdout that is never ended can
    // outlive the work by a libuv handle, and an exit that waits on it looks
    // exactly like a wedged adapter.
    output.end?.();
    process.exit(code);
  };
  const shutdown = () => close(0);
  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);

  try {
    await readRecords(input, (record) => {
      // A reply to one of this host's own requests.
      if (record.method === undefined && record.m === undefined) {
        peer.settle(record.id, record);
        return;
      }
      const method = record.m ?? record.method;
      const params = record.p ?? record.params ?? {};
      const id = record.id;
      if (!method) return;
      host.handle(method, params, id).catch((cause) => {
        if (id !== undefined) {
          writer.fail(id, -32603, describeError(cause), cause.reason ?? "internal_error");
        }
      });
    });
  } catch (cause) {
    writer.notify("environment/log", { level: "error", message: describeError(cause) });
  }
  watcher.stop();
  await host.shutdown();
  // The input stream ended: nobody is left to serve, so the process must exit.
  // Leaving the event loop alive here would hold Sun's pipe open after Sun has
  // decided the conversation is over.
  process.exit(0);
}

async function main() {
  const args = process.argv.slice(2);
  if (args.includes("--list") || args.includes("--check")) {
    const { environments, problems } = await discover(ENVIRONMENTS_DIR);
    const report = describeEnvironments(environments, problems);
    console.log(JSON.stringify({ version: HOST_VERSION, protocol: PROTOCOL_VERSION, environments: report }, null, 2));
    process.exit(Object.keys(problems).length > 0 || report.some((a) => !a.healthy) ? 1 : 0);
  }
  if (args.includes("--version")) {
    console.log(HOST_VERSION);
    return;
  }
  await serve();
}

// Only run when invoked directly, so tests can import serve().
if (process.argv[1] && realpathSync(process.argv[1]) === realpathSync(fileURLToPath(import.meta.url))) {
  main().catch((cause) => {
    process.stderr.write(`environment host failed: ${describeError(cause)}\n`);
    process.exit(1);
  });
}

export { InputHost, Peer, RunningInstance, loadEnvironment, withTimeout };
