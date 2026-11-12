#!/usr/bin/env node
// TresorBot agent host.
//
// A single resident process that multiplexes many agent sessions. Earth spawns
// it once and speaks line-delimited JSON-RPC 2.0 over stdin/stdout. One process
// per turn would cost tens of megabytes each and cannot reach a wide Worker
// fleet; one process per Earth keeps a 500-session fleet inside a few hundred
// megabytes.
//
// Usage:
//   node host/host.mjs              serve Earth on stdio
//   node host/host.mjs --check      validate adapters and exit
//   node host/host.mjs --list       print the discovered adapters as JSON

import { fileURLToPath } from "node:url";
import { dirname, join, resolve } from "node:path";
import { realpathSync } from "node:fs";

import { PROTOCOL_VERSION, ProtocolError, RecordWriter, readRecords } from "./protocol.mjs";
import { describeAdapters, describeError, discover, normalizeSettings, watchAdapters } from "./registry.mjs";

const HERE = dirname(fileURLToPath(import.meta.url));
const HOST_VERSION = "1.0.0";
const AGENTS_DIR = process.env.TRESORBOT_AGENTS_DIR || resolve(HERE, "..", "adapters");
const MAX_SESSIONS = Number(process.env.TRESORBOT_MAX_SESSIONS || 500);

/**
 * Owns the live sessions. Sessions are cheap: measured at roughly 0.4 MB each
 * for a pi session, so the ceiling exists to fail predictably rather than to
 * chase a small number.
 */
class SessionPool {
  constructor(limit) {
    this.limit = limit;
    this.sessions = new Map();
  }

  get size() {
    return this.sessions.size;
  }

  add(session) {
    if (this.sessions.size >= this.limit) {
      throw new ProtocolError(
        "overloaded",
        `host is at its ${this.limit}-session ceiling`,
      );
    }
    this.sessions.set(session.id, session);
  }

  get(id) {
    const session = this.sessions.get(id);
    if (!session) throw new ProtocolError("unknown_session", `no session ${id}`);
    return session;
  }

  delete(id) {
    const session = this.sessions.get(id);
    this.sessions.delete(id);
    return session;
  }

  /** All sessions, for shutdown and for reconciliation after a restart. */
  all() {
    return [...this.sessions.values()];
  }
}

class AgentHost {
  constructor({ agentsDir, writer, limit = MAX_SESSIONS }) {
    this.agentsDir = agentsDir;
    this.writer = writer;
    this.pool = new SessionPool(limit);
    this.adapters = new Map();
    this.problems = [];
    this.bustCounter = 0;
    this.shuttingDown = false;
  }

  async reload({ name } = {}) {
    this.bustCounter += 1;
    const { adapters, problems } = await discover(this.agentsDir, {
      previous: this.adapters,
      bust: this.bustCounter,
    });
    const next = new Map();
    for (const adapter of adapters) {
      if (name && adapter.name !== name) {
        const prior = this.adapters.get(adapter.name);
        if (prior) next.set(adapter.name, prior);
        continue;
      }
      next.set(adapter.name, adapter);
    }
    this.adapters = next;
    this.problems = problems;
    return describeAdapters([...this.adapters.values()], problems);
  }

  catalogue() {
    return describeAdapters([...this.adapters.values()], this.problems);
  }

  async handle(method, params, id) {
    switch (method) {
      case "host/hello": {
        if (params?.protocol !== PROTOCOL_VERSION) {
          this.writer.fail(
            id,
            -32600,
            `host speaks protocol ${PROTOCOL_VERSION}`,
            "protocol_mismatch",
          );
          return;
        }
        this.writer.result(id, {
          protocol: PROTOCOL_VERSION,
          host_version: HOST_VERSION,
          adapters: this.catalogue(),
          max_sessions: this.pool.limit,
        });
        return;
      }
      case "host/reload": {
        const adapters = await this.reload({ name: params?.name });
        this.writer.result(id, { adapters });
        this.writer.notify("host/adapters_changed", { adapters });
        return;
      }
      case "session/open": {
        await this.open(params, id);
        return;
      }
      case "session/prompt": {
        const session = this.pool.get(params?.session_id);
        await session.prompt(params?.text ?? "");
        this.writer.result(id, { accepted: true });
        // The turn ends on its own schedule. Waiting here would block the
        // protocol, so completion is reported as a notification.
        session
          .wait()
          .then((settled) => {
            if (settled) this.settle(params.session_id, settled);
          })
          .catch((cause) => {
            this.settle(params.session_id, {
              stop_reason: "error",
              error: describeError(cause),
            });
          });
        return;
      }
      case "session/cancel": {
        const session = this.pool.get(params?.session_id);
        if (session.settled) {
          // Nothing is running, so this is not a failure: Earth may cancel a turn
          // that finished while the request was in flight.
          this.writer.result(id, { cancelled: false });
          return;
        }
        const cancelled = await session.cancel(params?.reason ?? "");
        this.writer.result(id, { cancelled });
        return;
      }
      case "session/close": {
        const session = this.pool.delete(params?.session_id);
        if (session) await session.close();
        this.writer.result(id, {});
        return;
      }
      case "host/shutdown": {
        this.writer.result(id, {});
        await this.shutdown();
        return;
      }
      default:
        this.writer.fail(id, -32601, `unknown method ${method}`, "unknown_method");
    }
  }

  async open(params, id) {
    const adapter = this.adapters.get(params?.adapter);
    if (!adapter) {
      this.writer.fail(
        id,
        -32602,
        `adapter ${JSON.stringify(params?.adapter)} is not loaded`,
        "unknown_adapter",
      );
      return;
    }
    if (adapter.stale) {
      this.writer.fail(
        id,
        -32603,
        `adapter ${adapter.name} is unhealthy: ${adapter.error}`,
        "unknown_adapter",
      );
      return;
    }
    // Reserve the slot before awaiting, so N concurrent opens cannot overshoot.
    if (this.pool.size >= this.pool.limit) {
      this.writer.fail(
        id,
        -32000,
        `host is at its ${this.pool.limit}-session ceiling`,
        "overloaded",
      );
      return;
    }
    const sessionId = params.session_id;
    // Validate here, once, so every adapter gets the same guarantees and Earth
    // learns about a bad key before an agent starts.
    let settings;
    try {
      // Settings live beside the adapter, not in Earth's config, so an adapter
      // carries its own configuration with it when installed or replaced.
      settings = normalizeSettings(adapter.settings, {
        ...(adapter.config ?? {}),
        ...(params.settings ?? {}),
      });
    } catch (cause) {
      this.writer.fail(id, -32602, describeError(cause), cause.reason ?? "bad_request");
      return;
    }
    let session;
    try {
      session = await adapter.adapter.createSession({
        ...params,
        settings,
        adapter: adapter.name,
        manifest: adapterModuleManifest(adapter),
        emit: (event) => this.writer.notify("session/event", { session_id: sessionId, event }),
        log: (level, message) =>
          this.writer.notify("session/log", { session_id: sessionId, level, message }),
      });
    } catch (cause) {
      this.writer.fail(id, -32603, describeError(cause), "open_failed");
      return;
    }
    try {
      this.pool.add({ id: sessionId, ...session, adapterName: adapter.name });
    } catch (cause) {
      await session.close?.();
      this.writer.fail(id, -32000, describeError(cause), "overloaded");
      return;
    }
    this.writer.result(id, {
      session_id: sessionId,
      session_ref: session.sessionRef ?? "",
      diagnostics: session.diagnostics ?? [],
    });
    // The session is tracked until its turn settles, so a cancel can always
    // find it and a crash can report every affected assignment.
  }

  settle(sessionId, settled) {
    const session = this.pool.sessions.get(sessionId);
    // Mark rather than delete: Earth closes a session right after it settles, and
    // a late cancel or close must not fail merely because the turn finished first.
    if (session) session.settled = true;
    this.writer.notify("session/settled", { session_id: sessionId, ...settled });
  }

  async shutdown() {
    if (this.shuttingDown) return;
    this.shuttingDown = true;
    await Promise.allSettled(this.pool.all().map((session) => session.close?.()));
    this.pool.sessions.clear();
    this.writer.close();
    // Give the loop one turn to flush, then exit non-zero so a lingering handle
    // cannot keep Earth waiting.
    setImmediate(() => process.exit(0));
  }
}

function adapterModuleManifest(adapter) {
  return {
    name: adapter.name,
    version: adapter.version,
    capabilities: adapter.capabilities,
    settings: adapter.settings,
  };
}

/** Runs the stdio server. */
export async function serve({ agentsDir = AGENTS_DIR, input = process.stdin, output = process.stdout } = {}) {
  const writer = new RecordWriter(output);
  const host = new AgentHost({ agentsDir, writer });
  await host.reload();

  const watcher = watchAdapters(agentsDir, async () => {
    const adapters = await host.reload();
    writer.notify("host/adapters_changed", { adapters });
  });

  const close = async (code = 0) => {
    watcher.stop();
    await host.shutdown();
    process.exit(code);
  };
  process.on("SIGTERM", () => close(0));
  process.on("SIGINT", () => close(0));

  try {
    await readRecords(input, (record) => {
      const method = record.m ?? record.method;
      const params = record.p ?? record.params ?? {};
      const id = record.id;
      if (!method) return;
      // Dispatch without awaiting: a long prompt must not block a cancel.
      host.handle(method, params, id).catch((cause) => {
        if (id !== undefined) {
          writer.fail(id, -32603, describeError(cause), cause.reason ?? "internal_error");
        }
      });
    });
  } catch (cause) {
    writer.notify("session/log", { level: "error", message: describeError(cause) });
  }
  watcher.stop();
  await host.shutdown();
}

async function main() {
  const args = process.argv.slice(2);
  if (args.includes("--list") || args.includes("--check")) {
    const { adapters, problems } = await discover(AGENTS_DIR);
    const report = describeAdapters(adapters, problems);
    console.log(JSON.stringify({ version: HOST_VERSION, protocol: PROTOCOL_VERSION, adapters: report }, null, 2));
    process.exit(problems.length > 0 || report.some((a) => !a.healthy) ? 1 : 0);
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
    process.stderr.write(`agent host failed: ${describeError(cause)}\n`);
    process.exit(1);
  });
}
