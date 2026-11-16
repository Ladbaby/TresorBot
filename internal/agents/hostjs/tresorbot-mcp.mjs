#!/usr/bin/env node
// Bridges an MCP stdio client (Codex, Claude Code) to Earth's loopback tool
// gateway, which speaks a plain JSON HTTP API rather than MCP.
//
// Why this exists: Earth's ToolGateway exposes only `POST /definitions` and
// `POST /execute` (internal/earthexec/toolgateway.go). It is not an MCP server,
// so pointing an agent's MCP client straight at it can only fail. Rather than
// teach Earth a second protocol, this process adapts one to the other: the agent
// speaks MCP over stdio to us, and we speak HTTP to Earth.
//
// Configured entirely by environment, because that is the only channel the host
// passes through reliably:
//
//   TRESORBOT_TOOL_URL    gateway base, e.g. http://127.0.0.1:PORT/a/<id>/<epoch>
//   TRESORBOT_TOOL_TOKEN  per-assignment bearer token
//
// The runner credential never appears here: Earth performs the upstream call.

import { createHash } from "node:crypto";
import { createInterface } from "node:readline";

const TOOL_URL = process.env.TRESORBOT_TOOL_URL ?? "";
const TOOL_TOKEN = process.env.TRESORBOT_TOOL_TOKEN ?? "";

/** JSON-RPC error codes, per the MCP spec. */
const METHOD_NOT_FOUND = -32601;
const INTERNAL_ERROR = -32603;

function send(message) {
  // MCP over stdio is newline-delimited JSON.
  process.stdout.write(`${JSON.stringify(message)}\n`);
}

async function callGateway(path, body) {
  const response = await fetch(TOOL_URL + path, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${TOOL_TOKEN}`,
    },
    body: JSON.stringify(body),
  });
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload?.error?.code || `gateway responded ${response.status}`);
  }
  return payload;
}

/**
 * Renders a Sun tool payload as text.
 *
 * Sun's replies are already structured; the agent only needs something it can
 * read, so the same convention as the pi and claude-code adapters is used.
 */
function resultText(result) {
  if (result == null) return "";
  if (typeof result === "string") return result;
  if (typeof result === "object") {
    if (typeof result.text === "string" && result.text) return result.text;
    if (result.error && typeof result.error === "object") {
      const code = result.error.code || "";
      const message = result.error.message || code;
      return code && message && code !== message ? `${code}: ${message}` : message;
    }
    try {
      return JSON.stringify(result, null, 2);
    } catch {
      return String(result);
    }
  }
  return String(result);
}

let toolsCache = null;

async function listTools() {
  if (toolsCache) return toolsCache;
  const definitions = await callGateway("/definitions", {});
  toolsCache = (definitions.tools ?? []).map((tool) => ({
    name: tool.name,
    description: tool.description ?? "",
    inputSchema: tool.parameters ?? { type: "object", properties: {} },
  }));
  return toolsCache;
}

async function callTool(name, args) {
  // The gateway deduplicates by operation id, so it has to be stable for a given
  // call. A per-invocation id is enough: MCP does not retry a call on its own.
  const operationId = createHash("sha256")
    .update(`${name}\u0000${JSON.stringify(args ?? {})}\u0000${Date.now()}\u0000${Math.random()}`)
    .digest("hex");
  const result = await callGateway("/execute", {
    operation_id: operationId,
    tool: name,
    arguments: args ?? {},
  });
  return { content: [{ type: "text", text: resultText(result) }], isError: Boolean(result?.error) };
}

async function handle(message) {
  const { id, method, params } = message ?? {};

  // Notifications carry no id and expect no reply.
  const isNotification = id === undefined || id === null;

  try {
    switch (method) {
      case "initialize":
        return send({
          jsonrpc: "2.0",
          id,
          result: {
            protocolVersion: params?.protocolVersion ?? "2025-06-18",
            capabilities: { tools: {} },
            serverInfo: { name: "tresorbot", version: "1.0.0" },
          },
        });
      case "notifications/initialized":
      case "initialized":
        return undefined;
      case "ping":
        if (!isNotification) send({ jsonrpc: "2.0", id, result: {} });
        return undefined;
      case "tools/list":
        // An unreachable gateway must not fail the whole turn: Codex aborts when
        // tools/list errors, which turns "Sun is briefly down" into "the bot
        // cannot answer at all". Reporting no tools degrades instead of breaking,
        // and the operator sees the reason on stderr.
        try {
          return send({ jsonrpc: "2.0", id, result: { tools: await listTools() } });
        } catch (cause) {
          process.stderr.write(`tresorbot-mcp: tools/list failed: ${cause?.message ?? cause}\n`);
          return send({ jsonrpc: "2.0", id, result: { tools: [] } });
        }
      case "tools/call": {
        const name = params?.name ?? "";
        const outcome = await callTool(name, params?.arguments);
        return send({ jsonrpc: "2.0", id, result: outcome });
      }
      default:
        // Unknown methods must be answered, or the client waits forever.
        if (isNotification) return undefined;
        return send({
          jsonrpc: "2.0",
          id,
          error: { code: METHOD_NOT_FOUND, message: `unknown method: ${method}` },
        });
    }
  } catch (cause) {
    if (isNotification) return undefined;
    return send({
      jsonrpc: "2.0",
      id,
      error: { code: INTERNAL_ERROR, message: String(cause?.message ?? cause) },
    });
  }
}

if (!TOOL_URL || !TOOL_TOKEN) {
  process.stderr.write("tresorbot-mcp: TRESORBOT_TOOL_URL and TRESORBOT_TOOL_TOKEN are required\n");
  process.exit(1);
}

const lines = createInterface({ input: process.stdin, crlfDelay: Infinity });
// Serialised so a slow tools/call cannot interleave replies out of order.
let queue = Promise.resolve();
lines.on("line", (line) => {
  const text = line.trim();
  if (!text) return;
  let message;
  try {
    message = JSON.parse(text);
  } catch {
    return; // A malformed line is not worth killing the session over.
  }
  queue = queue.then(() => handle(message)).catch(() => {});
});
lines.on("close", () => {
  queue.finally(() => process.exit(0));
});
