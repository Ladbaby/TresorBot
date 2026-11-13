// Adapter discovery and hot reload.
//
// An adapter is a directory with agent.json and an entry module. Reloading means
// importing the module again under a cache-busting URL and swapping the registry
// entry. Sessions that already started keep the module object they opened with,
// so a reload never changes the behaviour of a turn in flight.

import { existsSync } from "node:fs";
import { readFile, readdir, stat } from "node:fs/promises";
import { createHash } from "node:crypto";
import { pathToFileURL } from "node:url";
import { join, resolve, sep } from "node:path";

import { PROTOCOL_VERSION, ProtocolError } from "./protocol.mjs";

const MANIFEST = "agent.json";

export function validAdapterName(name) {
  return typeof name === "string" && /^[a-z0-9][a-z0-9_-]{0,63}$/.test(name);
}

/** Loads one adapter directory, returning its record or throwing a clear error. */
export async function loadAdapter(dir, { bust = 0 } = {}) {
  const manifestPath = join(dir, MANIFEST);
  const raw = await readFile(manifestPath, "utf8");
  let manifest;
  try {
    manifest = JSON.parse(raw);
  } catch (cause) {
    throw new ProtocolError("invalid_manifest", `${MANIFEST} is not valid JSON: ${cause.message}`);
  }
  if (!validAdapterName(manifest.name)) {
    throw new ProtocolError("invalid_manifest", `invalid adapter name ${JSON.stringify(manifest.name)}`);
  }
  if (manifest.protocol !== PROTOCOL_VERSION) {
    throw new ProtocolError(
      "protocol_mismatch",
      `adapter ${manifest.name} targets protocol ${manifest.protocol}, host speaks ${PROTOCOL_VERSION}`,
    );
  }
  if (typeof manifest.version !== "string" || manifest.version.length === 0) {
    throw new ProtocolError("invalid_manifest", `adapter ${manifest.name} has no version`);
  }
  if (typeof manifest.entry !== "string" || manifest.entry.length === 0) {
    throw new ProtocolError("invalid_manifest", `adapter ${manifest.name} has no entry`);
  }
  const entryPath = resolve(dir, manifest.entry);
  // An entry must stay inside its own directory; an adapter is trusted code but
  // it must still be the code the operator put there.
  if (!entryPath.startsWith(resolve(dir) + sep) && entryPath !== resolve(dir)) {
    throw new ProtocolError("invalid_manifest", `adapter ${manifest.name} entry escapes its directory`);
  }
  if (!existsSync(entryPath)) {
    throw new ProtocolError("missing_entry", `adapter ${manifest.name} entry ${manifest.entry} is missing`);
  }

  // config.json holds this installation's settings for the adapter, so Earth's
  // config file carries no adapter-specific keys.
  let config = {};
  try {
    const text = await readFile(join(dir, "config.json"), "utf8");
    const parsed = JSON.parse(text);
    if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new Error("config.json must be a JSON object");
    }
    config = parsed;
  } catch (cause) {
    if (cause.code !== "ENOENT") {
      throw new ProtocolError("invalid_manifest", `config.json: ${cause.message}`);
    }
  }

  const entrySource = await readFile(entryPath, "utf8");
  const digest = createHash("sha256").update(raw).update("\0").update(entrySource).digest("hex");

  const url = pathToFileURL(entryPath).href + (bust ? `?v=${bust}` : "");
  const module = await import(url);
  const factory = module.default ?? module.adapter;
  if (typeof factory !== "function") {
    throw new ProtocolError(
      "invalid_adapter",
      `adapter ${manifest.name} must export a default factory function`,
    );
  }
  const info = await factory({ manifest, mode: "describe" });
  if (!info || typeof info.createSession !== "function") {
    throw new ProtocolError(
      "invalid_adapter",
      `adapter ${manifest.name} factory must return an object with createSession()`,
    );
  }
  return {
    config,
    name: manifest.name,
    version: manifest.version,
    displayName: manifest.display_name ?? manifest.name,
    description: manifest.description ?? "",
    capabilities: manifest.capabilities ?? [],
    settings: manifest.settings ?? [],
    // Optional per-adapter session ceiling. Absent means "use the host ceiling",
    // so an adapter that declares nothing behaves exactly as it did before.
    limits: normalizeLimits(manifest.limits),
    digest,
    dir: resolve(dir),
    adapter: info,
  };
}

/** Validates the optional `limits` block; an unusable value is ignored. */
function normalizeLimits(limits) {
  if (!limits || typeof limits !== "object") return undefined;
  const maxSessions = limits.max_sessions;
  if (typeof maxSessions !== "number" || !Number.isInteger(maxSessions) || maxSessions <= 0) {
    return undefined;
  }
  return { max_sessions: maxSessions };
}

/**
 * Scans the adapter directory. A broken adapter is reported, not fatal: one bad
 * directory must never stop Earth or the other adapters.
 */
export async function discover(root, { previous = new Map(), bust = 0 } = {}) {
  const adapters = [];
  const problems = [];
  let entries;
  try {
    entries = await readdir(root, { withFileTypes: true });
  } catch (cause) {
    if (cause.code === "ENOENT") return { adapters, problems };
    throw cause;
  }
  for (const entry of entries) {
    if (!entry.isDirectory() || entry.name.startsWith(".")) continue;
    const dir = join(root, entry.name);
    try {
      const loaded = await loadAdapter(dir, { bust });
      adapters.push(loaded);
    } catch (cause) {
      problems.push({ name: entry.name, error: describeError(cause) });
      // Keep the last good version so a mid-edit save does not take a working
      // adapter offline.
      const prior = previous.get(entry.name);
      if (prior) adapters.push({ ...prior, stale: true, error: describeError(cause) });
    }
  }
  adapters.sort((a, b) => a.name.localeCompare(b.name));
  return { adapters, problems };
}

/** Renders adapter records in the wire shape Earth expects. */
export function describeAdapters(adapters, problems = []) {
  const seen = new Set();
  const out = adapters.map((a) => {
    seen.add(a.name);
    const record = {
      name: a.name,
      version: a.version,
      display_name: a.displayName,
      description: a.description,
      capabilities: a.capabilities,
      settings: a.settings,
      healthy: !a.stale,
    };
    if (a.error) record.error = a.error;
    // Reported so Earth can refuse work it knows will be shed, instead of
    // discovering the ceiling only when an assignment fails.
    if (a.limits) record.limits = a.limits;
    return record;
  });
  for (const problem of problems) {
    if (seen.has(problem.name)) continue;
    out.push({ name: problem.name, version: "", healthy: false, error: problem.error });
  }
  out.sort((a, b) => a.name.localeCompare(b.name));
  return out;
}

/**
 * Validates settings against the adapter's declared descriptors and fills
 * defaults. An unknown key is an error, not a silently ignored value: a typo in
 * earth.yaml must be reported at open time rather than quietly changing nothing.
 */
export function normalizeSettings(descriptors, values) {
  const known = new Map((descriptors ?? []).map((d) => [d.key, d]));
  const out = {};
  for (const key of Object.keys(values ?? {})) {
    if (!known.has(key)) {
      throw new ProtocolError("bad_request", `unknown setting ${JSON.stringify(key)}`);
    }
  }
  for (const descriptor of descriptors ?? []) {
    const present = Object.prototype.hasOwnProperty.call(values ?? {}, descriptor.key);
    const value = present ? values[descriptor.key] : undefined;
    if (value === undefined || value === null) {
      if (descriptor.default !== undefined) {
        out[descriptor.key] = descriptor.default;
        continue;
      }
      if (descriptor.required) {
        throw new ProtocolError("bad_request", `setting ${JSON.stringify(descriptor.key)} is required`);
      }
      continue;
    }
    out[descriptor.key] = coerceSetting(descriptor, value);
  }
  return out;
}

function coerceSetting(descriptor, value) {
  switch (descriptor.type) {
    case "string":
      if (typeof value !== "string") throw badSetting(descriptor, "a string");
      return value;
    case "path": {
      if (typeof value !== "string") throw badSetting(descriptor, "a string path");
      if (value !== "" && !isAbsolute(value)) throw badSetting(descriptor, "an absolute path");
      return value;
    }
    case "bool":
      if (typeof value !== "boolean") throw badSetting(descriptor, "a boolean");
      return value;
    case "int":
      if (typeof value !== "number" || !Number.isInteger(value)) throw badSetting(descriptor, "an integer");
      return value;
    case "string_list": {
      if (!Array.isArray(value) || value.some((item) => typeof item !== "string")) {
        throw badSetting(descriptor, "a list of strings");
      }
      return value;
    }
    case "enum": {
      const allowed = descriptor.values ?? [];
      if (typeof value !== "string" || !allowed.includes(value)) {
        throw new ProtocolError(
          "bad_request",
          `setting ${JSON.stringify(descriptor.key)} must be one of ${allowed.join(", ")}`,
        );
      }
      return value;
    }
    default:
      throw new ProtocolError("invalid_manifest", `setting ${descriptor.key} has unknown type`);
  }
}

function badSetting(descriptor, expected) {
  return new ProtocolError("bad_request", `setting ${JSON.stringify(descriptor.key)} must be ${expected}`);
}

function isAbsolute(value) {
  return value.startsWith("/") || /^[A-Za-z]:[\/]/.test(value) || value.startsWith("\\");
}

export function describeError(cause) {
  if (!cause) return "unknown error";
  const reason = cause.reason ? `${cause.reason}: ` : "";
  return `${reason}${cause.message ?? String(cause)}`;
}

/**
 * Watches for adapter changes without a dependency. Polling a directory mtime is
 * cheap at these sizes and behaves the same on every platform, unlike fs.watch.
 */
export function watchAdapters(root, onChange, { intervalMs = 750 } = {}) {
  let last = AsyncFingerprint.empty();
  let running = false;
  let timer = null;
  let stopped = false;

  const tick = async () => {
    if (stopped || running) return;
    running = true;
    try {
      const next = await AsyncFingerprint.of(root);
      if (!last.equals(next)) {
        last = next;
        await onChange();
      }
    } catch {
      // A transient read error must not kill the watcher.
    } finally {
      running = false;
    }
  };

  timer = setInterval(tick, intervalMs);
  if (typeof timer.unref === "function") timer.unref();
  builtinWatchInitial(tick);

  return {
    stop() {
      stopped = true;
      if (timer) clearInterval(timer);
    },
  };
}

// Kick off one probe so the baseline is captured before the first change.
function builtinWatchInitial(tick) {
  tick();
}

class AsyncFingerprint {
  constructor(parts) {
    this.parts = parts;
  }
  static empty() {
    return new AsyncFingerprint([]);
  }
  static async of(root) {
    const parts = [];
    let entries;
    try {
      entries = await readdir(root, { withFileTypes: true });
    } catch {
      return AsyncFingerprint.empty();
    }
    for (const entry of entries.sort((a, b) => a.name.localeCompare(b.name))) {
      if (!entry.isDirectory()) continue;
      const dir = join(root, entry.name);
      for (const file of [MANIFEST, "adapter.mjs"]) {
        try {
          const info = await stat(join(dir, file));
          parts.push(`${entry.name}/${file}:${info.mtimeMs}:${info.size}`);
        } catch {
          parts.push(`${entry.name}/${file}:missing`);
        }
      }
    }
    return new AsyncFingerprint(parts);
  }
  equals(other) {
    return (
      this.parts.length === other.parts.length &&
      this.parts.every((value, index) => value === other.parts[index])
    );
  }
}
