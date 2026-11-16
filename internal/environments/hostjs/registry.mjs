// Input adapter discovery and hot reload for the TresorBot input host.
//
// Deliberately the same shape as the agent host's registry.mjs: an input is a
// directory with environment.json and an entry module, reloading re-imports under a
// cache-busting URL, and instances that are already running keep the module
// object they started with. A broken input is reported, never thrown, so one bad
// adapter cannot stop Sun from starting.

import { existsSync } from "node:fs";
import { readFile, readdir, stat } from "node:fs/promises";
import { createHash } from "node:crypto";
import { pathToFileURL } from "node:url";
import { join, resolve, sep } from "node:path";

import { PROTOCOL_VERSION, ProtocolError } from "./protocol.mjs";

const MANIFEST = "environment.json";

export function validInputName(name) {
  return typeof name === "string" && /^[a-z0-9][a-z0-9_-]{0,63}$/.test(name);
}

export function describeError(cause) {
  if (!cause) return "unknown error";
  return cause.message ?? String(cause);
}

/** Loads one input directory, returning its record or throwing a clear error. */
export async function loadEnvironment(dir, { bust = 0 } = {}) {
  const manifestPath = join(dir, MANIFEST);
  const raw = await readFile(manifestPath, "utf8");
  let manifest;
  try {
    manifest = JSON.parse(raw);
  } catch (cause) {
    throw new ProtocolError("invalid_manifest", `${MANIFEST} is not valid JSON: ${cause.message}`);
  }
  if (!validInputName(manifest.name)) {
    throw new ProtocolError("invalid_manifest", `invalid input name ${JSON.stringify(manifest.name)}`);
  }
  if (manifest.protocol !== PROTOCOL_VERSION) {
    throw new ProtocolError(
      "protocol_mismatch",
      `input ${manifest.name} targets protocol ${manifest.protocol}, host speaks ${PROTOCOL_VERSION}`,
    );
  }
  if (typeof manifest.version !== "string" || manifest.version.length === 0) {
    throw new ProtocolError("invalid_manifest", `input ${manifest.name} has no version`);
  }
  if (typeof manifest.entry !== "string" || manifest.entry.length === 0) {
    throw new ProtocolError("invalid_manifest", `input ${manifest.name} has no entry`);
  }
  const entryPath = resolve(dir, manifest.entry);
  // An entry must stay inside its own directory; an input is trusted code but it
  // must still be the code the operator put there.
  if (!entryPath.startsWith(resolve(dir) + sep) && entryPath !== resolve(dir)) {
    throw new ProtocolError("invalid_manifest", `input ${manifest.name} entry escapes its directory`);
  }
  if (!existsSync(entryPath)) {
    throw new ProtocolError("missing_entry", `input ${manifest.name} entry ${manifest.entry} is missing`);
  }

  const capabilities = Array.isArray(manifest.capabilities) ? manifest.capabilities : [];
  const settings = Array.isArray(manifest.settings) ? manifest.settings : [];

  const entrySource = await readFile(entryPath, "utf8");
  const digest = createHash("sha256").update(raw).update("\0").update(entrySource).digest("hex");

  const url = pathToFileURL(entryPath).href + (bust ? `?v=${bust}` : "");
  const module = await import(url);
  const factory = module.default ?? module.environment;
  if (typeof factory !== "function") {
    throw new ProtocolError(
      "invalid_input",
      `input ${manifest.name} must export a default factory function`,
    );
  }
  // A describe-mode call lets a factory declare itself unusable (a missing CLI,
  // for instance) without the host guessing from its settings.
  const info = await factory({ manifest, mode: "describe" });
  if (!info || typeof info.createInstance !== "function") {
    throw new ProtocolError(
      "invalid_input",
      `input ${manifest.name} factory must return an object with createInstance()`,
    );
  }

  return {
    manifest,
    dir,
    name: manifest.name,
    version: manifest.version,
    displayName: manifest.display_name,
    description: manifest.description,
    capabilities,
    settings,
    requires: Array.isArray(manifest.requires) ? manifest.requires : [],
    limits: manifest.limits ?? {},
    sunset: manifest.sunset ?? {},
    digest,
    createInstance: info.createInstance,
  };
}

/** Scans a directory for input adapters, reporting failures per directory. */
export async function discover(dir) {
  let entries;
  try {
    entries = await readdir(dir, { withFileTypes: true });
  } catch (cause) {
    return { environments: [], problems: {}, missing: true };
  }
  const environments = [];
  const problems = {};
  for (const entry of entries) {
    if (!entry.isDirectory() || entry.name.startsWith(".")) continue;
    const environmentDir = join(dir, entry.name);
    try {
      environments.push(await loadEnvironment(environmentDir));
    } catch (cause) {
      const reason = cause instanceof ProtocolError ? cause.message : describeError(cause);
      problems[entry.name] = reason;
    }
  }
  environments.sort((a, b) => a.name.localeCompare(b.name));
  return { environments, problems, missing: false };
}

/** Renders the catalogue for the wire, including unhealthy entries. */
export function describeEnvironments(environments, problems) {
  const healthy = environments.map((environment) => ({
    name: environment.name,
    version: environment.version,
    display_name: environment.displayName,
    description: environment.description,
    capabilities: environment.capabilities,
    // Sunset lets Sun retire a capability without removing it from the manifest,
    // so a dashboard can mark an input as deprecated instead of silently failing
    // when the parser is removed from the Go side.
    sunset: environment.sunset ?? {},
    settings: environment.settings,
    healthy: true,
  }));
  const broken = Object.entries(problems ?? {}).map(([name, error]) => ({
    name,
    version: "",
    healthy: false,
    error,
  }));
  return [...healthy, ...broken].sort((a, b) => a.name.localeCompare(b.name));
}

/**
 * Watches the catalogue for changes, coalescing bursts into one reload.
 *
 * `baseline` is the fingerprint of the catalogue as it was already loaded. It is
 * supplied by the caller rather than captured on the first tick, because a change
 * that lands between startup and the first tick would otherwise be folded into
 * the baseline and never reported — an install immediately after boot would sit
 * invisible until the next change.
 */
export function watchEnvironments(dir, onChange, { intervalMs = 1000, baseline = null } = {}) {
  let timer = null;
  let stopped = false;
  let last = baseline;
  let primed = baseline !== null;
  const tick = async () => {
    if (stopped) return;
    try {
      const stamp = await catalogueStamp(dir);
      if (!primed) {
        last = stamp;
        primed = true;
      } else if (stamp !== last) {
        last = stamp;
        await onChange();
      }
    } catch {
      // A transient read failure must not tear the watcher down.
    }
    if (!stopped) timer = setTimeout(tick, intervalMs);
  };
  const stop = () => {
    stopped = true;
    if (timer) clearTimeout(timer);
  };
  if (typeof onChange === "function") {
    timer = setTimeout(tick, intervalMs);
  }
  return { stop };
}

/** Fingerprints the catalogue by directory mtime and each manifest's content. */
async function catalogueStamp(dir) {
  let entries;
  try {
    entries = await readdir(dir, { withFileTypes: true });
  } catch {
    return "missing";
  }
  const parts = [];
  for (const entry of entries.sort((a, b) => a.name.localeCompare(b.name))) {
    if (!entry.isDirectory() || entry.name.startsWith(".")) continue;
    const manifestPath = join(dir, entry.name, MANIFEST);
    try {
      const [text, info] = await Promise.all([readFile(manifestPath, "utf8"), stat(manifestPath)]);
      parts.push(`${entry.name}:${info.mtimeMs}:${text.length}`);
    } catch {
      parts.push(`${entry.name}:absent`);
    }
  }
  return parts.join("|");
}

export { MANIFEST, catalogueStamp };
