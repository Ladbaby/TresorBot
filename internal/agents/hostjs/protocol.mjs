// Line-delimited JSON-RPC 2.0 framing for the TresorBot agent protocol.
//
// One reader, one writer, no interleaving. Records are separated by a single LF
// and nothing else: a generic line reader would also split on U+2028/U+2029,
// which are legal inside a JSON string.

export const PROTOCOL_VERSION = 1;
export const MAX_RECORD_BYTES = 8 * 1024 * 1024;

export class ProtocolError extends Error {
  constructor(reason, message) {
    super(message);
    this.name = "ProtocolError";
    this.reason = reason;
  }
}

/** Reads LF-framed records from a stream and yields parsed objects. */
export async function readRecords(stream, onRecord) {
  let pending = Buffer.alloc(0);
  for await (const chunk of stream) {
    pending = pending.length === 0 ? chunk : Buffer.concat([pending, chunk]);
    if (pending.length > MAX_RECORD_BYTES) {
      throw new ProtocolError("record_too_large", `record exceeds ${MAX_RECORD_BYTES} bytes`);
    }
    let index;
    while ((index = pending.indexOf(0x0a)) !== -1) {
      let line = pending.subarray(0, index);
      pending = pending.subarray(index + 1);
      if (line.length > 0 && line[line.length - 1] === 0x0d) {
        line = line.subarray(0, line.length - 1);
      }
      if (line.length === 0) continue;
      let value;
      try {
        value = JSON.parse(line.toString("utf8"));
      } catch (cause) {
        throw new ProtocolError("invalid_record", `invalid JSON record: ${cause.message}`);
      }
      onRecord(value);
    }
  }
}

/** Serialises one record. Refuses to emit something the host cannot parse back. */
export function encodeRecord(value) {
  const body = JSON.stringify(value);
  if (Buffer.byteLength(body) > MAX_RECORD_BYTES) {
    throw new ProtocolError("record_too_large", "outgoing record exceeds the size limit");
  }
  return body + "\n";
}

/**
 * A writer that owns stdout. Writes are queued so ordering is preserved even
 * when many sessions emit at once, and backpressure is honoured rather than
 * dropped: losing an event would desynchronise the turn.
 */
export class RecordWriter {
  constructor(output) {
    this.output = output;
    this.queue = [];
    this.draining = false;
    this.closed = false;
  }

  write(value) {
    if (this.closed) return;
    this.queue.push(encodeRecord(value));
    this.drain();
  }

  result(id, value) {
    this.write({ id, result: value ?? {} });
  }

  fail(id, code, message, reason) {
    const error = { code, message };
    if (reason) error.data = reason;
    this.write({ id, error });
  }

  notify(method, params) {
    this.write({ n: method, p: params });
  }

  drain() {
    if (this.draining) return;
    this.draining = true;
    while (this.queue.length > 0) {
      const chunk = this.queue.shift();
      const flushed = this.output.write(chunk);
      if (!flushed) {
        // Wait for the kernel to accept the rest before writing again.
        this.draining = false;
        this.output.once("drain", () => this.drain());
        return;
      }
    }
    this.draining = false;
  }

  close() {
    this.closed = true;
    this.queue.length = 0;
  }
}
