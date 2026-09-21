import { ChangeSignal } from './change-signal.js';
import type { EventRecord, EventSource, Observation } from './types.js';

/** Flat in-memory append-only event log. */
export class EventLog implements EventSource {
  readonly changes = new ChangeSignal();
  #events: EventRecord[] = [];

  append(stream: string, payload: unknown): EventRecord {
    const event: EventRecord = {
      sequence: this.#events.length + 1,
      stream,
      payload,
    };
    // Commit first, signal second: a notification must never run ahead of
    // the commit it hints at.
    this.#events.push(event);
    this.changes.notify();
    return event;
  }

  /** Append a batch as one commit, raising a single notification. */
  appendAll(stream: string, payloads: Iterable<unknown>): EventRecord[] {
    const events: EventRecord[] = [];
    for (const payload of payloads) {
      const event: EventRecord = {
        sequence: this.#events.length + 1,
        stream,
        payload,
      };
      this.#events.push(event);
      events.push(event);
    }
    if (events.length > 0) this.changes.notify();
    return events;
  }

  read(from = 1, limit = Infinity): EventRecord[] {
    const start = Math.max(0, from - 1);
    return limit === Infinity
      ? this.#events.slice(start)
      : this.#events.slice(start, start + limit);
  }

  watermark(): number {
    return this.#events.length;
  }

  observe(): Observation {
    return this.changes.observe();
  }
}
