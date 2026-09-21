import { ChangeSignal } from './change-signal.js';
import type { EventRecord, EventSource, Observation } from './types.js';

/**
 * Append-only event log split into segments. The tail segment rotates once it
 * reaches `segmentCapacity` events, or when rotate() is called explicitly.
 * Sequences stay contiguous across rotations, so readers never observe the
 * boundaries.
 */
export class SegmentedEventLog implements EventSource {
  readonly changes = new ChangeSignal();
  readonly segmentCapacity: number;
  #segments: EventRecord[][] = [[]];
  #size = 0;

  constructor(segmentCapacity = 1024) {
    if (!Number.isInteger(segmentCapacity) || segmentCapacity < 1) {
      throw new RangeError('segmentCapacity must be a positive integer');
    }
    this.segmentCapacity = segmentCapacity;
  }

  get segmentCount(): number {
    return this.#segments.length;
  }

  append(stream: string, payload: unknown): EventRecord {
    this.#rotateIfFull();
    const event: EventRecord = { sequence: this.#size + 1, stream, payload };
    this.#tail().push(event);
    this.#size += 1;
    // Commit (and rotate) first, signal second — never the other way round.
    this.changes.notify();
    return event;
  }

  /** Append a batch as one commit, rotating as needed, with one notification. */
  appendAll(stream: string, payloads: Iterable<unknown>): EventRecord[] {
    const events: EventRecord[] = [];
    for (const payload of payloads) {
      this.#rotateIfFull();
      const event: EventRecord = { sequence: this.#size + 1, stream, payload };
      this.#tail().push(event);
      this.#size += 1;
      events.push(event);
    }
    if (events.length > 0) this.changes.notify();
    return events;
  }

  /** Seal the tail segment and open a new one. No-op while the tail is empty. */
  rotate(): void {
    if (this.#tail().length > 0) this.#segments.push([]);
  }

  read(from = 1, limit = Infinity): EventRecord[] {
    const out: EventRecord[] = [];
    let base = 0; // events contained in the segments walked so far
    for (const segment of this.#segments) {
      if (base + segment.length < from) {
        base += segment.length;
        continue; // the whole segment lies before `from`
      }
      for (const event of segment) {
        if (event.sequence < from) continue;
        out.push(event);
        if (out.length >= limit) return out;
      }
      base += segment.length;
    }
    return out;
  }

  watermark(): number {
    return this.#size;
  }

  observe(): Observation {
    return this.changes.observe();
  }

  #tail(): EventRecord[] {
    return this.#segments[this.#segments.length - 1];
  }

  #rotateIfFull(): void {
    if (this.#tail().length >= this.segmentCapacity) this.#segments.push([]);
  }
}
