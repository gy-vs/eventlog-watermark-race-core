import type { EventRecord, EventSource } from './types.js';

export interface FollowOptions {
  /** First sequence to deliver; defaults to 1. */
  from?: number;
  /** Maximum events read from the source per batch; defaults to 128. */
  batchSize?: number;
  /** Aborts the follower; equivalent to close(). */
  signal?: AbortSignal;
}

const DONE: IteratorResult<EventRecord> = { done: true, value: undefined };

/**
 * Tails an EventSource and delivers every committed event exactly once, in
 * sequence order.
 *
 * Lossless wakeup protocol, per pull:
 *   1. establish an observation point,
 *   2. read the visible commit watermark,
 *   3. drain everything up to the watermark,
 *   4. only then wait — and treat the wakeup purely as a hint to re-read.
 * A commit racing steps 1–4 happens after the observation, so its
 * notification resolves the wait and the loop re-checks. Nothing can fall
 * between "scanned up to the tail" and "registered for notifications".
 *
 * Acknowledgement: `position` advances only to events whose delivery the
 * consumer confirmed by asking for the next one. Cancelling or throwing
 * mid-event never confirms it, so resuming a new Follower from
 * `position + 1` loses nothing (the in-flight event may be redelivered).
 */
export class Follower implements AsyncIterable<EventRecord> {
  #source: EventSource;
  #batchSize: number;
  #next: number;
  #position: number;
  #unconfirmed: number | null = null;
  #buffer: EventRecord[] = [];
  #closed = false;
  #iterated = false;
  #wakeClose!: () => void;
  #closePromise: Promise<void>;
  #abortSignal?: AbortSignal;
  #onAbort?: () => void;

  constructor(source: EventSource, options: FollowOptions = {}) {
    const from = options.from ?? 1;
    if (!Number.isInteger(from) || from < 1) {
      throw new RangeError('from must be a positive integer');
    }
    const batchSize = options.batchSize ?? 128;
    if (!Number.isInteger(batchSize) || batchSize < 1) {
      throw new RangeError('batchSize must be a positive integer');
    }
    this.#source = source;
    this.#batchSize = batchSize;
    this.#next = from;
    this.#position = from - 1;
    this.#closePromise = new Promise<void>((resolve) => {
      this.#wakeClose = resolve;
    });
    const signal = options.signal;
    if (signal) {
      if (signal.aborted) {
        this.close();
      } else {
        this.#abortSignal = signal;
        this.#onAbort = () => this.close();
        signal.addEventListener('abort', this.#onAbort, { once: true });
      }
    }
  }

  /**
   * Last confirmed-delivered sequence; `from - 1` before anything is
   * delivered. The event currently held by the consumer is not confirmed
   * until it asks for the next one.
   */
  get position(): number {
    return this.#position;
  }

  get closed(): boolean {
    return this.#closed;
  }

  /** Stop delivery and settle any pending pull. Idempotent. */
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    if (this.#abortSignal && this.#onAbort) {
      this.#abortSignal.removeEventListener('abort', this.#onAbort);
    }
    this.#wakeClose();
  }

  [Symbol.asyncIterator](): AsyncIterator<EventRecord> {
    if (this.#iterated) {
      throw new Error(
        'a Follower can be iterated once; resume with a new Follower from position + 1',
      );
    }
    this.#iterated = true;
    let finished = false;
    const finish = (): IteratorResult<EventRecord> => {
      finished = true;
      return DONE;
    };
    return {
      next: async () => {
        if (finished) return DONE;
        const result = await this.#pull();
        if (result.done) finished = true;
        return result;
      },
      // return() must settle a pending pull rather than wait for it, so a
      // consumer breaking out of `for await` never hangs on an idle log.
      return: async () => {
        this.close();
        return finish();
      },
      throw: async (error?: unknown) => {
        this.close();
        finish();
        throw error;
      },
    };
  }

  async #pull(): Promise<IteratorResult<EventRecord>> {
    // The consumer came back for more: the previously delivered event is
    // confirmed, and only now does the position advance to it.
    if (this.#unconfirmed !== null) {
      this.#position = this.#unconfirmed;
      this.#unconfirmed = null;
    }
    if (this.#closed) return DONE;
    if (this.#buffer.length > 0) return this.#deliver(this.#buffer.shift()!);
    for (;;) {
      if (this.#closed) return DONE;
      // 1. Observation point first …
      const observation = this.#source.observe();
      // 2. … then the visible commit watermark …
      const watermark = this.#source.watermark();
      // 3. … then drain everything visible. A commit racing these steps is
      //    covered by the observation and wakes the wait below.
      if (this.#next <= watermark) {
        const batch = this.#source.read(this.#next, this.#batchSize);
        if (batch.length > 0) {
          for (let i = 1; i < batch.length; i += 1) this.#buffer.push(batch[i]);
          return this.#deliver(batch[0]);
        }
      }
      // 4. Wait for a hint. Whatever wakes us — a commit, a spurious poke,
      //    or close() — the loop re-establishes the observation and re-reads
      //    the watermark; the notification itself proves nothing.
      await Promise.race([observation.wait(), this.#closePromise]);
      observation.cancel();
    }
  }

  #deliver(event: EventRecord): IteratorResult<EventRecord> {
    if (event.sequence !== this.#next) {
      throw new Error(
        `source broke sequence contiguity: expected ${this.#next}, got ${event.sequence}`,
      );
    }
    this.#unconfirmed = event.sequence;
    this.#next = event.sequence + 1;
    return { done: false, value: event };
  }
}
