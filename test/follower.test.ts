import { describe, expect, it } from 'vitest';
import {
  EventLog,
  Follower,
  SegmentedEventLog,
} from '../src/index.js';
import type { EventRecord, EventSource } from '../src/index.js';

const tick = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const seq = (from: number, to: number) =>
  Array.from({ length: to - from + 1 }, (_, i) => from + i);

async function collect(
  source: EventSource,
  count: number,
  options?: ConstructorParameters<typeof Follower>[1],
): Promise<EventRecord[]> {
  const out: EventRecord[] = [];
  for await (const event of new Follower(source, options)) {
    out.push(event);
    if (out.length >= count) break;
  }
  return out;
}

function sequences(events: EventRecord[]): number[] {
  return events.map((e) => e.sequence);
}

/**
 * Deterministically reproduces a commit racing the follower's registration
 * protocol: the wrapped source appends `payloads` from inside the chosen
 * protocol step, i.e. exactly into the window where the old
 * scan-then-register reader lost events.
 */
type HookPoint = 'beforeObserve' | 'afterObserve' | 'watermark' | 'read';

function withAppendsAt(
  log: SegmentedEventLog,
  point: HookPoint,
  payloads: unknown[],
): EventSource {
  let fired = false;
  const fire = () => {
    if (fired) return;
    fired = true;
    log.appendAll('race', payloads);
  };
  return {
    observe() {
      if (point === 'beforeObserve') fire();
      const observation = log.observe();
      if (point === 'afterObserve') fire();
      return observation;
    },
    watermark() {
      const watermark = log.watermark();
      // Commit lands after the watermark was read but before the follower
      // waits — the classic lost-wakeup window. The stale value is returned
      // on purpose, as a racy read would observe it.
      if (point === 'watermark') fire();
      return watermark;
    },
    read(from, limit) {
      const batch = log.read(from, limit);
      if (point === 'read') fire();
      return batch;
    },
  };
}

describe('lossless wakeup protocol', () => {
  it.each(['beforeObserve', 'afterObserve', 'watermark', 'read'] as HookPoint[])(
    'delivers commits landing at %s during registration',
    async (point) => {
      const log = new SegmentedEventLog(4);
      log.appendAll('base', ['a', 'b']);
      const source = withAppendsAt(log, point, ['c', 'd', 'e']);
      const events = await collect(source, 5, { batchSize: 2 });
      expect(sequences(events)).toEqual([1, 2, 3, 4, 5]);
    },
  );

  it('does not lose an append between the watermark read and the wait', async () => {
    // The exact reported bug: scan sees the tail, then a commit lands before
    // the waiter is registered. The observation is established first, so the
    // commit's notification still resolves the wait — promptly.
    const log = new SegmentedEventLog(4);
    const source = withAppendsAt(log, 'watermark', ['late']);
    const result = await Promise.race([
      collect(source, 1).then((events) => sequences(events)),
      sleep(200).then(() => 'timed out' as const),
    ]);
    expect(result).toEqual([1]);
  });

  it('treats notifications as hints: spurious wakeups deliver nothing', async () => {
    const log = new EventLog();
    const follower = new Follower(log);
    const seen: number[] = [];
    const consuming = (async () => {
      for await (const event of follower) {
        seen.push(event.sequence);
        if (seen.length === 2) break;
      }
    })();
    await tick();
    log.changes.notify(); // nothing committed — must not produce an event
    log.changes.notify();
    await tick();
    await tick();
    expect(seen).toEqual([]);
    log.append('s', 'a');
    log.changes.notify(); // append already notified; extra poke is harmless
    log.append('s', 'b');
    await consuming;
    expect(seen).toEqual([1, 2]);
  });

  it('coalesced notifications still deliver every committed event', async () => {
    const log = new EventLog();
    const follower = new Follower(log);
    const seen: number[] = [];
    const consuming = (async () => {
      for await (const event of follower) {
        seen.push(event.sequence);
        if (seen.length === 10) break;
      }
    })();
    await tick(); // follower is now waiting
    for (let i = 0; i < 10; i += 1) log.append('s', i); // 10 notifies, one wakeup processed
    await consuming;
    expect(seen).toEqual(seq(1, 10));
  });

  it('delivers a multi-event append raised by a single notification', async () => {
    const log = new EventLog();
    const follower = new Follower(log, { batchSize: 2 });
    const seen: number[] = [];
    const consuming = (async () => {
      for await (const event of follower) {
        seen.push(event.sequence);
        if (seen.length === 5) break;
      }
    })();
    await tick();
    log.appendAll('s', ['a', 'b', 'c', 'd', 'e']); // one commit, one hint
    await consuming;
    expect(seen).toEqual([1, 2, 3, 4, 5]);
  });

  it('keeps every event for a slow consumer without dropping or duplicating', async () => {
    const log = new SegmentedEventLog(3);
    const follower = new Follower(log, { batchSize: 2 });
    const seen: number[] = [];
    const consuming = (async () => {
      for await (const event of follower) {
        seen.push(event.sequence);
        if (seen.length === 20) break;
        await sleep(1); // consumer is slower than the producer
      }
    })();
    for (let i = 0; i < 20; i += 1) log.append('s', i);
    await consuming;
    expect(seen).toEqual(seq(1, 20));
  });

  it('starts from a requested sequence and waits for it', async () => {
    const log = new EventLog();
    log.appendAll('s', [1, 2]);
    const follower = new Follower(log, { from: 5 });
    expect(follower.position).toBe(4);
    const seen: number[] = [];
    const consuming = (async () => {
      for await (const event of follower) {
        seen.push(event.sequence);
        if (seen.length === 2) break;
      }
    })();
    await tick();
    log.appendAll('s', [3, 4, 5, 6]);
    await consuming;
    expect(seen).toEqual([5, 6]);
  });
});

describe('acknowledgement', () => {
  it('confirms only delivered events; cancelling does not pre-acknowledge', async () => {
    const log = new EventLog();
    log.appendAll('s', [1, 2, 3, 4, 5]);
    const follower = new Follower(log, { batchSize: 5 });
    const seen: number[] = [];
    for await (const event of follower) {
      seen.push(event.sequence);
      if (seen.length === 3) break;
    }
    expect(seen).toEqual([1, 2, 3]);
    // Event 3 was delivered but never confirmed: the consumer broke before
    // asking for the next one, so the position stays at 2.
    expect(follower.position).toBe(2);

    // Resuming from position + 1 loses nothing; the in-flight event is
    // redelivered exactly once.
    const resumed = await collect(log, 3, { from: follower.position + 1 });
    expect(sequences(resumed)).toEqual([3, 4, 5]);
  });

  it('does not confirm the in-flight event when the consumer throws', async () => {
    const log = new EventLog();
    log.appendAll('s', [1, 2, 3]);
    const follower = new Follower(log);
    await expect(
      (async () => {
        for await (const event of follower) {
          if (event.sequence === 2) throw new Error('consumer failed');
        }
      })(),
    ).rejects.toThrow('consumer failed');
    expect(follower.position).toBe(1);
  });

  it('confirms up to the last delivered event while draining a batch', async () => {
    const log = new EventLog();
    log.appendAll('s', [1, 2, 3, 4]);
    const follower = new Follower(log, { batchSize: 4 });
    const iterator = follower[Symbol.asyncIterator]();
    expect(follower.position).toBe(0);
    await iterator.next(); // delivers 1
    expect(follower.position).toBe(0);
    await iterator.next(); // confirms 1, delivers 2
    expect(follower.position).toBe(1);
    await iterator.next(); // confirms 2, delivers 3
    expect(follower.position).toBe(2);
    await iterator.return!();
    expect(follower.position).toBe(2); // 3 stays unconfirmed
  });
});

describe('cancel and close races', () => {
  it('close() settles a pull that is waiting for events', async () => {
    const log = new EventLog();
    const follower = new Follower(log);
    const iterator = follower[Symbol.asyncIterator]();
    const pending = iterator.next();
    follower.close();
    await expect(pending).resolves.toEqual({ done: true, value: undefined });
    expect(follower.closed).toBe(true);
  });

  it('return() settles a pending pull instead of hanging', async () => {
    const log = new EventLog();
    const follower = new Follower(log);
    const iterator = follower[Symbol.asyncIterator]();
    const pending = iterator.next();
    await iterator.return!();
    await expect(pending).resolves.toMatchObject({ done: true });
  });

  it('close() is idempotent and later commits are not delivered', async () => {
    const log = new EventLog();
    const follower = new Follower(log);
    const iterator = follower[Symbol.asyncIterator]();
    follower.close();
    follower.close();
    log.append('s', 1);
    await expect(iterator.next()).resolves.toMatchObject({ done: true });
    expect(follower.position).toBe(0);
  });

  it('close() racing a buffered drain stops delivery without confirming', async () => {
    const log = new EventLog();
    log.appendAll('s', [1, 2, 3]);
    const follower = new Follower(log, { batchSize: 3 });
    const iterator = follower[Symbol.asyncIterator]();
    await iterator.next(); // delivers 1, buffers 2 and 3
    follower.close();
    await expect(iterator.next()).resolves.toMatchObject({ done: true });
    expect(follower.position).toBe(1);
  });

  it('an abort signal closes the follower', async () => {
    const log = new EventLog();
    const controller = new AbortController();
    const follower = new Follower(log, { signal: controller.signal });
    const iterator = follower[Symbol.asyncIterator]();
    const pending = iterator.next();
    controller.abort();
    await expect(pending).resolves.toMatchObject({ done: true });
    expect(follower.closed).toBe(true);
  });

  it('a follower created with an already-aborted signal is closed', async () => {
    const log = new EventLog();
    log.append('s', 1);
    const controller = new AbortController();
    controller.abort();
    const follower = new Follower(log, { signal: controller.signal });
    const iterator = follower[Symbol.asyncIterator]();
    await expect(iterator.next()).resolves.toMatchObject({ done: true });
  });

  it('cannot be iterated twice', () => {
    const follower = new Follower(new EventLog());
    follower[Symbol.asyncIterator]();
    expect(() => follower[Symbol.asyncIterator]()).toThrow(/iterated once/);
  });

  it('rejects invalid options', () => {
    const log = new EventLog();
    expect(() => new Follower(log, { from: 0 })).toThrow(RangeError);
    expect(() => new Follower(log, { batchSize: 0 })).toThrow(RangeError);
  });
});

describe('segment rotation', () => {
  it('follows appends across rotations, batch spanning segments', async () => {
    const log = new SegmentedEventLog(2); // rotate every 2 events
    const follower = new Follower(log, { batchSize: 3 }); // batch crosses boundaries
    const seen: number[] = [];
    const consuming = (async () => {
      for await (const event of follower) {
        seen.push(event.sequence);
        if (seen.length === 7) break;
      }
    })();
    for (let i = 0; i < 7; i += 1) {
      log.append('s', i);
      await tick(); // let the follower observe each rotation boundary
    }
    await consuming;
    expect(log.segmentCount).toBe(4); // 2 + 2 + 2 + 1
    expect(seen).toEqual(seq(1, 7));
  });

  it('delivers a batch appended across rotations from one notification', async () => {
    const log = new SegmentedEventLog(3);
    log.append('s', 0); // tail holds 1
    const follower = new Follower(log);
    const seen: number[] = [];
    const consuming = (async () => {
      for await (const event of follower) {
        seen.push(event.sequence);
        if (seen.length === 8) break;
      }
    })();
    await tick();
    log.appendAll('s', [1, 2, 3, 4, 5, 6, 7]); // fills the tail, rotates twice
    await consuming;
    expect(log.segmentCount).toBe(3); // 3 + 3 + 2
    expect(seen).toEqual(seq(1, 8));
  });

  it('survives explicit rotate() calls while the follower is waiting', async () => {
    const log = new SegmentedEventLog(100);
    log.append('s', 1);
    log.rotate();
    log.rotate(); // no-op on the empty tail
    const follower = new Follower(log);
    const seen: number[] = [];
    const consuming = (async () => {
      for await (const event of follower) {
        seen.push(event.sequence);
        if (seen.length === 3) break;
      }
    })();
    await tick();
    log.rotate(); // rotate while waiting (empty tail: no-op)
    log.append('s', 2);
    log.rotate(); // seal [2]
    log.append('s', 3);
    await consuming;
    expect(log.segmentCount).toBe(3);
    expect(seen).toEqual([1, 2, 3]);
  });

  it('a commit rotating the segment wakes a waiting follower', async () => {
    const log = new SegmentedEventLog(2);
    log.appendAll('s', ['a', 'b']); // tail is exactly full
    const follower = new Follower(log, { from: 3 });
    const seen: number[] = [];
    const consuming = (async () => {
      for await (const event of follower) {
        seen.push(event.sequence);
        if (seen.length === 2) break;
      }
    })();
    await tick();
    log.append('s', 'c'); // rotates, then commits into the fresh segment
    log.append('s', 'd');
    await consuming;
    expect(seen).toEqual([3, 4]);
  });
});
