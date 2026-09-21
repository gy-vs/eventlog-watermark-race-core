import { describe, expect, it } from 'vitest';
import { EventLog, Follower, SegmentedEventLog } from '../src/index.js';

const tick = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const seq = (n: number) => Array.from({ length: n }, (_, i) => i + 1);

/** Deterministic PRNG (mulberry32) so failures reproduce. */
function prng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

describe('stress', () => {
  it('delivers every committed sequence exactly once, in order', async () => {
    const random = prng(0xc0ffee);
    const log = new SegmentedEventLog(7); // rotate constantly
    const TOTAL = 3000;
    const follower = new Follower(log, { batchSize: 5 });
    const received: number[] = [];
    const consumer = (async () => {
      for await (const event of follower) {
        received.push(event.sequence);
        if (received.length === TOTAL) break;
        if (random() < 0.05) await sleep(1); // slow consumer
      }
    })();

    let appended = 0;
    while (appended < TOTAL) {
      const roll = random();
      if (roll < 0.15) {
        // burst: several events, one notification
        const n = Math.min(1 + Math.floor(random() * 8), TOTAL - appended);
        log.appendAll('burst', Array.from({ length: n }, () => random()));
        appended += n;
      } else {
        log.append('s', random());
        appended += 1;
      }
      if (random() < 0.3) await tick(); // let the consumer interleave
      if (random() < 0.05) log.changes.notify(); // spurious wakeup
      if (random() < 0.05) log.rotate(); // explicit rotation
    }
    await consumer;

    expect(log.watermark()).toBe(TOTAL);
    expect(received).toEqual(seq(TOTAL));
  });

  it('concurrent followers at different speeds each receive every event exactly once', async () => {
    const random = prng(42);
    const log = new SegmentedEventLog(5);
    const TOTAL = 1500;
    const followers = [
      new Follower(log, { batchSize: 1 }),
      new Follower(log, { batchSize: 4 }),
      new Follower(log, { batchSize: 64 }),
    ];
    const received: number[][] = followers.map(() => []);
    const consumers = followers.map((follower, i) =>
      (async () => {
        for await (const event of follower) {
          received[i].push(event.sequence);
          if (received[i].length === TOTAL) break;
          if (random() < 0.03 * (i + 1)) await sleep(1);
        }
      })(),
    );
    for (let i = 0; i < TOTAL; i += 1) {
      log.append('s', i);
      if (random() < 0.2) await tick();
    }
    await Promise.all(consumers);
    for (const actual of received) expect(actual).toEqual(seq(TOTAL));
  });

  it('cancel-and-resume chains never lose a committed sequence', async () => {
    const random = prng(7);
    const log = new SegmentedEventLog(4);
    const TOTAL = 500;
    log.appendAll('s', Array.from({ length: TOTAL }, (_, i) => i));

    const confirmed: number[] = [];
    let from = 1;
    while (from <= TOTAL) {
      const remaining = TOTAL - from + 1;
      const take = Math.min(2 + Math.floor(random() * 20), remaining);
      const follower = new Follower(log, {
        from,
        batchSize: 1 + Math.floor(random() * 9),
      });
      const iterator = follower[Symbol.asyncIterator]();
      for (let i = 0; i < take; i += 1) {
        const result = await iterator.next();
        // In-order and contiguous within the session, starting at `from`.
        expect(result.done).toBe(false);
        expect(result.value?.sequence).toBe(from + i);
      }
      if (take === remaining) {
        // Final session: one more pull confirms the last delivered event,
        // then cancelling settles that pull instead of hanging.
        const confirming = iterator.next();
        await iterator.return!();
        await expect(confirming).resolves.toMatchObject({ done: true });
      } else {
        await iterator.return!();
      }
      // Everything up to position was confirmed; resume exactly there.
      for (let s = from; s <= follower.position; s += 1) confirmed.push(s);
      from = follower.position + 1;
    }
    // Across all sessions every sequence was confirmed exactly once, in order.
    expect(confirmed).toEqual(seq(TOTAL));
  });

  it('a follower attached mid-stream catches up and then keeps up', async () => {
    const random = prng(2026);
    const log = new EventLog();
    const TOTAL = 800;
    log.appendAll('s', Array.from({ length: 300 }, (_, i) => i)); // backlog

    const follower = new Follower(log, { batchSize: 7 });
    const received: number[] = [];
    const consumer = (async () => {
      for await (const event of follower) {
        received.push(event.sequence);
        if (received.length === TOTAL) break;
        if (random() < 0.1) await tick();
      }
    })();
    for (let i = 300; i < TOTAL; i += 1) {
      log.append('s', i);
      if (random() < 0.4) await tick();
    }
    await consumer;
    expect(received).toEqual(seq(TOTAL));
  });
});
