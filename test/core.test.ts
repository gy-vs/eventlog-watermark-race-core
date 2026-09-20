import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { EventLog, LogSealedError, WakeBus } from '../src/index.js';

/** 等待所有已排期的微任务/定时器回调执行完毕。 */
async function settle(ms = 1): Promise<void> {
  await vi.advanceTimersByTimeAsync(ms);
}

/** 让挂起的跟随者进入 wait()（watch→watermark→wait 是连续微任务）。 */
async function parked(): Promise<void> {
  await vi.advanceTimersByTimeAsync(0);
  await Promise.resolve();
  await Promise.resolve();
}

describe('EventLog basics', () => {
  it('appends in order', () => {
    const x = new EventLog();
    x.append('a', 1);
    expect(x.read()[0].sequence).toBe(1);
  });

  it('read/watermark over segments', () => {
    const log = new EventLog();
    log.appendMany('s', [1, 2]);
    log.rotate();
    log.appendMany('s', [3, 4]);
    expect(log.watermark()).toBe(4);
    expect(log.read(2, 3).map((e) => e.sequence)).toEqual([2, 3]);
    expect(log.read(3).map((e) => e.sequence)).toEqual([3, 4]);
    expect(log.segmentCount()).toBe(2);
  });

  it('rejects append after close', () => {
    const log = new EventLog();
    log.close();
    expect(() => log.append('s', 1)).toThrow(LogSealedError);
    expect(() => log.rotate()).toThrow(LogSealedError);
  });
});

describe('follow: live delivery', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it('delivers historical events then live events', async () => {
    const log = new EventLog();
    log.appendMany('s', [1, 2]);
    const seen: number[] = [];
    const done = log.follow((e) => void seen.push(e.sequence));
    await parked();
    expect(seen).toEqual([1, 2]);

    log.append('s', 3);
    await parked();
    expect(seen).toEqual([1, 2, 3]);

    log.appendMany('s', [4, 5, 6]);
    await parked();
    expect(seen).toEqual([1, 2, 3, 4, 5, 6]);

    log.close();
    await expect(done).resolves.toBe(6);
  });

  it('starts from the given sequence', async () => {
    const log = new EventLog();
    log.appendMany('s', [1, 2, 3]);
    const seen: number[] = [];
    const done = log.follow((e) => void seen.push(e.sequence), { from: 2 });
    await parked();
    expect(seen).toEqual([2, 3]);
    log.close();
    await done;
  });
});

describe('follow: append at every timing around registration', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it('append before follow() starts is seen via watermark scan', async () => {
    const log = new EventLog();
    log.append('s', 'before');
    const seen: string[] = [];
    const done = log.follow((e) => void seen.push(e.payload as string));
    await parked();
    expect(seen).toEqual(['before']);
    log.close();
    await done;
  });

  it('append between watch and watermark read cannot be lost', async () => {
    // 精确命中“建立观察点之后、读取水位之前”：watch 同步返回旧代号后，
    // 在 follow 读水位前同步注入一次追加。正确行为：事件被本轮扫描交付，
    // 随后的 wait(旧代号) 因代号已推进而立即返回，绝不挂起或重复交付。
    const log = new EventLog();
    const bus = log.wakeBus;
    const realWatch = bus.watch.bind(bus);
    let armed = true;
    vi.spyOn(bus, 'watch').mockImplementation(() => {
      const epoch = realWatch();
      if (armed) {
        armed = false;
        log.append('s', 'in-window');
      }
      return epoch;
    });

    const seen: string[] = [];
    const done = log.follow((e) => void seen.push(e.payload as string));
    await settle(5);
    expect(seen).toEqual(['in-window']);
    // 跟随者没有因旧代号而永久挂起：后续追加照常送达
    log.append('s', 'after');
    await settle(5);
    expect(seen).toEqual(['in-window', 'after']);
    log.close();
    await done;
  });

  it('append between watermark read and wait registration cannot be lost', async () => {
    // 这是最初报告的竞态。协议下无法通过公开 API 插入该窗口（watch→
    // watermark→wait 注册之间无 await），这里直接在总线层验证：
    // wait() 注册前代号已推进时，wait 必须立即返回而非挂起。
    const bus = new WakeBus();
    const observed = bus.watch();
    bus.notify(); // 模拟“记录水位后、注册前”的追加
    let woke = false;
    await bus.wait(observed).then(() => void (woke = true));
    expect(woke).toBe(true);
  });

  it('append after wait registration wakes the follower', async () => {
    const log = new EventLog();
    const seen: number[] = [];
    const done = log.follow((e) => void seen.push(e.sequence));
    await parked(); // 已在 wait 中
    log.append('s', 1);
    await parked();
    expect(seen).toEqual([1]);
    log.close();
    await done;
  });

  it('append racing with close: committed before close is drained', async () => {
    const log = new EventLog();
    const seen: number[] = [];
    const done = log.follow((e) => void seen.push(e.sequence));
    await parked();
    log.appendMany('s', [1, 2, 3]);
    log.close();
    await settle(5);
    expect(seen).toEqual([1, 2, 3]);
    await done;
  });
});

describe('follow: spurious wakeups and coalesced notifications', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it('spurious wakeup with no new data simply re-parks', async () => {
    const log = new EventLog();
    const done = log.follow(() => {
      /* no-op */
    });
    await parked();
    log.wakeBus.notify(); // 虚假唤醒：没有任何事件
    await parked();
    log.append('s', 1);
    const seen: number[] = [];
    // 验证虚假唤醒后跟随者仍然活着：再起一个跟随者观察
    const d2 = log.follow((e) => void seen.push(e.sequence));
    await parked();
    expect(seen).toEqual([1]);
    log.close();
    await Promise.all([done, d2]);
  });

  it('many notifications coalesce; none of the events are lost', async () => {
    const log = new EventLog();
    const seen: number[] = [];
    const done = log.follow((e) => void seen.push(e.sequence));
    await parked();
    // 连续 100 次通知，其中只有部分携带追加；通知合并不允许丢事件
    for (let i = 1; i <= 100; i++) {
      if (i % 3 === 0) log.append('s', i);
      log.wakeBus.notify();
    }
    await settle(10);
    // 序号连续（payload 才是 3 的倍数）：合并不允许丢任何事件
    expect(seen).toEqual(Array.from({ length: 33 }, (_, i) => i + 1));
    log.close();
    await done;
  });

  it('batch append produces a single wake but delivers every event in order', async () => {
    const log = new EventLog();
    let wakes = 0;
    vi.spyOn(log.wakeBus, 'notify').mockImplementation(() => wakes++);
    const evs = log.appendMany('s', Array.from({ length: 50 }, (_, i) => i));
    expect(evs).toHaveLength(50);
    expect(wakes).toBe(1);
    vi.restoreAllMocks();

    const seen: number[] = [];
    const done = log.follow((e) => void seen.push(e.sequence));
    await parked();
    expect(seen).toEqual(Array.from({ length: 50 }, (_, i) => i + 1));
    log.close();
    await done;
  });
});

describe('follow: slow consumer', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it('delivers events that arrive during slow onEvent without skipping', async () => {
    const log = new EventLog();
    log.appendMany('s', [1, 2]);
    const seen: number[] = [];
    let release: (() => void) | undefined;
    const gate = new Promise<void>((r) => (release = r));

    const done = log.follow(async (e) => {
      seen.push(e.sequence);
      if (e.sequence === 1) await gate;
    });
    await parked(); // 交付了 1，正挂在 gate 上
    expect(seen).toEqual([1]);
    // 慢消费者处理期间持续追加（含一次批量、一次轮换）
    log.append('s', 3);
    log.rotate();
    log.appendMany('s', [4, 5]);
    await parked(); // 跟随者仍被 gate 阻塞，不应有越界推进
    expect(seen).toEqual([1]);
    release!();
    await settle(10);
    expect(seen).toEqual([1, 2, 3, 4, 5]);
    log.close();
    await done;
  });
});

describe('follow: cancellation and close races', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it('already-aborted signal rejects before any delivery', async () => {
    const log = new EventLog();
    log.append('s', 1);
    const controller = new AbortController();
    controller.abort(new Error('cancel'));
    const seen: number[] = [];
    await expect(log.follow((e) => void seen.push(e.sequence), { signal: controller.signal })).rejects.toThrow(
      'cancel',
    );
    expect(seen).toEqual([]);
  });

  it('abort while parked rejects; no event after abort is delivered', async () => {
    const log = new EventLog();
    const seen: number[] = [];
    const controller = new AbortController();
    const done = log.follow((e) => void seen.push(e.sequence), { signal: controller.signal });
    done.catch(() => undefined); // 断言在下方 await；先挂处理器避免瞬态 unhandled rejection
    await parked();
    controller.abort(new Error('stop'));
    await settle(5);
    log.append('s', 1);
    await settle(5);
    expect(seen).toEqual([]);
    await expect(done).rejects.toThrow('stop');
  });

  it('abort during delivery leaves resume point on the rejection', async () => {
    const log = new EventLog();
    log.appendMany('s', [1, 2, 3]);
    const controller = new AbortController();
    let release: (() => void) | undefined;
    const gate = new Promise<void>((r) => (release = r));
    const seen: number[] = [];
    const done = log.follow(
      async (e) => {
        seen.push(e.sequence);
        if (e.sequence === 2) {
          controller.abort(new Error('stop'));
          await gate;
        }
      },
      { signal: controller.signal },
    );
    done.catch(() => undefined); // 断言在下方 await
    await parked();
    release!();
    const err = await done.then(
      () => new Error('should have rejected'),
      (e) => e as Error & { lastDeliveredSequence?: number },
    );
    expect(err.message).toBe('stop');
    // 序号 3 的回调在 abort 检查处未执行；恢复点 = 2
    expect(err.lastDeliveredSequence).toBe(2);
    expect(seen).toEqual([1, 2]);

    // 从恢复点继续：3 恰好补交付一次
    const resumed: number[] = [];
    const d2 = log.follow((e) => void resumed.push(e.sequence), { from: 3 });
    await parked();
    expect(resumed).toEqual([3]);
    log.close();
    await d2;
  });

  it('handler failure does not advance past the failed event; resume works', async () => {
    const log = new EventLog();
    log.appendMany('s', [1, 2, 3, 4]);
    const seen: number[] = [];
    const boom = new Error('boom');
    const done = log.follow(async (e) => {
      seen.push(e.sequence);
      if (e.sequence === 3) throw boom;
    });
    done.catch(() => undefined); // 断言在下方 await
    await settle(5);
    const err = (await done.then(
      () => new Error('should have rejected'),
      (e) => e as Error & { lastDeliveredSequence?: number },
    ));
    expect(err).toBe(boom);
    expect(err.lastDeliveredSequence).toBe(2);
    expect(seen).toEqual([1, 2, 3]); // 3 的回调执行了但未确认

    const resumed: number[] = [];
    const d2 = log.follow((e) => void resumed.push(e.sequence), { from: 3 });
    await parked();
    expect(resumed).toEqual([3, 4]);
    log.close();
    await d2;
  });

  it('close while parked drains nothing more and resolves', async () => {
    const log = new EventLog();
    const seen: number[] = [];
    const done = log.follow((e) => void seen.push(e.sequence));
    await parked();
    log.close();
    await expect(done).resolves.toBe(0);
    expect(seen).toEqual([]);
  });

  it('close racing a slow consumer: late commits are still drained', async () => {
    const log = new EventLog();
    log.append('s', 1);
    const seen: number[] = [];
    let release: (() => void) | undefined;
    const gate = new Promise<void>((r) => (release = r));
    const done = log.follow(async (e) => {
      seen.push(e.sequence);
      if (e.sequence === 1) await gate;
    });
    await parked();
    // 慢回调期间关闭日志（关闭后不可再追加；这些提交发生在关闭前的快照后）
    log.appendMany('s', [2, 3]);
    log.close();
    release!();
    await settle(10);
    expect(seen).toEqual([1, 2, 3]);
    await done;
  });

  it('abort racing close on an empty log wins (rejects, does not resolve)', async () => {
    const log = new EventLog();
    const controller = new AbortController();
    const done = log.follow(() => undefined, { signal: controller.signal });
    done.catch(() => undefined);
    await parked();
    controller.abort(new Error('cancel'));
    log.close();
    await settle(5);
    await expect(done).rejects.toThrow('cancel');
  });
});

describe('WakeBus protocol unit', () => {
  it('wait returns immediately when epoch advanced before registration', async () => {
    const bus = new WakeBus();
    const observed = bus.watch();
    bus.notify();
    let done = false;
    await bus.wait(observed).then(() => (done = true));
    expect(done).toBe(true);
  });

  it('wait parks when nothing changed and releases on notify', async () => {
    const bus = new WakeBus();
    const observed = bus.watch();
    let done = false;
    const p = bus.wait(observed).then(() => (done = true));
    await Promise.resolve();
    expect(done).toBe(false);
    bus.notify();
    await p;
    expect(done).toBe(true);
  });

  it('notifications to a parked waiter coalesce into one wake, epoch reflects all', async () => {
    const bus = new WakeBus();
    const observed = bus.watch();
    const wakes: number[] = [];
    await Promise.resolve();
    const p = bus.wait(observed).then(() => wakes.push(bus.watch()));
    bus.notify();
    bus.notify();
    bus.notify();
    await p;
    expect(wakes).toEqual([3]);
  });

  it('closeAll releases parked waiters; wait on closed bus resolves at once', async () => {
    const bus = new WakeBus();
    const observed = bus.watch();
    let done = false;
    const p = bus.wait(observed).then(() => (done = true));
    bus.closeAll();
    await p;
    expect(done).toBe(true);
    await bus.wait(bus.watch()); // 不挂起
    expect(bus.closed).toBe(true);
  });

  it('abort rejects a parked wait and removes it from the waiter set', async () => {
    const bus = new WakeBus();
    const controller = new AbortController();
    const p = bus.wait(bus.watch(), controller.signal);
    await Promise.resolve();
    controller.abort(new Error('cancel'));
    await expect(p).rejects.toThrow('cancel');
    // 之后的 notify 不应触及已注销的等待者（不抛错即可）
    expect(() => bus.notify()).not.toThrow();
  });

  it('abort that loses the race to notify still wakes normally', async () => {
    const bus = new WakeBus();
    const observed = bus.watch();
    const controller = new AbortController();
    const p = bus.wait(observed, controller.signal);
    bus.notify(); // 通知先到
    controller.abort(new Error('cancel'));
    await expect(p).resolves.toBeUndefined();
  });
});

describe('stress: exactly-once in-order delivery under chaos', () => {
  it('single producer / single follower, fake timers, interleaved appends', async () => {
    vi.useFakeTimers();
    const log = new EventLog();
    const N = 5_000;
    const delivered: number[] = [];
    const done = log.follow((e) => void delivered.push(e.sequence));

    for (let i = 1; i <= N; ) {
      if (i % 10 === 0) {
        const first = i++;
        if (i <= N) {
          const second = i++;
          log.appendMany('s', [first, second]);
        } else {
          log.appendMany('s', [first]);
        }
      } else {
        log.append('s', i++);
      }
      if (i % 250 === 0) {
        log.wakeBus.notify(); // 周期性虚假唤醒
        await vi.advanceTimersByTimeAsync(1);
      }
      if (i % 1000 === 0) log.rotate();
    }
    log.close();
    await vi.advanceTimersByTimeAsync(10);
    await done;

    expect(delivered).toHaveLength(N);
    delivered.forEach((seq, i) => expect(seq).toBe(i + 1));
  });

  it('multiple followers each receive every committed sequence exactly once', async () => {
    vi.useRealTimers();
    const log = new EventLog();
    const N = 20_000;
    const F = 4;
    const streams = Array.from({ length: F }, () => [] as number[]);
    const done = streams.map((seen) =>
      log.follow((e) => {
        seen.push(e.sequence);
        // 慢消费者：不同的步进制造不同的追赶快慢
        if (e.sequence % 31 === seen.length % 7) return Promise.resolve();
      }),
    );

    // 给跟随者时间挂起，再开始生产
    await new Promise((r) => setTimeout(r, 5));
    let seq = 0;
    function pump(): void {
      const chunk = Math.min(500, N - seq);
      if (chunk <= 0) return;
      const kind = seq % 4;
      if (kind === 0) {
        log.appendMany('s', Array.from({ length: chunk }, (_, k) => seq + k + 1));
      } else {
        for (let k = 0; k < chunk; k++) log.append('s', seq + k + 1);
      }
      seq += chunk;
      if (seq % 4000 < 500) log.rotate();
      if (seq < N) setImmediate(pump);
      else log.close();
    }
    pump();

    const finals = await Promise.all(done);
    for (const final of finals) expect(final).toBe(N);
    for (const seen of streams) {
      expect(seen).toHaveLength(N);
      for (let i = 0; i < N; i++) expect(seen[i]).toBe(i + 1);
    }
    expect(new Set(streams.map((s) => s.length)).size).toBe(1);
  });

  it('repeated resume after random handler failures covers every sequence once overall', async () => {
    vi.useRealTimers();
    const log = new EventLog();
    const N = 10_000;
    const allDelivered: number[] = [];
    let producerClosed = false;
    let attempts = 0;

    // 后台生产者：单条/批量追加交替，并在途中轮换 segment。
    const producer = (async () => {
      let seq = 0;
      while (seq < N) {
        if (seq % 4 === 0) {
          const chunk = Math.min(127, N - seq);
          log.appendMany('s', Array.from({ length: chunk }, (_, k) => seq + k + 1));
          seq += chunk;
        } else {
          for (let k = 0; k < 127 && seq < N; k++) log.append('s', seq++ + 1);
        }
        if (seq % 4000 < 127 && Math.random() < 0.3) log.rotate();
        await new Promise((r) => setImmediate(r));
      }
      log.close();
      producerClosed = true;
    })();

    let from = 1;
    let failureSeed = 0;
    for (;;) {
      attempts++;
      let next = from; // 本轮实际交付进度
      let failedAt: number | null = null;
      const followPromise = log.follow(
        async (e) => {
          // 伪随机瞬时失败（约 1/131）：种子在每次重启时递增，同一序号
          // 重试后大概率换一条轨道，不会在原地无限失败。失败回调不确认。
          if (failedAt === null && e.sequence !== from && (e.sequence * 31 + failureSeed * 17) % 131 === 0) {
            failedAt = e.sequence;
            throw new Error('transient');
          }
          allDelivered.push(e.sequence);
          next = e.sequence + 1;
        },
        { from },
      );
      // 同步挂处理器，避免 abort/失败拒绝产生 unhandled rejection 噪音。
      const result = await followPromise.then(
        (final: number) => ({ ok: true as const, final }),
        (err: Error & { lastDeliveredSequence?: number }) => ({ ok: false as const, err }),
      );

      if (result.ok) {
        // 正常结束只可能发生在日志关闭后排空时
        expect(producerClosed).toBe(true);
        expect(next).toBe(N + 1);
        break;
      }
      expect(failedAt).not.toBeNull();
      expect(result.err.lastDeliveredSequence).toBe(failedAt! - 1);
      from = failedAt!;
      failureSeed++;
      if (attempts > 10_000) throw new Error('too many restarts');
    }
    await producer;

    expect(attempts).toBeGreaterThan(1); // 确实发生了失败重启
    expect(allDelivered).toHaveLength(N);
    allDelivered.forEach((seq, i) => expect(seq).toBe(i + 1));
  }, 30_000);
});
