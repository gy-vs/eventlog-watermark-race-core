/**
 * 无丢唤醒协议（lossless wakeup protocol）
 *
 * 旧实现的竞态：读取器扫到文件尾后“先记录水位/序号，再注册等待”。
 * 追加若恰好发生在这两步之间，事件既不在扫描结果里，也不会触发等待，
 * 于是被永久丢弃。
 *
 * 本实现遵守以下不变量：
 *
 *  1. 观察点优先：订阅者先建立观察点（watch），再读取可见提交水位。
 *     观察点携带一个提交代号（epoch/generation）。
 *  2. 通知只是提示（level-triggered / hint）：通知既不携带事件也不携带
 *     序号，只表示“可能有新数据，请重新读取”。因此通知可被虚假触发、
 *     可合并，都不会造成丢事件或重复确认。
 *  3. 代号闭环：append 使代号单调递增；等待在“注册之后”重新比较代号，
 *     若注册前（建立观察点与读取水位之间）已有提交，则等待立即返回，
 *     关闭掉“扫描后—注册前”的丢事件窗口。
 *  4. 只确认实际交付：每次循环结束时，位置只推进到本次实际交付的最后
 *     一个序号；回调抛错、取消都不会推进游标，恢复后从原序号重读
 *     （at-least-once 重试 + 单调游标 = 恰好一次语义）。
 *
 * 整个循环为：建立观察点 → 读水位/批量读 → 交付并推进到最后交付序号
 * → 等待通知 → 建立新观察点重新检查。水位读取在观察点之后，保证
 * 观察点之后的任何提交都必然推动代号并唤醒等待。
 */

export type EventRecord = { sequence: number; stream: string; payload: unknown };

/**
 * 单调代号 + 等待者集合构成的通知总线。
 * 不承载任何事件内容：通知永远只是“重新读一次”的提示。
 */
export class WakeBus {
  #epoch = 0;
  #waiters = new Set<() => void>();
  #closed = false;

  /**
   * 建立观察点，返回当前提交代号。
   * 调用方必须在读取水位之前调用本方法。
   */
  watch(): number {
    return this.#epoch;
  }

  /** 日志已关闭后不再接受新的等待。 */
  get closed(): boolean {
    return this.#closed;
  }

  /**
   * 等待下一次通知。
   *
   * 注册在本方法内同步完成，并在注册后再次比较代号：
   *  - 若 watch(observed) 之后已经发生过 notify，立即 resolve；
   *  - 否则挂起，直到 notify/closeAll/abort。
   *
   * 这正是补掉“先记录水位、后注册等待”窗口的关键：建立观察点与真正
   * 注册之间发生的任何提交都会体现在代号上，从而立即唤醒。
   */
  wait(observed: number, signal?: AbortSignal): Promise<void> {
    if (this.#epoch !== observed || this.#closed) return Promise.resolve();
    if (signal?.aborted) return Promise.reject(signal.reason ?? new DOMException('aborted', 'AbortError'));
    return new Promise<void>((resolve, reject) => {
      const finish = (fn: () => void) => {
        this.#waiters.delete(wake);
        signal?.removeEventListener('abort', onAbort);
        fn();
      };
      const wake = () => finish(resolve);
      const onAbort = () => finish(() => reject(signal!.reason ?? new DOMException('aborted', 'AbortError')));
      // 注册必须先于再次比较，且二者之间没有 await：单线程下 append
      // 要么完整发生在注册前（代号已变，立即返回），要么在注册后
      // （wake 已在集合中，必然被通知）。
      this.#waiters.add(wake);
      if (this.#epoch !== observed || this.#closed) {
        wake();
        return;
      }
      signal?.addEventListener('abort', onAbort, { once: true });
    });
  }

  /**
   * 有新提交或仅需触发一次重新读取（包括虚假唤醒注入）。
   * 所有等待者最多被唤醒一次；多个 notify 合并为一次唤醒，因为唤醒后
   * 读取方会重新观察代号并按需再次等待。
   */
  notify(): void {
    this.#epoch++;
    const waiters = [...this.#waiters];
    this.#waiters.clear();
    for (const wake of waiters) wake();
  }

  /** 关闭总线：释放所有等待者，使其重新走“观察—读水位”路径自行收尾。 */
  closeAll(): void {
    this.#closed = true;
    const waiters = [...this.#waiters];
    this.#waiters.clear();
    for (const wake of waiters) wake();
  }
}

/** 一个段（segment）：序号 [base, base + events.length) 的连续事件。 */
export type Segment = { readonly base: number; readonly events: EventRecord[] };

/** 追加发生在日志关闭之后。 */
export class LogSealedError extends Error {
  constructor(message = 'cannot append: event log is closed') {
    super(message);
    this.name = 'LogSealedError';
  }
}

export class EventLog {
  #segments: Segment[] = [];
  /** 当前可追加段（始终在 #segments 末尾，rotate 后换新）。 */
  #active: { base: number; events: EventRecord[] };
  #committed = 0;
  #closed = false;
  #wake = new WakeBus();

  constructor() {
    this.#active = { base: 1, events: [] };
    this.#segments.push(this.#active);
  }

  /** 可见提交水位：已提交事件的最大序号。 */
  watermark(): number {
    return this.#committed;
  }

  /** 段总数（空段不计入，便于轮换测试断言）。 */
  segmentCount(): number {
    return this.#segments.reduce((n, s) => n + (s.events.length > 0 ? 1 : 0), 0);
  }

  get closed(): boolean {
    return this.#closed;
  }

  /** 仅供测试：注入虚假唤醒 / 验证观察点协议。通知不代表任何具体事件。 */
  get wakeBus(): WakeBus {
    return this.#wake;
  }

  /**
   * 原子追加单条事件。序号即提交顺序，push 与水位推进、通知之间没有
   * await，因此在观察点之后提交必然推动代号。
   */
  append(stream: string, payload: unknown): EventRecord {
    if (this.#closed) throw new LogSealedError();
    const event: EventRecord = { sequence: this.#committed + 1, stream, payload };
    this.#active.events.push(event);
    this.#committed = event.sequence;
    this.#wake.notify();
    return event;
  }

  /**
   * 原子追加一批事件：一次水位推进、一次通知。批量内多事件对等待者
   * 只产生一次唤醒提示，由读取方重新扫描批量交付。
   */
  appendMany(stream: string, payloads: readonly unknown[]): EventRecord[] {
    if (this.#closed) throw new LogSealedError();
    if (payloads.length === 0) return [];
    const base = this.#committed;
    const events = payloads.map((payload, i): EventRecord => ({
      sequence: base + i + 1,
      stream,
      payload,
    }));
    this.#active.events.push(...events);
    this.#committed = base + events.length;
    this.#wake.notify();
    return events;
  }

  /**
   * 轮换 segment：后续追加进入新段。对读取方完全透明——read/follow 都
   * 按序号跨段扫描，且轮换本身不产生也不需要通知。
   */
  rotate(): void {
    if (this.#closed) throw new LogSealedError();
    this.#active = { base: this.#committed + 1, events: [] };
    this.#segments.push(this.#active);
  }

  /**
   * 读取序号区间 [from, to] 内的已提交事件，跨段按序返回。
   * to 缺省读到当前水位（follow 循环始终传入自己在建立观察点之后读到
   * 的水位快照，避免快照不一致）。
   */
  read(from = 1, to = this.#committed): EventRecord[] {
    const end = Math.min(to, this.#committed);
    if (from > end || from < 1) return [];
    const out: EventRecord[] = [];
    for (const segment of this.#segments) {
      const lo = segment.base;
      const hi = segment.base + segment.events.length - 1;
      if (hi < from || lo > end) continue;
      for (const event of segment.events) {
        if (event.sequence >= from && event.sequence <= end) out.push(event);
      }
    }
    return out;
  }

  /**
   * 追随读取。返回一个在日志关闭且全部已提交事件交付完毕后 resolve
   * 的 Promise（resolve 值为最终交付的最后序号）。
   *
   * 每轮严格按协议执行：
   *   (1) watch() 建立观察点（拿到代号）
   *   (2) 读取水位（必须在观察点之后）
   *   (3) 批量读取并逐条交付
   *   (4) 游标只推进到“实际交付”的最后序号；回调抛错则原样拒绝且不推进
   *   (5) 无新数据时等待通知；唤醒只意味着回到 (1) 重新读取
   */
  async follow(
    onEvent: (event: EventRecord) => void | Promise<void>,
    options: { from?: number; signal?: AbortSignal } = {},
  ): Promise<number> {
    let cursor = Math.max(1, options.from ?? 1);
    const signal = options.signal;
    signal?.throwIfAborted();
    while (true) {
      // (1) 观察点优先
      const observed = this.#wake.watch();
      // (2) 观察点之后读水位
      const high = this.#committed;
      if (cursor <= high) {
        // (3) 批量读取（快照上界 = 本轮观察到的水位）
        const batch = this.read(cursor, high);
        let lastDelivered = cursor - 1;
        try {
          for (const event of batch) {
            signal?.throwIfAborted();
            await onEvent(event);
            // (4) 只推进到实际交付（await 完成）的最后序号
            lastDelivered = event.sequence;
          }
        } catch (err) {
          // 消费失败：游标不越过未交付事件。函数随之终止，恢复点
          // （最后一个真正交付完成的序号）附着在拒绝原因上，调用方
          // 应从 lastDeliveredSequence + 1 重新 follow，不会跳过也不会
          // 重复确认失败序号之前的事件。
          cursor = lastDelivered + 1;
          if (err !== null && (typeof err === 'object' || typeof err === 'function')) {
            (err as { lastDeliveredSequence?: number }).lastDeliveredSequence = lastDelivered;
          }
          throw err;
        }
        // 批量全部交付完成，才一次性推进到最后交付序号
        cursor = lastDelivered + 1;
      }
      // 显式取消优先：即使日志已关闭且恰好排空，也要以取消拒绝。
      signal?.throwIfAborted();
      // 关闭后仍要排空“观察快照之后、关闭之前”提交的事件：只有游标
      // 越过水位时才结束（wait 在关闭态立即返回，排空循环不会挂起）。
      if (this.#closed && cursor > this.#committed) return this.#committed;
      // (5) 通知只是提示；wait 在注册后重新比较代号，闭合竞态窗口
      await this.#wake.wait(observed, signal);
      // 醒来不假设发生了什么——回到 (1)，重新观察、重新读水位。
    }
  }

  /**
   * 关闭日志：拒绝新的追加，并通知所有跟随者。跟随者会先排空关闭时
   * 剩余的已提交事件，再结束。
   */
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#wake.closeAll();
  }
}
