import type { Observation } from './types.js';

/**
 * Lossless wakeup primitive shared by producers and followers.
 *
 * Protocol: producers call notify() only AFTER their commit is visible;
 * consumers observe() BEFORE reading the watermark and treat every wakeup as
 * a mere hint to re-read. notify() carries no payload — it never stands in
 * for an event — so coalesced and spurious wakeups are both harmless.
 */
export class ChangeSignal {
  #version = 0;
  #waiters = new Set<() => void>();

  /** Establish an observation point capturing the current change version. */
  observe(): Observation {
    const version = this.#version;
    let wake: (() => void) | null = null;
    let promise: Promise<void> | null = null;
    return {
      wait: () => {
        if (promise) return promise;
        // A notify() that landed between observe() and wait() must not be
        // missed: the version has moved, so resolve immediately.
        if (this.#version !== version) {
          promise = Promise.resolve();
          return promise;
        }
        promise = new Promise<void>((resolve) => {
          // The version check above and this registration are one synchronous
          // run, so no notify() can slip between them.
          const waiter = () => {
            this.#waiters.delete(waiter);
            resolve();
          };
          wake = waiter;
          this.#waiters.add(waiter);
        });
        return promise;
      },
      cancel: () => {
        if (wake) this.#waiters.delete(wake);
      },
    };
  }

  /** Wake every current observer. Only ever a hint to re-read, never a payload. */
  notify(): void {
    this.#version += 1;
    const waiters = [...this.#waiters];
    this.#waiters.clear();
    for (const wake of waiters) wake();
  }
}
