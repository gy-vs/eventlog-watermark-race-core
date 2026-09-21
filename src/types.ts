/** A single committed event. Sequences are contiguous and start at 1. */
export interface EventRecord {
  sequence: number;
  stream: string;
  payload: unknown;
}

/**
 * A point in time established by a subscriber BEFORE it reads the watermark.
 *
 * Any change notified after the observation is guaranteed to resolve wait(),
 * which is what makes the follow loop lossless: an event committed between
 * "read the watermark" and "start waiting" can never be missed.
 */
export interface Observation {
  /** Resolves on the first change notified after this observation was made. */
  wait(): Promise<void>;
  /** Deregisters the observation; a pending wait() then never resolves. */
  cancel(): void;
}

/** A commit log that a Follower can tail. */
export interface EventSource {
  /** Highest committed (visible) sequence; 0 when empty. Monotonic. */
  watermark(): number;
  /**
   * Events with sequence >= `from`, in sequence order, at most `limit` of
   * them. Contains sequence `from` whenever from <= watermark().
   */
  read(from: number, limit?: number): EventRecord[];
  /** Establish an observation point; see Follower for the protocol. */
  observe(): Observation;
}
