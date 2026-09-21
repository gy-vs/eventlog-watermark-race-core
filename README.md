# Event log core

TypeScript library for append-only event storage, with a lossless follower.

## API

- `EventLog` — flat in-memory log: `append`, `appendAll`, `read(from, limit)`, `watermark()`.
- `SegmentedEventLog` — same contract, split into segments that rotate at a
  fixed capacity (or on explicit `rotate()`); sequences stay contiguous
  across rotations.
- `Follower` — tails any `EventSource` and delivers every committed event
  exactly once, in sequence order. Async-iterable.
- `ChangeSignal` — the wakeup primitive shared by logs and followers.

## Lossless wakeup protocol

A naive reader scans to the tail, records the position, and only then
registers for notifications — an append landing between the two steps is
neither in the scan nor wakes the reader. The `Follower` inverts the order,
per pull:

1. establish an observation point (`source.observe()`),
2. read the visible commit watermark,
3. drain everything up to the watermark,
4. only then wait — and treat the wakeup purely as a hint to re-read.

Producers commit first and notify second (`append` pushes, then signals).
A commit racing the read therefore always resolves the wait, so nothing can
fall between "scanned" and "waiting". Notifications carry no payload: they
never stand in for an event, which makes coalesced and spurious wakeups
harmless.

## Acknowledgement

`follower.position` advances only to events whose delivery the consumer
confirmed by asking for the next one. Cancelling or throwing mid-event never
confirms it, so resuming a new follower from `position + 1` loses nothing
(the in-flight event may be redelivered — resume is at-least-once).

```ts
const follower = new Follower(log, { from: 1, batchSize: 128 });
for await (const event of follower) {
  process(event);                 // throwing here does not confirm `event`
  if (done) break;                // cancel: position stays at the last confirmed event
}
resumeFrom(follower.position + 1);
```

`follower.close()` (or an `AbortSignal`) stops delivery and settles any
pending pull without hanging; breaking out of `for await` does the same.

## Development

Run `npm install`, then `npm test` and `npm run build`.
