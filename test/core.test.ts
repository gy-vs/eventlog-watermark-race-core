import { describe, expect, it } from 'vitest';
import { EventLog, SegmentedEventLog } from '../src/index.js';

it('appends in order', () => {
  const x = new EventLog();
  x.append('a', 1);
  expect(x.read()[0].sequence).toBe(1);
});

describe('EventLog', () => {
  it('reads a bounded batch from a sequence', () => {
    const log = new EventLog();
    log.appendAll('s', ['a', 'b', 'c', 'd']);
    expect(log.read(2, 2).map((e) => e.payload)).toEqual(['b', 'c']);
    expect(log.read(3).map((e) => e.sequence)).toEqual([3, 4]);
    expect(log.read(5)).toEqual([]);
  });

  it('appends a batch atomically with a single notification', () => {
    const log = new EventLog();
    let notifications = 0;
    const observation = log.observe();
    observation.wait().then(() => {
      notifications += 1;
    });
    const events = log.appendAll('s', [1, 2, 3]);
    expect(events.map((e) => e.sequence)).toEqual([1, 2, 3]);
    expect(log.watermark()).toBe(3);
    return observation
      .wait()
      .then(() => new Promise((r) => setTimeout(r, 10)))
      .then(() => {
        expect(notifications).toBe(1);
      });
  });

  it('exposes the commit watermark', () => {
    const log = new EventLog();
    expect(log.watermark()).toBe(0);
    log.append('s', 1);
    expect(log.watermark()).toBe(1);
  });
});

describe('SegmentedEventLog', () => {
  it('keeps sequences contiguous across rotations', () => {
    const log = new SegmentedEventLog(3);
    for (let i = 0; i < 7; i += 1) log.append('s', i);
    expect(log.segmentCount).toBe(3); // 3 + 3 + 1
    expect(log.watermark()).toBe(7);
    expect(log.read().map((e) => e.sequence)).toEqual([1, 2, 3, 4, 5, 6, 7]);
  });

  it('reads batches across segment boundaries', () => {
    const log = new SegmentedEventLog(2);
    log.appendAll('s', ['a', 'b', 'c', 'd', 'e']);
    expect(log.read(2, 3).map((e) => e.payload)).toEqual(['b', 'c', 'd']);
    expect(log.read(4, 10).map((e) => e.sequence)).toEqual([4, 5]);
  });

  it('rotates explicitly and ignores rotate on an empty tail', () => {
    const log = new SegmentedEventLog(100);
    log.rotate();
    expect(log.segmentCount).toBe(1);
    log.append('s', 1);
    log.rotate();
    expect(log.segmentCount).toBe(2);
    log.rotate();
    expect(log.segmentCount).toBe(2);
    log.append('s', 2);
    expect(log.read().map((e) => e.sequence)).toEqual([1, 2]);
  });

  it('appends a batch spanning several segments with one notification', () => {
    const log = new SegmentedEventLog(3);
    log.append('s', 0);
    const events = log.appendAll('s', [1, 2, 3, 4, 5, 6, 7]);
    expect(events).toHaveLength(7);
    expect(log.segmentCount).toBe(3); // 3 + 3 + 2
    expect(log.read().map((e) => e.sequence)).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);
  });

  it('rejects a non-positive segment capacity', () => {
    expect(() => new SegmentedEventLog(0)).toThrow(RangeError);
  });
});
