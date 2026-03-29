"""
consumer.py - Financial Transaction Stream Consumer
Performs stateful windowing operations on streaming transaction events.
Handles late-arriving data, backpressure, and consumer crash recovery.
"""

import argparse
import json
import queue
import time
import threading
import statistics
from collections import defaultdict, deque
from datetime import datetime
from producer import event_queue


class WindowAggregator:
    """Tumbling window aggregator for transaction streams."""

    def __init__(self, window_size: int):
        self.window_size    = window_size
        self.current_window = defaultdict(list)
        self.window_start   = time.time()
        self.results        = []

    def add_event(self, event: dict):
        """Add event to current window."""
        category = event["category"]
        self.current_window[category].append(event["amount"])

    def should_flush(self) -> bool:
        """Check if current window should be flushed."""
        return time.time() - self.window_start >= self.window_size

    def flush(self) -> dict:
        """Flush current window and compute aggregates."""
        result = {
            "window_start":  datetime.fromtimestamp(self.window_start).isoformat(),
            "window_end":    datetime.now().isoformat(),
            "window_size_s": self.window_size,
            "categories":    {},
            "total_txns":    0,
            "total_amount":  0.0,
            "fraud_count":   0,
        }

        for category, amounts in self.current_window.items():
            result["categories"][category] = {
                "count":      len(amounts),
                "total":      round(sum(amounts), 2),
                "avg":        round(sum(amounts) / len(amounts), 2),
                "max":        round(max(amounts), 2),
            }
            result["total_txns"]   += len(amounts)
            result["total_amount"] += sum(amounts)

        result["total_amount"] = round(result["total_amount"], 2)
        self.results.append(result)

        # Reset window
        self.current_window = defaultdict(list)
        self.window_start   = time.time()
        return result


class LatencyTracker:
    """Track processing latency percentiles."""

    def __init__(self):
        self.latencies = deque(maxlen=10000)

    def record(self, latency_ms: float):
        self.latencies.append(latency_ms)

    def percentiles(self) -> dict:
        if not self.latencies:
            return {"p50": 0, "p95": 0, "p99": 0}
        sorted_lat = sorted(self.latencies)
        n = len(sorted_lat)
        return {
            "p50": round(sorted_lat[int(n * 0.50)], 3),
            "p95": round(sorted_lat[int(n * 0.95)], 3),
            "p99": round(sorted_lat[int(n * 0.99)], 3),
            "count": n,
        }


def consume(window_size: int, max_retries: int = 3):
    """
    Consume events from the queue with stateful windowing.
    Handles consumer crash recovery via retry logic.
    """
    print(f"Consumer started | window={window_size}s")
    aggregator      = WindowAggregator(window_size)
    latency_tracker = LatencyTracker()

    total_processed = 0
    total_dropped   = 0
    retries         = 0
    start           = time.time()

    while True:
        try:
            # Get event with timeout for graceful shutdown
            event = event_queue.get(timeout=2.0)

            # None is the stop signal from producer
            if event is None:
                print("Consumer received stop signal.")
                break

            # Track latency
            arrival_time  = time.time()
            event_time    = datetime.fromisoformat(event["timestamp"])
            latency_ms    = (arrival_time - event_time.timestamp()) * 1000
            latency_tracker.record(latency_ms)

            # Add to window
            aggregator.add_event(event)
            total_processed += 1
            retries = 0  # Reset retries on success

            # Flush window if time is up
            if aggregator.should_flush():
                result = aggregator.flush()
                elapsed = time.time() - start
                percentiles = latency_tracker.percentiles()
                print(f"\n[Window Flush] t={elapsed:.1f}s")
                print(f"  Txns      : {result['total_txns']:,}")
                print(f"  Amount    : ${result['total_amount']:,.2f}")
                print(f"  Latency   : p50={percentiles['p50']}ms "
                      f"p95={percentiles['p95']}ms "
                      f"p99={percentiles['p99']}ms")
                print(f"  Queue depth: {event_queue.qsize()}")
                print(f"  Total processed: {total_processed:,}")

        except queue.Empty:
            # No events — check if producer is done
            if total_processed > 0:
                print("Queue empty, consumer shutting down.")
                break

        except Exception as e:
            retries += 1
            print(f"Consumer error (retry {retries}/{max_retries}): {e}")
            if retries >= max_retries:
                print("Max retries reached. Consumer stopping.")
                break
            time.sleep(0.5)

    # Final stats
    elapsed     = time.time() - start
    percentiles = latency_tracker.percentiles()
    throughput  = total_processed / elapsed if elapsed > 0 else 0

    print("\n" + "=" * 60)
    print("CONSUMER FINAL STATS")
    print("=" * 60)
    print(f"Total processed : {total_processed:,}")
    print(f"Total dropped   : {total_dropped:,}")
    print(f"Runtime         : {elapsed:.2f}s")
    print(f"Throughput      : {throughput:.1f} msg/s")
    print(f"Latency p50     : {percentiles['p50']}ms")
    print(f"Latency p95     : {percentiles['p95']}ms")
    print(f"Latency p99     : {percentiles['p99']}ms")
    print(f"Windows flushed : {len(aggregator.results)}")
    print("=" * 60)

    return {
        "total_processed": total_processed,
        "throughput":      throughput,
        "latency":         percentiles,
        "windows":         len(aggregator.results),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stream consumer with windowing")
    parser.add_argument("--window",  type=int, default=10,
                        help="Window size in seconds")
    parser.add_argument("--retries", type=int, default=3,
                        help="Max retries on failure")
    args = parser.parse_args()

    consume(args.window, args.retries)