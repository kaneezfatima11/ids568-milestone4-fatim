"""
producer.py - Financial Transaction Event Producer
Generates streaming financial transaction events at configurable rates,
simulating realistic patterns including bursts and steady-state traffic.
"""

import argparse
import json
import queue
import random
import time
import threading
from datetime import datetime

import numpy as np
from faker import Faker

# Shared queue between producer and consumer
event_queue = queue.Queue(maxsize=10000)

fake = Faker()
Faker.seed(42)
random.seed(42)
np.random.seed(42)

CATEGORIES = ["grocery", "entertainment", "travel", "dining", "utilities",
              "healthcare", "retail", "electronics", "insurance", "transfer"]
CURRENCIES = ["USD", "EUR", "GBP", "CAD", "AUD"]
COUNTRIES  = ["US", "UK", "CA", "DE", "FR", "AU", "JP", "SG"]
MERCHANTS  = [fake.company() for _ in range(200)]


def generate_event() -> dict:
    """Generate a single realistic financial transaction event."""
    amount = round(np.random.lognormal(mean=3.5, sigma=1.5), 2)
    amount = float(np.clip(amount, 0.01, 50000.0))
    return {
        "transaction_id": f"TXN{random.randint(1000000000, 9999999999)}",
        "user_id":        random.randint(1000, 100000),
        "merchant_id":    random.randint(1, 201),
        "merchant_name":  random.choice(MERCHANTS),
        "category":       random.choice(CATEGORIES),
        "amount":         amount,
        "currency":       random.choice(CURRENCIES),
        "country":        random.choice(COUNTRIES),
        "is_fraud":       int(random.random() < 0.02),
        "hour_of_day":    datetime.now().hour,
        "day_of_week":    datetime.now().weekday(),
        "timestamp":      datetime.now().isoformat(),
    }


def produce(rate: int, duration: int, pattern: str):
    """
    Produce events at a given rate for a given duration.
    Patterns: steady, burst, ramp
    """
    print(f"Producer started | rate={rate} msg/s | duration={duration}s | pattern={pattern}")
    start      = time.time()
    total_sent = 0
    interval   = 1.0 / rate

    while time.time() - start < duration:
        loop_start = time.time()

        # Adjust rate based on pattern
        if pattern == "burst":
            # Every 10s: 5s of 3x rate, 5s of normal
            cycle = int(time.time() - start) % 10
            current_rate = rate * 3 if cycle < 5 else rate
        elif pattern == "ramp":
            # Gradually increase rate over duration
            elapsed  = time.time() - start
            factor   = 1.0 + (elapsed / duration) * 4
            current_rate = int(rate * factor)
        else:
            current_rate = rate

        current_interval = 1.0 / current_rate

        event = generate_event()
        try:
            event_queue.put(event, timeout=0.1)
            total_sent += 1
        except queue.Full:
            pass  # Drop event if queue is full (backpressure)

        # Print stats every 5 seconds
        elapsed = time.time() - start
        if total_sent % (current_rate * 5) == 0 and total_sent > 0:
            print(f"  [Producer] t={elapsed:.1f}s | sent={total_sent:,} | "
                  f"queue_depth={event_queue.qsize()} | rate={current_rate} msg/s")

        # Sleep to maintain rate
        sleep_time = current_interval - (time.time() - loop_start)
        if sleep_time > 0:
            time.sleep(sleep_time)

    # Signal consumer to stop
    event_queue.put(None)
    print(f"Producer done. Total sent: {total_sent:,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Financial transaction event producer")
    parser.add_argument("--rate",     type=int, default=100,
                        help="Events per second")
    parser.add_argument("--duration", type=int, default=60,
                        help="Duration in seconds")
    parser.add_argument("--pattern",  type=str, default="steady",
                        choices=["steady", "burst", "ramp"],
                        help="Traffic pattern")
    args = parser.parse_args()

    produce(args.rate, args.duration, args.pattern)