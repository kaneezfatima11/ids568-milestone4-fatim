"""
generate_data.py - Synthetic Financial Transaction Data Generator
Generates 10M+ rows of realistic financial transaction data with seeded
randomness for reproducible results.
"""

import argparse
import os
import time
import numpy as np
import pandas as pd
from faker import Faker


def generate_transactions(rows: int, seed: int, output_dir: str, chunk_size: int = 500_000):
    """Generate synthetic financial transaction data in chunks."""
    fake = Faker()
    Faker.seed(seed)
    np.random.seed(seed)

    os.makedirs(output_dir, exist_ok=True)

    merchants = [fake.company() for _ in range(200)]
    categories = ["grocery", "entertainment", "travel", "dining", "utilities",
                  "healthcare", "retail", "electronics", "insurance", "transfer"]
    currencies = ["USD", "EUR", "GBP", "CAD", "AUD"]
    countries = ["US", "UK", "CA", "DE", "FR", "AU", "JP", "SG"]

    print(f"Generating {rows:,} rows with seed={seed} into '{output_dir}/'")
    start = time.time()

    chunks_written = 0
    rows_remaining = rows

    while rows_remaining > 0:
        n = min(chunk_size, rows_remaining)

        amounts = np.round(np.random.lognormal(mean=3.5, sigma=1.5, size=n), 2)
        amounts = np.clip(amounts, 0.01, 50000.0)

        df = pd.DataFrame({
            "transaction_id": [f"TXN{np.random.randint(1000000000, 9000000000)}"
                               for _ in range(n)],
            "user_id":        np.random.randint(1000, 100000, size=n),
            "merchant_id":    np.random.randint(1, 201, size=n),
            "merchant_name":  np.random.choice(merchants, size=n),
            "category":       np.random.choice(categories, size=n),
            "amount":         amounts,
            "currency":       np.random.choice(currencies, size=n),
            "country":        np.random.choice(countries, size=n),
            "is_fraud":       np.random.choice([0, 1], size=n, p=[0.98, 0.02]),
            "hour_of_day":    np.random.randint(0, 24, size=n),
            "day_of_week":    np.random.randint(0, 7, size=n),
            "timestamp":      pd.date_range(
                                  start="2023-01-01", periods=n, freq="3s"
                              ).astype(str),
        })

        out_path = os.path.join(output_dir, f"transactions_{chunks_written:04d}.parquet")
        df.to_parquet(out_path, index=False)

        chunks_written += 1
        rows_remaining -= n
        written = rows - rows_remaining
        print(f"  Written {written:,} / {rows:,} rows ({100*written/rows:.1f}%)")

    elapsed = time.time() - start
    print(f"Done! {chunks_written} files in '{output_dir}/' in {elapsed:.1f}s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic financial transaction data")
    parser.add_argument("--rows",       type=int, default=10_000_000)
    parser.add_argument("--seed",       type=int, default=42)
    parser.add_argument("--output",     type=str, default="data/")
    parser.add_argument("--chunk-size", type=int, default=500_000)
    args = parser.parse_args()

    generate_transactions(
        rows=args.rows,
        seed=args.seed,
        output_dir=args.output,
        chunk_size=args.chunk_size,
    )