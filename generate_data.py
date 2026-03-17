import argparse
import random
import os

def generate_data(rows, output_dir, seed):
    random.seed(seed)

    os.makedirs(output_dir, exist_ok=True)

    file_path = os.path.join(output_dir, "data.csv")

    with open(file_path, "w") as f:
        f.write("id,value1,value2\n")

        for i in range(rows):
            value1 = random.randint(1, 100)
            value2 = random.random()
            f.write(f"{i},{value1},{value2}\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--output", type=str, default="data/")
    parser.add_argument("--seed", type=int, default=42)

    args = parser.parse_args()

    generate_data(args.rows, args.output, args.seed)
