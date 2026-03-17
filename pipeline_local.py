import argparse
import os

def process_data(input_dir, output_dir):
    input_file = os.path.join(input_dir, "data.csv")
    output_file = os.path.join(output_dir, "processed.csv")

    os.makedirs(output_dir, exist_ok=True)

    with open(input_file, "r") as f_in, open(output_file, "w") as f_out:
        header = f_in.readline()
        f_out.write(header.strip() + ",value_sum\n")

        for line in f_in:
            parts = line.strip().split(",")
            value1 = int(parts[1])
            value2 = float(parts[2])

            value_sum = value1 + value2

            f_out.write(f"{parts[0]},{value1},{value2},{value_sum}\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, required=True)
    parser.add_argument("--output", type=str, required=True)

    args = parser.parse_args()

    process_data(args.input, args.output)
