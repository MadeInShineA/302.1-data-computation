import os
from collections import Counter
from typing import List

def aggregate_counts(
    base_path: str = ".",
    count_files: List[str] = None,
    output_file: str = "word_counts.tsv"
) -> str:
    """
    Aggregate word counts from multiple shard count files into a single TSV.

    Args:
        base_path: Base directory path
        count_files: List of count file names to aggregate
        output_file: Name of the final aggregated output file

    Returns:
        Name of the output file
    """
    output_dir = os.path.join(base_path, "output")

    if not os.path.exists(output_dir):
        raise FileNotFoundError(f"Output directory not found: {output_dir}")

    if count_files is None:
        # Default to finding all counts_*.tsv files
        count_files = [f for f in os.listdir(output_dir) if f.startswith("counts_") and f.endswith(".tsv")]

    if not count_files:
        raise FileNotFoundError(f"No count files found in {output_dir}")

    total_counts = Counter()

    # Aggregate counts from all shard files
    for count_file in count_files:
        count_path = os.path.join(base_path, "output", count_file)

        if not os.path.exists(count_path):
            raise FileNotFoundError(f"Count file not found: {count_path}")

        with open(count_path, "r", encoding="utf-8") as f:
            # Skip header line
            next(f)
            for line in f:
                line = line.strip()
                if line:
                    try:
                        word, count = line.split('\t')
                        total_counts[word] += int(count)
                    except ValueError as e:
                        print(f"Warning: Skipping malformed line in {count_file}: {line}")
                        continue

    # Write aggregated counts to final TSV
    output_path = os.path.join(base_path, "output", output_file)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("word\tcount\n")
        # Sort by count descending, then by word ascending
        for word, count in sorted(total_counts.items(), key=lambda x: (-x[1], x[0])):
            f.write(f"{word}\t{count}\n")

    return output_file

__all__ = ["aggregate_counts"]