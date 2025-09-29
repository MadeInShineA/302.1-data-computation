import os
from typing import List

def split_into_shards(
    base_path: str = ".",
    input_file: str = "corpus.txt",
    num_shards: int = 4
) -> List[str]:
    """
    Split the corpus into approximately equal-sized shards for parallel processing.

    Args:
        base_path: Base directory path
        input_file: Input corpus file name
        num_shards: Number of shards to create

    Returns:
        List of shard file names
    """
    input_path = os.path.join(base_path, "output", input_file)

    # Read all lines
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    with open(input_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    if not lines:
        raise ValueError(f"Input file is empty: {input_path}")

    total_lines = len(lines)
    lines_per_shard = total_lines // num_shards
    remainder = total_lines % num_shards

    shard_files = []
    start_idx = 0

    for i in range(num_shards):
        # Distribute remainder lines among first few shards
        shard_size = lines_per_shard + (1 if i < remainder else 0)
        end_idx = start_idx + shard_size

        shard_filename = f"shard_{i:02d}.txt"
        shard_path = os.path.join(base_path, "output", shard_filename)

        with open(shard_path, "w", encoding="utf-8") as f:
            f.writelines(lines[start_idx:end_idx])

        shard_files.append(shard_filename)
        start_idx = end_idx

    return shard_files

__all__ = ["split_into_shards"]