import os
from collections import Counter
from typing import Dict

def count_words_shard(
    base_path: str = ".",
    shard_filename: str = "",
    shard_id: int = 0
) -> str:
    """
    Count words in a single shard and save results to a file.

    Args:
        base_path: Base directory path
        shard_filename: Name of the shard file to process
        shard_id: ID of the shard for output naming

    Returns:
        Name of the output counts file
    """
    shard_path = os.path.join(base_path, "output", shard_filename)
    output_filename = f"counts_{shard_id:02d}.tsv"
    output_path = os.path.join(base_path, "output", output_filename)

    if not os.path.exists(shard_path):
        raise FileNotFoundError(f"Shard file not found: {shard_path}")

    word_counts = Counter()

    # Read shard and count words
    with open(shard_path, "r", encoding="utf-8") as f:
        for line in f:
            words = line.strip().split()
            if words:  # Only process non-empty lines
                word_counts.update(words)

    # Write counts to TSV file
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("word\tcount\n")
        for word, count in sorted(word_counts.items()):
            f.write(f"{word}\t{count}\n")

    return output_filename

__all__ = ["count_words_shard"]