from datetime import datetime
from airflow.decorators import dag, task
from airflow.decorators  import task_group

from tasks.fetch_newsgroups import fetch_newsgroups_task
from tasks.prepare_dirs import prepare_dirs
from tasks.split_into_shards import split_into_shards
from tasks.count_words_shard import count_words_shard
from tasks.aggregate_counts import aggregate_counts


@dag(start_date=datetime(2024, 1, 1), schedule=None, catchup=False)
def count_words_dag():
    @task
    def prepare_dirs_task():
        return prepare_dirs(base_path="/opt/airflow")

    @task
    def fetch_newsgroups_task_wrapper():
        return fetch_newsgroups_task(base_path="/opt/airflow", subset="train")

    @task
    def split_into_shards_task():
        return split_into_shards(base_path="/opt/airflow", num_shards=4)

    @task
    def count_words_shard_task(shard_filename: str, shard_id: int):
        return count_words_shard(
            base_path="/opt/airflow",
            shard_filename=shard_filename,
            shard_id=shard_id
        )

    @task
    def aggregate_counts_task():
        return aggregate_counts(base_path="/opt/airflow")

    @task
    def display_results_task():
        import os

        # Read the final aggregated results
        results_path = "/opt/airflow/output/word_counts.tsv"

        if not os.path.exists(results_path):
            return "No results file found"

        # Read the entire TSV file content
        with open(results_path, 'r', encoding='utf-8') as f:
            tsv_content = f.read()

        # Read all words and their counts
        all_words = []
        total_unique_words = 0
        total_word_count = 0

        with open(results_path, 'r', encoding='utf-8') as f:
            next(f)  # Skip header
            for line in f:
                line = line.strip()
                if line:
                    word, count = line.split('\t')
                    count = int(count)
                    total_unique_words += 1
                    total_word_count += count
                    all_words.append({"word": word, "count": count})

        summary = {
            "total_unique_words": total_unique_words,
            "total_word_count": total_word_count,
            "all_words": all_words,
            "results_file": results_path,
            "tsv_file_content": tsv_content
        }

        return summary

    # Task instances
    prepare = prepare_dirs_task()
    fetch = fetch_newsgroups_task_wrapper()
    split = split_into_shards_task()

    # Create parallel count tasks for each shard
    @task_group(
            group_id="count_words"
    )
    def count_shards():
        count_tasks = []
        for i in range(4):
            count_task = count_words_shard_task.override(task_id=f"count_words_shard_{i:02d}")(
                shard_filename=f"shard_{i:02d}.txt",
                shard_id=i
            )
            count_tasks.append(count_task)
        count_tasks

    aggregate = aggregate_counts_task()
    display = display_results_task()

    # Set up dependencies
    prepare >> fetch >> split >> count_shards() >> aggregate >> display


dag = count_words_dag()
