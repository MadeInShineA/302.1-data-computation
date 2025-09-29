import os


def prepare_directories(base_path: str = "/opt/airflow"):
    """Create necessary output directories for the neuro pipeline."""
    output_dir = os.path.join(base_path, "output", "neuro")
    data_dir = os.path.join(output_dir, "data")
    results_dir = os.path.join(output_dir, "results")

    # Create directories
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(results_dir, exist_ok=True)

    print(f"Created directories: {data_dir}, {results_dir}")
    return {"data_dir": data_dir, "results_dir": results_dir}