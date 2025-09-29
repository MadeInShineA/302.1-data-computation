import os
import shutil


def cleanup_workdir(base_path: str = "/opt/airflow"):
    """Clean up and prepare the working directory for neuroimaging pipeline."""
    work_dir = os.path.join(base_path, "output", "neuro", "work")

    # Remove existing work directory if it exists
    if os.path.exists(work_dir):
        shutil.rmtree(work_dir)
        print(f"Removed existing work directory: {work_dir}")

    # Create fresh work directory
    os.makedirs(work_dir, exist_ok=True)
    print(f"Created clean work directory: {work_dir}")

    return {"work_dir": work_dir, "status": "cleaned"}