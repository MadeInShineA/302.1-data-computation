import os


def create_sample_data(base_path: str = "/opt/airflow"):
    """Create sample neuroimaging data for processing."""
    data_dir = os.path.join(base_path, "output", "neuro", "data")

    # Create a simple sample data file
    sample_file = os.path.join(data_dir, "sample_data.txt")

    with open(sample_file, 'w') as f:
        f.write("# Sample neuroimaging dataset information\n")
        f.write("subject_001,session_01,task_rest,run_01\n")
        f.write("subject_002,session_01,task_rest,run_01\n")
        f.write("subject_003,session_01,task_rest,run_01\n")

    print(f"Created sample data file: {sample_file}")
    return sample_file