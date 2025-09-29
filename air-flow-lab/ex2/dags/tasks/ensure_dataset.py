import os
import urllib.request


def ensure_dataset(base_path: str = "/opt/airflow"):
    """Ensure neuroimaging dataset is available for processing."""
    data_dir = os.path.join(base_path, "output", "neuro", "data")
    work_dir = os.path.join(base_path, "output", "neuro", "work")

    # Create data directory if it doesn't exist
    os.makedirs(data_dir, exist_ok=True)

    # Check if we already have a dataset
    sample_brain = os.path.join(data_dir, "brain.nii.gz")

    if not os.path.exists(sample_brain):
        # Create a simple NIfTI header as placeholder for demo
        # In real scenario, this would download actual neuroimaging data
        with open(sample_brain, 'wb') as f:
            # Minimal NIfTI header (348 bytes) + some dummy data
            nifti_header = b'\x5c\x01\x00\x00' + b'\x00' * 344  # Basic NIfTI-1 header
            dummy_data = b'\x00' * 1000  # Placeholder brain data
            f.write(nifti_header + dummy_data)

        print(f"Created sample brain dataset: {sample_brain}")
    else:
        print(f"Dataset already exists: {sample_brain}")

    # Create metadata file
    metadata_file = os.path.join(data_dir, "dataset_info.txt")
    with open(metadata_file, 'w') as f:
        f.write("# Neuroimaging Dataset Information\n")
        f.write("Dataset: Sample Brain Data\n")
        f.write("Format: NIfTI (.nii.gz)\n")
        f.write("Purpose: Neuroimaging pipeline demonstration\n")

    return {
        "dataset_file": sample_brain,
        "metadata_file": metadata_file,
        "status": "ready"
    }