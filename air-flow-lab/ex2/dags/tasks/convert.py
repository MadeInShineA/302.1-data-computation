import os


def convert(base_path: str = "/opt/airflow"):
    """Convert segmented brain files to final format."""
    work_dir = os.path.join(base_path, "output", "neuro", "work")
    results_dir = os.path.join(base_path, "output", "neuro", "results")
    final_dir = os.path.join(results_dir, "final")

    # Input files
    input_gm = os.path.join(work_dir, "brain_gm.nii.gz")
    input_wm = os.path.join(work_dir, "brain_wm.nii.gz")
    input_csf = os.path.join(work_dir, "brain_csf.nii.gz")

    # Output files
    output_gm = os.path.join(final_dir, "final_gm.nii.gz")
    output_wm = os.path.join(final_dir, "final_wm.nii.gz")
    output_csf = os.path.join(final_dir, "final_csf.nii.gz")

    # Ensure directories exist
    os.makedirs(final_dir, exist_ok=True)

    # Convert files (in real scenario, this might change format, apply transformations, etc.)
    if all(os.path.exists(f) for f in [input_gm, input_wm, input_csf]):
        import shutil

        # Copy to final directory with new names
        shutil.copy2(input_gm, output_gm)
        shutil.copy2(input_wm, output_wm)
        shutil.copy2(input_csf, output_csf)

        # Create conversion log
        log_file = os.path.join(results_dir, "conversion_log.txt")
        with open(log_file, 'w') as f:
            f.write("# Format Conversion Log\n")
            f.write("Converted files to final format:\n")
            f.write(f"  - {output_gm}\n")
            f.write(f"  - {output_wm}\n")
            f.write(f"  - {output_csf}\n")
            f.write("Format: NIfTI compressed (.nii.gz)\n")
            f.write("Status: Conversion completed successfully\n")

        # Create pipeline summary
        summary_file = os.path.join(results_dir, "pipeline_summary.txt")
        with open(summary_file, 'w') as f:
            f.write("# Neuroimaging Pipeline Summary\n")
            f.write("Pipeline: cleanup_workdir -> ensure_dataset -> skullstrip -> segment -> convert\n")
            f.write("Status: All steps completed successfully\n")
            f.write("\nFinal outputs:\n")
            f.write(f"  - Gray Matter: {output_gm}\n")
            f.write(f"  - White Matter: {output_wm}\n")
            f.write(f"  - CSF: {output_csf}\n")

        print(f"Conversion completed. Final files in: {final_dir}")
        return {
            "final_gm": output_gm,
            "final_wm": output_wm,
            "final_csf": output_csf,
            "log_file": log_file,
            "summary_file": summary_file,
            "status": "completed"
        }
    else:
        missing = [f for f in [input_gm, input_wm, input_csf] if not os.path.exists(f)]
        raise FileNotFoundError(f"Segmented files not found: {missing}")