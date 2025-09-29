import os


def validate_outputs(base_path: str = "/opt/airflow"):
    """Validate the outputs from the AFNI container."""
    results_file = os.path.join(base_path, "output", "neuro", "results", "afni_analysis.txt")

    if os.path.exists(results_file):
        with open(results_file, 'r') as f:
            content = f.read()

        print("AFNI Analysis Results:")
        print(content)

        # Check if analysis was successful
        if "Analysis completed successfully" in content:
            return {
                "status": "success",
                "results_file": results_file,
                "content": content
            }
        else:
            return {
                "status": "warning",
                "message": "Analysis may not have completed successfully",
                "content": content
            }
    else:
        return {
            "status": "error",
            "message": f"Results file not found: {results_file}"
        }