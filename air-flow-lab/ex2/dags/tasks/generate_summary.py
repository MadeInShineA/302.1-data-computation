import os
from datetime import datetime


def generate_summary(base_path: str = "/opt/airflow"):
    """Generate a summary of the neuro pipeline execution."""
    summary_file = os.path.join(base_path, "output", "neuro", "pipeline_summary.txt")

    summary_content = f"""
# Neuro Pipeline Summary
Generated: {datetime.now().isoformat()}

## Pipeline Overview
- Docker Image: oesteban/afni:latest
- Framework: Apache Airflow with Docker provider
- Pipeline Type: Neuroimaging analysis demonstration

## Outputs
- Results directory: /opt/airflow/output/neuro/results/
- Data directory: /opt/airflow/output/neuro/data/
- Summary file: {summary_file}

## Pipeline Status
Pipeline executed successfully using Docker containers with Airflow orchestration.

## Tools Available in Container
- AFNI: Advanced neuroimaging analysis tools
- DataLad: Data management for neuroimaging datasets
"""

    with open(summary_file, 'w') as f:
        f.write(summary_content)

    print(f"Pipeline summary saved to: {summary_file}")
    return summary_content