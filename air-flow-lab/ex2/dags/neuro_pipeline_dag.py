from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.docker.operators.docker import DockerOperator

from tasks.cleanup_workdir import cleanup_workdir
from tasks.ensure_dataset import ensure_dataset
from tasks.skullstrip import skullstrip
from tasks.segment import segment
from tasks.convert import convert
import os
import json


@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["neuro", "afni", "docker"]
)
def neuro_pipeline_dag():
    """
    Neuroimaging pipeline DAG: cleanup_workdir -> ensure_dataset -> skullstrip -> segment -> convert
    This DAG demonstrates a complete neuroimaging preprocessing pipeline.
    """

    @task
    def cleanup_workdir_task():
        """Clean up and prepare the working directory."""
        return cleanup_workdir(base_path="/opt/airflow")

    @task
    def ensure_dataset_task():
        """Ensure neuroimaging dataset is available."""
        return ensure_dataset(base_path="/opt/airflow")

    # Docker tasks for neuroimaging pipeline
    skullstrip_task = DockerOperator(
        task_id='skullstrip',
        image='oesteban/afni:latest',
        command=[
            'python3', '-c',
            '''
import sys
sys.path.append("/opt/airflow/dags")
from tasks.skullstrip import skullstrip
result = skullstrip("/opt/airflow")
print(f"Skullstrip completed: {result}")
            '''
        ],
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        auto_remove='success',
        mounts=[
            {
                'source': '/home/madeinshinea/Coding/airflow-lab-03/ex2/output',
                'target': '/opt/airflow/output',
                'type': 'bind'
            },
            {
                'source': '/home/madeinshinea/Coding/airflow-lab-03/ex2/dags',
                'target': '/opt/airflow/dags',
                'type': 'bind'
            }
        ],
        mount_tmp_dir=False,
        environment={
            'PYTHONPATH': '/opt/airflow/src:/opt/airflow/dags'
        }
    )

    segment_task = DockerOperator(
        task_id='segment',
        image='oesteban/afni:latest',
        command=[
            'python3', '-c',
            '''
import sys
sys.path.append("/opt/airflow/dags")
from tasks.segment import segment
result = segment("/opt/airflow")
print(f"Segmentation completed: {result}")
            '''
        ],
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        auto_remove='success',
        mounts=[
            {
                'source': '/home/madeinshinea/Coding/airflow-lab-03/ex2/output',
                'target': '/opt/airflow/output',
                'type': 'bind'
            },
            {
                'source': '/home/madeinshinea/Coding/airflow-lab-03/ex2/dags',
                'target': '/opt/airflow/dags',
                'type': 'bind'
            }
        ],
        mount_tmp_dir=False,
        environment={
            'PYTHONPATH': '/opt/airflow/src:/opt/airflow/dags'
        }
    )

    convert_task = DockerOperator(
        task_id='convert',
        image='oesteban/afni:latest',
        command=[
            'python3', '-c',
            '''
import sys
sys.path.append("/opt/airflow/dags")
from tasks.convert import convert
result = convert("/opt/airflow")
print(f"Conversion completed: {result}")
            '''
        ],
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        auto_remove='success',
        mounts=[
            {
                'source': '/home/madeinshinea/Coding/airflow-lab-03/ex2/output',
                'target': '/opt/airflow/output',
                'type': 'bind'
            },
            {
                'source': '/home/madeinshinea/Coding/airflow-lab-03/ex2/dags',
                'target': '/opt/airflow/dags',
                'type': 'bind'
            }
        ],
        mount_tmp_dir=False,
        environment={
            'PYTHONPATH': '/opt/airflow/src:/opt/airflow/dags'
        }
    )

    @task
    def display_results():
        """Collect and display final pipeline results in XCom."""
        base_path = "/opt/airflow"
        output_dir = os.path.join(base_path, "output")

        results = {
            "pipeline_status": "completed",
            "timestamp": datetime.now().isoformat(),
            "stages_completed": [
                "cleanup_workdir",
                "ensure_dataset",
                "skullstrip",
                "segment",
                "convert"
            ]
        }

        # Check for output files
        if os.path.exists(output_dir):
            output_files = []
            for root, dirs, files in os.walk(output_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    rel_path = os.path.relpath(file_path, output_dir)
                    file_size = os.path.getsize(file_path)
                    output_files.append({
                        "filename": rel_path,
                        "size_bytes": file_size,
                        "size_mb": round(file_size / (1024*1024), 2)
                    })

            results["output_files"] = output_files
            results["total_files"] = len(output_files)
            results["output_directory"] = output_dir
        else:
            results["output_files"] = []
            results["total_files"] = 0
            results["note"] = "Output directory not found"

        # Add summary statistics
        total_size_mb = sum(f.get("size_mb", 0) for f in results.get("output_files", []))
        results["total_output_size_mb"] = round(total_size_mb, 2)

        print("=== NEUROIMAGING PIPELINE RESULTS ===")
        print(json.dumps(results, indent=2))
        print("=====================================")

        return results

    # Define task dependencies
    cleanup = cleanup_workdir_task()
    dataset = ensure_dataset_task()
    summary = display_results()

    # Set up the sequential pipeline flow
    cleanup >> dataset >> skullstrip_task >> segment_task >> convert_task >> summary


# Instantiate the DAG
dag = neuro_pipeline_dag()