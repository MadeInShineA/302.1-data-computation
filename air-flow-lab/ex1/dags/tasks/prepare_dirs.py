import os
import shutil

def prepare_dirs(base_path: str = "."):
    """Ensure output folder exists and is clean from previous runs.

    - If `output/` exists: clean its contents but keep the directory (for mounted volumes).
    - If not: create `output/`.
    """
    dir_path = os.path.join(base_path, "output")
    if os.path.exists(dir_path):
        # Clean contents but keep the directory (handles mounted volumes)
        for filename in os.listdir(dir_path):
            file_path = os.path.join(dir_path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f'Failed to delete {file_path}. Reason: {e}')
    else:
        os.makedirs(dir_path, exist_ok=True)


__all__ = ["prepare_dirs"]