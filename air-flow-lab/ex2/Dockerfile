FROM apache/airflow:2.11.0-python3.9
USER root
# Install Docker CLI and required dependencies for Docker provider
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        apt-transport-https \
        ca-certificates \
        curl \
        gnupg \
        lsb-release && \
    curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/debian $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null && \
    apt-get update && \
    apt-get install -y --no-install-recommends docker-ce-cli && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Add the Python environment with Docker provider
RUN python -m pip install -U --no-cache-dir pip && \
    python -m pip install --no-cache-dir \
        "scikit-learn>=1.2" \
        "apache-airflow-providers-docker>=3.7.0"

# Ensure docker group exists with correct GID and add airflow user
RUN if ! getent group docker; then groupadd -g 997 docker; fi && \
    usermod -aG docker airflow && \
    # Verify the user was added to docker group
    id airflow

# Back to the airflow user
USER airflow

ENV PYTHONPATH="/opt/airflow/dags/src:/opt/airflow/dags"