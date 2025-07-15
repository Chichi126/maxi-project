FROM apache/airflow:latest-python3.10
# use the official airflow image as a base
USER root

# Install any system dependencies here if required
RUN apt-get update && apt-get install -y \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# copy requirements.txt file
COPY requirements.txt /requirements.txt

# Install python dependencies
RUN pip install --no-cache-dir -r /requirements.txt

# Copy dags and other files
COPY dags/ /opt/airflow/dags/
COPY plugins/ /opt/airflow/plugins

