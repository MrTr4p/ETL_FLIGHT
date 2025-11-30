# Start with the official Airflow image
FROM apache/airflow:2.7.3

# We switch to the powerful 'root' user to install packages and fix file permissions.
USER root

# ----------------------------------------------------
# 1. INSTALL PACKAGES AND JAVA (Requires Root)
# ----------------------------------------------------

# Install Network Utility
RUN apt-get update && apt-get install -y netcat-openbsd

# Install Java (OpenJDK-11) needed for Spark and Talend
RUN apt-get update \
  && apt-get install -y --no-install-recommends openjdk-11-jre-headless procps \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Set the Java location variable
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# ----------------------------------------------------
# 2. COPY AND FIX TALEND PERMISSIONS (Requires Root)
# ----------------------------------------------------

# Copy the Talend job files into the container.
# They will be owned by 'root' initially.
COPY etl-flight/parent_job /opt/airflow/talend_jobs/parent_job

# Change the file ownership from 'root' to 'airflow'.
# This lets the Airflow worker (which runs as 'airflow' user) access the files.
RUN chown -R airflow /opt/airflow/talend_jobs/parent_job

# Give the 'airflow' user permission to RUN the script.
# This fixes the "Permission denied" error.
RUN chmod +x /opt/airflow/talend_jobs/parent_job/parent_job_run.sh

# ----------------------------------------------------
# 3. INSTALL PYTHON DEPENDENCIES (Runs as 'airflow')
# ----------------------------------------------------

# Switch to the default, less privileged 'airflow' user for security and runtime.
USER airflow

# Install Python dependencies using the correct constraints file
RUN pip install --no-cache-dir \
    "apache-airflow-providers-apache-spark" \
    "kafka-python" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.3/constraints-3.8.txt"