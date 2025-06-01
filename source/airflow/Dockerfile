FROM apache/airflow:2.10.5

# create models directory and set ownership
USER root
RUN mkdir -p /app/models

USER airflow
# copy requirements file
COPY requirements.txt /tmp/requirements.txt

# install python packages as root (system)
RUN pip install -r /tmp/requirements.txt

# switch back to airflow usr for safety
USER airflow
