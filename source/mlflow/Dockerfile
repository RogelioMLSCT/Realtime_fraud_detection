FROM python:3.12.3-slim-bookworm
WORKDIR /app
COPY requirements.txt /tmp/requirements.txt

# install python packages as root (system)
RUN pip install -r /tmp/requirements.txt

# Copy the rest of the application
COPY . .

# switch back to airflow usr for safety
EXPOSE 5500