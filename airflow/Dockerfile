
FROM apache/airflow:2.8.1

# Install system dependencies for Spark (Java, ps/ps aux, etc.) and
# then the Python Spark/Delta libraries as the airflow user.
USER root
RUN apt-get update \
	 && apt-get install -y --no-install-recommends \
		 default-jre-headless \
		 procps \
	 && apt-get clean \
	 && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
