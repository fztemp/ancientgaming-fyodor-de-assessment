FROM apache/airflow:2.11.0

# Update Environment
USER root
RUN apt-get update \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

# Dependencies
ENV PIP_USER=false
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r ./requirements.txt
COPY .flake8 ./.flake8
ENV PIP_USER=true

ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"
