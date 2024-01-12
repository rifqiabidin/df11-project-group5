FROM ghcr.io/dbt-labs/dbt-bigquery:1.5.6

# Set the working directory in the container
WORKDIR /app
COPY . ./

RUN pip install --no-cache-dir --trusted-host pypi.python.org -r requirements.txt

# Run loading script when the container launches
ENTRYPOINT ["python", "app.py"]  