# DBT Cloud Run

## Pre-requisites

There are several ways to recreate this project but to follow along line for line you will need:
 - You have a billable GCP project setup.
 - [Docker](https://www.docker.com/)


## Getting Started

Let's create a new virtual environment.

```bash
python -m venv venv
source venv/Scripts/activate
pip install -r requirements.txt
```

## Update your DBT Profile

Update `dbt/profiles.yml` with YOUR_DATASET and YOUR_PROJECT.

```yml
YOUR_DBT_PROJECT:
  target: dev
  outputs:
    dev:
      type: bigquery
      dataset: dwh_final_project
      job_execution_timeout_seconds: 300
      job_retries: 1
      keyfile: /secrets/cloud-run-dbt-service-keyfile
      location: asia-southeast2
      method: service-account
      priority: interactive
      project: final-project-df11-group5
      threads: 4
```
We are going to mounted the service-keyfile as volume on the container so we don't need to include the service-account-key into the project repositories. 

## Batch Processing 
we are going perform ELT within the cloud run and then we will schedule the job every day at 3AM.


## Testing Locally
with virtual environment we create before, let try to run the main.py

```bash
python main.py
```
Download & Install [Postman](https://www.postman.com/) onto your system. then make POST request, if it "return 200, processing running successfully" then we are good to deploy it into cloud


## Deploy App to Cloud Run
we are going to use cloud build to build our image and set CI/CD pipeline within our repositories,by triggering new build each push into our GitHub repo. 