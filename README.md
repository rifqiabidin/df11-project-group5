# Documentation

## Problem Statement
The marketing team of a bank company wants to launch a more effective marketing campaign program for housing loan product. To do this, they need to understand the different types of customers they have and how to target them. They request the data team to create customer segmentation based on their demographic characteristics, such as age, education, occupation, etc.

The marketing team need data source about customer segmentation in order to decide their marketing strategy. This reference must be reliable and future-proof for upcoming marketing campaign.

To initiate this project, the data team receives a dump file of customer profile data in csv format from the marketing team. The data team will create end to end data pipeline to provide reliable data source, insights and recommendations to the marketing team. The dev team will also create an API to feed the customer profile data to the marketing campaign program, so that the marketing team can send personalized messages and offers to each segment.

## Objective
1. Data pipeline that shows demographic customer segmentation from registration data
1. Analytical dashboard that shows customer segmentation and target market for housing loan product campaign

## Architecture
![Architecture](./architecture.png)

## Data Preparation
We treat the dump file as opening account bank process by integrate it with application form. By this approach, the raw file got copied to google spreadsheet first. Then We doing data enrichment by adding timestamp on spreadsheet. As we speak, the spreadhseet was integrated with google form as an application form for bank account opening. 

## Streaming
### Prerequisite:

A. Python 3: [Install Here](https://www.python.org/downloads/)
    
B. Docker and Docker Compose: [Install Here](https://docs.docker.com/engine/install/ubuntu/)
    
C. Google's Credential Service Account used for GCS and Bigquery access: [Get Here](https://developers.google.com/workspace/guides/create-credentials)
    
D. Kafka Python Libraries, Install using `pip` package manager.
    
Rename the google credentials into `google_credentials.json` and store it in your `$HOME` directory

### Project Guide:

A. Clone this project
            
    git clone https://github.com/rifqiabidin/df11-project-group5.git
        
B. Open this project in terminal and navigate to the directory of streaming folder	
        
    # change directory
    cd src/streaming
        
C. Install the required Kafka library, on terminal run following command

    pip install -r requirements.txt

D. Set up a local Kafka cluster by creating images and containers using `docker-compose`

    docker compose up
	
E. Start the data streaming by generating the messages using Kafka Producer

	python producer.py

The outputs will be messages sent to the topic on the Kafka cluster

F. While the Kafka Producer is running, open up new terminal and start the Kafka Consumer to read out the data from kafka topic

	python consumer.py
		
Messages generated by the produced will start showing up in the consumer window
    
![kafka producer consumer](./Kafka.jpg)

## Batch Process (DBT Cloud Run)

### Pre-requisites

There are several ways to recreate this project but to follow along line for line you will need:
 - You have a billable GCP project setup.
 - [Docker](https://www.docker.com/)


### Getting Started

Let's create a new virtual environment.

```bash
python -m venv venv
source venv/Scripts/activate
pip install -r requirements.txt
```

### Update your DBT Profile

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

### Batch Processing 
we are going perform ELT within the cloud run and then we will schedule the job every day at 3AM.


### Testing Locally
with virtual environment we create before, let try to run the main.py

```bash
python main.py
```
Download & Install [Postman](https://www.postman.com/) onto your system. then make POST request, if it "return 200, processing running successfully" then we are good to deploy it into cloud


### Deploy App to Cloud Run
we are going to use cloud build to build our image and set CI/CD pipeline within our repositories,by triggering new build each push into our GitHub repo. 

## Data Warehouse
We implement medallion architecture using dbt. 
1. Bronze Layer
Consist of raw state of the data source and select only the required column 
2. Silver Layer
Consist of cleaned and normalized data. In this layer we did feature encoding, standardization values, delete unneeded characters, and change the date into suitable format. Then we divide columsn into their own separate dimensions and fact table. To optimize it, we partition and cluster the table.
   ![ERD](./erd%20_final_project.png)
4. Gold Layer
Consist of refined data and presented in a format suitable for reporting

## Visualization
Analytical dashboard is one of key objectives on this project. Two dashboard have been created to provide insight for marketing team. The first dashboard shows overview insight for current customers demographics. It shows number of customers and customers who own housing loan that grouped by occupation and age group. We use simple score card to gives quick insight about number of customers and housing loan product. The barchart and stacked barchart used to shows it distribution among occupation and age group.

![Dashboard Overview](./dashboard-1.png)

The second dashboard talks about target market numbers. We defined target market as customers who elligble to buy housing loan product under certain criteria, default record and age group. We use barchart to shows distribution among occupation and treemap to show proportion for educational level. Both education and occupation were used by marketing team to decide their pricing and communication strategy for marketing campaign.

![Target Market Segmentation](./dashboard-2.png)

# Source
- [Code](/src)
- [Presentation](https://docs.google.com/presentation/d/1_grq5J4qOXGRR3SS7LwWzY4tLQM4eoFDs-DKsgmkfCg/edit?usp=sharing)
- [Dashboard](https://lookerstudio.google.com/reporting/d643c225-ab7b-4c13-a3fa-04e0f3ae1a9c)
