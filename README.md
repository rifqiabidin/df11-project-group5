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
We treat the dump file as opening account bank process by integrate it with application form. By this approach, the raw file got copied to google spreadsheet first. Then We doing data enrichment by adding user id and timestamp on spreadsheet. As we speak, the spreadhseet was integrated with google form as an application form for bank account opening. 

## Streaming

## Batch

## Transformation

## Visualization
![Dashboard Overview](./dashboard-1.png)

![Target Market Segmentation](./dashboard-2.png)

# Source
- [Code](/src)
- [Presentation]()
- [Dashboard]()