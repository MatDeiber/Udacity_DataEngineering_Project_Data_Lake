# Udacity-NDDE-Project: STEDI Human Balance Analytics

This project is part of Udacity Data Engineer Nanodegree. 

In this project, we are using AWS Glue to process data from multiple sources, categorize the data, and curate it to be queried in the future for multiple purposes.


## Project Details
The STEDI Team has been hard at work developing a hardware STEDI Step Trainer that:

 - trains the user to do a STEDI balance exercise;
 - and has sensors on the device that collect data to train a machine-learning algorithm to detect steps; 
 - has a companion mobile app that collects customer data and interacts with the device sensors.
 - 
STEDI has heard from millions of early adopters who are willing to purchase the STEDI Step Trainers and use them.

Several customers have already received their Step Trainers, installed the mobile application, and begun using them together to test their balance. The Step Trainer is just a motion sensor that records the distance of the object detected. The app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions.

The STEDI team wants to use the motion sensor data to train a machine learning model to detect steps accurately in real-time. Privacy will be a primary consideration in deciding what data can be used.

Some of the early adopters have agreed to share their data for research purposes. Only these customers’ Step Trainer and accelerometer data should be used in the training data for the machine learning model.


## Project Objectives

Landing Zone : 
 - Use Glue Studio to ingest data from an S3 bucket
 - Manually create a Glue Table using Glue Console from JSON data
 - Use Athena to query the Landing Zone.

Trusted Zone :
 - Configure Glue Studio to dynamically update a Glue Table schema from JSON data
 - Use Athena to query Trusted Glue Tables
 - Join Privacy tables with Glue Jobs
 - Filter protected PII with Spark in Glue Jobs

Curated Zone :
 - Write a Glue Job to join trusted data
 - Write a Glue Job to create curated data


## Python Scripts

The python scripts in this repository were exported from AWS Glue Studio.

## SQL Queries

SQL DDL scripts customer_landing.sql and accelerometer_landing.sql were also provided

## Snapshots

Snapshots of the queries were provided to demonstrate that the AWS Glue ETL were properly implemented. The tables created were queried via Athena.