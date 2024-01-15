# NCDC Weather Data Analysis

## Project Overview
This repository contains a comprehensive Big Data project focusing on the analysis of National Climatic Data Center (NCDC) weather records. The project is divided into four parts, each utilizing different big data technologies to analyze sky ceiling height and visibility distance from weather station data.

## Technologies Used
- Hadoop MapReduce
- PySpark
- Apache Pig
- Apache Hive

## Part 1: Sky Ceiling Height Range Analysis with MapReduce
- **Objective**: Calculate the range of sky ceiling height for each observation month.
- **Method**: Developed a MapReduce application in [programming language or tool used].
- **Data Handling**: The application filters out missing values (99999) and considers only good quality values ([01459]).

## Part 2: Average Visibility Distance Analysis with PySpark
- **Objective**: Compute the average visibility distance for each USAF weather station ID.
- **Method**: Implemented a Python application in PySpark.
- **Data Considerations**: Excludes missing values (999999) and uses quality values ([01459]).

## Part 3: Sky Ceiling Height Range with Apache Pig
- **Objective**: Determine the range of sky ceiling height for each USAF weather station ID.
- **Method**: Loaded and analyzed the text data using Apache Pig.

## Part 4: Average Sky Ceiling Height with Apache Hive
- **Objective**: Calculate the average sky ceiling height for each USAF weather station ID.
- **Method**: Data analysis performed using Apache Hive.

## Data Source
- National Climatic Data Center (NCDC) records.


