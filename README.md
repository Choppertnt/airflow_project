# Docker Airflow, PySpark, GCP, BigQuery

## Objective
This project demonstrates the use of Apache Airflow for orchestration, PySpark for data cleaning and validation, data extraction from Google Cloud Platform (GCP), uploading data to BigQuery, and presenting the data through Power BI. The goal is to showcase the integration of these technologies to manage and analyze data effectively.

## Features
- **PySpark 3.5.1**: Utilized for scalable data processing and transformation.
- **Airflow 2.9.3**: Employed for workflow orchestration and task management.
- **Docker Desktop v4.33.1**: Used to containerize and manage the application environment.

## Components
1. **Apache Airflow**:
   - Used to schedule and monitor workflows.
   - Configured with Docker to ensure consistency across development environments.

2. **PySpark**:
   - Handles data cleaning and validation tasks.
   - Integrated with Airflow to automate data processing pipelines.

3. **GCP (Google Cloud Platform)**:
   - Used for data storage and management.
   - Data is extracted from GCP buckets and processed with PySpark.

4. **BigQuery**:
   - Data is uploaded to BigQuery for advanced analytics and querying.

5. **Power BI**:
   - Data visualizations and dashboards are created to present insights derived from the processed data.

## Results project
![Airflow Diagram](project_airflows/workflow.png)