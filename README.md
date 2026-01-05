# Daily Sales Summary ETL Project

## **Overview**
This project implements a daily sales ETL pipeline using **Apache Airflow** and **PySpark**.  
The pipeline performs the following tasks:  
1. Extract and Transform - Runs the Spark job to process,transform and load sales data.
2. Validate - Validates the transformed output files for nulls and existence.
3. Load - Simulates loading the data into the target system (e.g., Snowflake) 

The project is containerized using Docker and deployed via Docker Compose, while Apache Airflow orchestrates the ETL workflow

---

## **Requirements**

- **Docker**: version 26.1.4  
- **Docker Compose**: version v2.27.1-desktop.1  
- **Python**: 3.9+ (inside container)  

---

## **Setup Instructions**

### **1. Clone the repository**

Run these commands on CLI:

git clone https://github.com/rimshakhan98/Daily_Sales_Summary.git

cd Daily_Sales_Summary

### **2. Start Services with Docker Compose**
bash
docker-compose up -d
This command will start all required services including Airflow & Postgres.

### **3. Access the Airflow UI**
Open your browser and navigate to:
http://localhost:8080

Username: admin

Password: admin

### **4. Run DAGs**
The DAG daily_sales_dag will be visible in the Airflow UI.

Trigger the DAG manually or wait for its scheduled daily run.

### **Notes**
Delta Lake tables are stored in the spark_job/output/ directory and partitioned appropriately.

Daily logs are created in the spark_job/logs/ folder for monitoring DAG runs.

Paths for CSV are defined in the spark_job/config/paths.json.

Spark configurations are defined in the spark_job/config/spark_config.json.

Ensure your local machine has sufficient resources to run Airflow and Spark containers.