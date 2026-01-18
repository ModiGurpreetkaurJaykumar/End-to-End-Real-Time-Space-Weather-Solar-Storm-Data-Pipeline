# End-to-End-Real-Time-Space-Weather-Solar-Storm-Data-Pipeline

## Project Description
This project implements a distributed, end-to-end big data pipeline for real-time space weather monitoring. Using live telemetry from the **NOAA Space Weather Prediction Center (SWPC)**, the system tracks solar wind speed, proton density, and magnetic field components to provide insights into conditions that could impact satellite communications and power grids. The architecture follows a **Medallion Architecture** (Bronze, Silver, and Gold layers) to ensure high data quality and separation between raw ingestion and analytical insights.

## Cluster Setup Instructions

### List of Nodes and Roles
The pipeline is deployed on a **2-node distributed cluster**.

| Node | IP Address | Role | Services Running |

| **Master** | 10.0.0.97 | Master | HDFS NameNode, SecondaryNameNode, YARN ResourceManager, Spark Master, Kafka Broker, Zookeeper, InfluxDB, Grafana |

| **Worker 1** | 10.0.0.95 | Worker | HDFS DataNode, YARN NodeManager, Spark Worker |

### How to Start Services
Execute these commands on the **Master Node** in the following order to initialize the environment:

1.  **Hadoop/HDFS:**
    start-dfs.sh && start-yarn.sh
    
2.  **Kafka (Zookeeper first):**
    bin/zookeeper-server-start.sh config/zookeeper.properties &
    bin/kafka-server-start.sh config/server.properties &

3.  **InfluxDB & Grafana:**
    sudo systemctl start influxdb
    sudo systemctl start grafana-server
   
## How to Run
Follow these steps to execute the data pipeline jobs:

## 1. Initialize Kafka Topic
Create the topic for raw data ingestion:
bin/kafka-topics.sh --create --topic solar-wind-raw --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

## 2. Start the Data Producer
Run the Python script to fetch live data from the NOAA API and push it to Kafka:
python3 solar_wind_producer.py

## 3. Submit Spark Jobs
Execute the processing pipeline through YARN:

Ingest to HDFS (Bronze): spark-submit kafka_to_hdfs.py 

Clean and Structure (Silver): spark-submit hdfs_to_silver.py

Export to InfluxDB (Gold): spark-submit silver_to_influx.py 

## Dependencies
Required software versions for the project environment:

Java: OpenJDK 8 or 11 

Hadoop: 3.3.x 

Spark: 3.x (Structured Streaming enabled) 

Kafka: 2.13-3.6.0 

InfluxDB: 2.x 

Python: 3.9+ (Libraries: kafka-python, requests, pyspark) 

## Monitoring
The system provides several tools for real-time monitoring and resource management:

Grafana Dashboard: Access at http://10.0.0.97:3000. Visualizes solar wind speed, density, temperature, and "Speed vs Density" correlations.

Hadoop NameNode: Access at http://10.0.0.97:9870 to verify HDFS metadata and storage health.

YARN ResourceManager: Access at http://10.0.0.97:8088 to monitor Spark job execution status.

InfluxDB Explorer: Access at http://10.0.0.97:8086 to query time-series metrics directly.
