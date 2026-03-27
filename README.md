<div align="center">

# Smart City Real-Time Data Engineering

[![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)](#)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python Version](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/docker-compose-2496ED.svg?logo=docker&logoColor=white)](#)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20.svg?logo=apachekafka&logoColor=white)](#)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C.svg?logo=apachespark&logoColor=white)](#)
[![AWS](https://img.shields.io/badge/AWS-232F3E.svg?logo=amazon-aws&logoColor=white)](#)

*A production-grade, end-to-end data engineering pipeline for streaming real-time IoT vehicle data through Apache Kafka and Spark to AWS cloud analytics.*

</div>

---

## Table of Contents
- [Overview](#overview)
- [Problem Statement](#problem-statement)
- [Dataset](#dataset)
- [Tools and Technologies](#tools-and-technologies)
- [Methods & Architecture](#methods--architecture)
- [Key Insights](#key-insights)
- [How to Run This Project](#how-to-run-this-project)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Execution](#execution)
- [Conclusion](#conclusion)

---

## Overview
This project implements a multi-layered, real-time data engineering pipeline designed to simulate, ingest, and process high-velocity data from a smart city IoT vehicle network. Targeting data engineers, cloud architects, and backend developers, the framework demonstrates a highly scalable approach to modern streaming architectures. The core features include multi-threaded Python simulated producers mapping a 10km urban route, real-time integration with the OpenWeather API, Apache Kafka message brokering, Spark Structured Streaming for schema enforcement and Parquet conversion, and an automated AWS cloud analytics backend utilizing S3, Glue, Athena, and Redshift.

## Problem Statement
Modern smart cities require robust, fault-tolerant infrastructure capable of instantly processing, analyzing, and storing millions of high-velocity IoT events. Connected vehicles generate a continuous stream of GPS coordinates, telemetry, weather conditions, and emergency alerts. The challenge lies in orchestrating a reliable data flow that guarantees low-latency ingestion while simultaneously structuring the data for complex historical analytics and intelligent traffic management. This project addresses this gap by providing a comprehensive, reproducible streaming blueprint.

## Dataset
This pipeline dynamically aggregates data from multiple disparate sources in real-time:
- **GPS Data**: A simulated 10km urban vehicle route through Birmingham (parsed from a `.gpx` file).
- **Weather Data**: Real-time localized weather conditions retrieved via the external OpenWeather API.
- **Telemetry & Emergency Data**: Procedurally generated engine metrics (RPM, fuel, temperature) and probabilistic test failure events synced with geographic proximity algorithms.

## Tools and Technologies
- **Languages**: Python 3.11+, SQL
- **Data Streaming & Processing**: Apache Kafka (3.5), Zookeeper, Apache Spark 3.5 (Structured Streaming)
- **Containerization**: Docker, Docker Compose
- **Cloud Infrastructure (AWS)**: Amazon S3, AWS Glue (Crawlers & Catalog), Amazon Athena, Amazon Redshift Spectrum

## Methods & Architecture
The system architecture follows a robust publish-subscribe pattern paired with a structured data lake approach:
1. **IoT Generation**: Multi-threaded Python producers (`main_producer.py`) continuously generate and synchronize JSON payloads, broadcasting to Kafka topics (`gps_data`, `weather_data`, etc.).
2. **Message Brokering**: A containerized Kafka broker orchestrates the high-throughput streams, ensuring decoupling between ingestion and processing.
3. **Stream Processing**: Apache Spark consumes the Kafka micro-batches, infers schemas, and transforms the raw JSON into optimized, partitioned Parquet format `(date, event_type)`.
4. **Cloud Analytics**: The Parquet files are written to Amazon S3. An automated Python script (`setup_glue_crawler.py`) triggers an AWS Glue Crawler to establish the Data Catalog. Finally, Amazon Athena and Redshift Spectrum query the partitioned data using serverless standard SQL.

<details>
<summary><strong>View Architecture Diagram</strong></summary>

```text
                        ┌─────────────────────────────────┐
                        │     IoT Vehicle Simulation      │
                        │                                 │
                        │  GPS  │ Weather │  Telemetry    │
                        └───────────────┬─────────────────┘
                                        │ JSON
                                        ▼
                        ┌────────────────────────────────┐
                        │       Apache Kafka (3.5)       │
                        │                                │
                        │  vehicle_data  │  gps_data     │
                        │  weather_data  │ telemetry_data│
                        │  emergency_data                │
                        └───────────────┬────────────────┘
                                        │ Structured Streaming
                                        ▼
                        ┌────────────────────────────────┐
                        │   Apache Spark 3.5 Cluster     │
                        │   (1 master + 2 workers)       │
                        │   Schema inference + Parquet   │
                        └───────────────┬────────────────┘
                                        │ Parquet / partitioned
                                        ▼
                        ┌────────────────────────────────┐
                        │         AWS S3 Bucket          │
                        │  smart-city/                   │
                        └──────┬─────────────────────────┘
                               │
                  ┌────────────┴──────────────┐
                  ▼                           ▼
     ┌────────────────────┐       ┌───────────────────────┐
     │   AWS Glue Crawler │       │   Amazon Athena       │
     │   → Glue Catalog   │──────▶│   (ad-hoc SQL)        │
     └────────────────────┘       └───────────────────────┘
```
</details>

## Key Insights
- **High-Throughput Ingestion**: Successfully orchestrated the concurrent, real-time ingestion of 5 distinct IoT event streams using heavily decoupled multi-threaded Python producers.
- **Optimized Data Storage**: Implemented time-and-event-based partitioning via Apache Spark, storing records as compressed Parquet files in S3. This architectural choice drastically minimizes required scan volumes and accelerates analytical queries.
- **Enterprise Cloud Integration**: Established a fully automated deployment of an AWS Glue Catalog data dictionary, demonstrating seamless interoperability between local Docker simulation and managed enterprise cloud data warehousing tools.

## How to Run This Project

### Prerequisites
- **Docker Desktop**: Version 24+ (Allocate at least 8 GB RAM and 4 CPU cores)
- **Python**: Version 3.11+
- **AWS Account**: Optional (Pipeline can run entirely locally without AWS credentials)
- **OpenWeather API Key**: Free tier available from [openweathermap.org](https://openweathermap.org/) (Pipeline falls back to simulated data if no key is provided)

### Installation

1. **Clone and Configure the Environment**
   ```bash
   git clone <your-repo-link>
   cd smart-city-streaming
   cp .env.example .env
   ```
2. **Setup Credentials**
   Open `.env` and fill in your API keys and AWS configuration:
   ```env
   OPENWEATHER_API_KEY=your_key_here
   AWS_ACCESS_KEY_ID=AKIA...
   AWS_SECRET_ACCESS_KEY=...
   AWS_DEFAULT_REGION=us-east-1
   S3_BUCKET_NAME=smart-city-streaming-bucket
   ```
3. **Install Python Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

### Execution

1. **Spin up the Docker Stack**
   Start Zookeeper, Kafka, and the Spark Cluster in detached mode:
   ```bash
   docker-compose up -d
   docker-compose ps
   ```

2. **Start the IoT Producers**
   Initialize the simulated vehicle data generation:
   ```bash
   python producers/main_producer.py
   ```

3. **Start the Spark Streaming Processor**
   In a separate terminal, execute the Spark job to consume Kafka streams:
   
   *To run locally (no Docker container needed):*
   ```bash
   spark-submit \
       --master local[*] \
       --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
       consumers/spark_processor.py
   ```
   *Note: Without AWS credentials, the processor will automatically write the Parquet outputs to `/output/smart-city/` locally.*

4. **Verify the Pipeline**
   Check for ingested Kafka messages:
   ```bash
   docker exec broker kafka-console-consumer \
     --bootstrap-server localhost:9092 \
     --topic gps_data --from-beginning --max-messages 5
   ```

5. **Cloud Deployment (Optional)**
   Trigger the AWS Glue Crawler to index your S3 bucket:
   ```bash
   python scripts/setup_glue_crawler.py
   ```

## Conclusion
This Smart City Data Pipeline serves as a comprehensive, production-ready blueprint for distributed real-time data engineering. It highlights the power of decoupling ingestion from processing using Kafka, paired with the analytical optimization of Spark and AWS. Future iterations could integrate Apache Flink for complex event processing (CEP), introduce real-time spatial dashboarding via Grafana, or embed machine learning modules for predictive telemetry anomaly detection over the stream.
