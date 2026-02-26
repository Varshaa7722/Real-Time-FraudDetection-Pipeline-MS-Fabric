# Real-Time-FraudDetection-Pipeline-MS-Fabric
This project demonstrates an end-to-end Real-Time Fraud Detection System built on Microsoft Fabric. It utilizes a Python-based synthetic data generator to simulate global credit card transactions, which are then processed through a Medallion Architecture (Bronze $\rightarrow$ Silver $\rightarrow$ Gold) using PySpark and Fabric Eventstreams.

## 🏗️ Architecture & Data Flow
The pipeline processes data through three distinct layers to ensure data quality and analytical readiness:

1) Bronze (Raw): Ingestion of high-velocity JSON transaction data from Azure Event Hubs into Delta tables.
2) Silver (Enriched): Stateful feature engineering using Spark Window functions to capture behavioral anomalies (Velocity, Geo-shifts, and Deviations).
3) Gold (Curated): Final risk scoring, merchant profiling, and alert generation for downstream BI and automated intervention.

## ⚙️ Technologies Used
Orchestration & Storage: Microsoft Fabric (OneLake & Lakehouse)
Processing Engine: PySpark (Spark SQL)
Data Format: Delta Lake (Acid compliant)
Ingestion: Fabric Eventstreams & Azure Event Hubs
Simulation: Python (Faker, PyCountry)

## Feature Engineering Logic (Silver Layer)
The Silver layer (silver_table_lh) performs complex transformations to define "normal" user behavior:
Transaction Velocity: Calculates txn_count_1min_per_user using a 60-second rolling window.
Spending Baselines: Establishes avg_amount_24h to identify sudden spikes in purchasing power.
Amount Deviation: Measures how much the current transaction differs from the user's 24-hour mean.
Geo-Anomaly Detection: Uses lag() functions to identify geo_change_flag when a user's location jumps between consecutive transactions.
Merchant Profiling: Computes a merchant_risk_score based on historical fraud-to-transaction ratios.

##  Risk Metrics & Scoring (Gold Layer)
The Gold layer (gold_table_lh) aggregates Silver features into a weighted risk model:The Risk Score FormulaThe system calculates a user_risk_score (0-100) based on weighted impact factors:
Score = (FraudRate * 0.5) + (Velocity * 0.1) + (Deviation * 0.1) + (Geo * 0.1) + (Merchant * 0.2)

Key Metrics Tracked
To evaluate the health and accuracy of the pipeline, we track the following:
1) User Risk Score: A 0–100 probability index of fraudulent intent.
2) Fraud Spike Detection: A logic-gate that flags users whose fraud rate is > 3x the global baseline.
3) Alert Rate: The percentage of transactions triggering alert_flag = 1 (Risk Score > 80).
4) Merchant Risk Average: A rounded (5 decimal) metric to identify high-risk nodes in the payment network.
5) Geo-Alert Frequency: Total count of suspicious location changes per user session.


## Project Structure
 ### Setup & Execution
Generate Data: Run syntheticdata-generator.py to begin streaming simulated transactions to Azure Event Hubs.
Ingest: Configure a Fabric Eventstream to sink the data into your Lakehouse bronze_table.
Process Silver: Execute the Silver Notebook to generate behavioral features.
Process Gold: Execute the Gold Notebook to generate final scores and alerts.
Monitor: View the final gold_table_lh for real-time fraud signals.

## Future Enhancements
1) Executive Dashboard: Building a Power BI Real-Time Dashboard (Direct Lake mode) to visualize fraud heatmaps, top-risk users, and merchant risk trends.
2) Real-time Streaming: Migrating from batch-intervals to Structured Streaming for sub-second latency.
3) ML Integration: Replacing weighted scoring with a Scikit-Learn or SynapseML Random Forest model for dynamic pattern recognition.
4) Notification Service: Integrating Fabric Data Activator to trigger automated emails or Teams alerts whenever alert_flag = 1.

## Author
Varshaa T
