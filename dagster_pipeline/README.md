# Dagster Pipeline for Telegram Data Analytics

This directory contains the complete Dagster pipeline orchestration for the Telegram Data Analytics project.

## 📁 Structure

```
dagster_pipeline/
├── __init__.py              # Main Dagster definitions
├── assets/                  # Asset definitions
│   ├── telegram_data_assets.py  # Raw data ingestion
│   ├── dbt_assets.py           # dbt transformations
│   ├── yolo_assets.py          # YOLO object detection
│   └── fastapi_assets.py       # API service management
├── jobs/                    # Job definitions
│   └── __init__.py             # All pipeline jobs
├── schedules/               # Schedule definitions
│   └── __init__.py             # All pipeline schedules
└── resources/               # Resource definitions
    └── __init__.py             # Database and tool resources
```

## 🚀 Quick Start

1. **Install Dependencies**

   ```bash
   pip install dagster dagster-webserver dagster-postgres dagster-dbt
   ```

2. **Start the Pipeline**

   ```bash
   python scripts/start_dagster.py
   ```

3. **Access Web UI**
   - Open http://localhost:3000 in your browser
   - Explore assets, jobs, and schedules

## 📊 Assets

### Data Ingestion Assets

- **raw_telegram_messages**: Load raw telegram messages from database
- **telegram_images**: Process and catalog telegram images
- **telegram_data_quality**: Validate data quality and completeness

### dbt Transformation Assets

- **dbt_staging_models**: Clean and prepare raw data
- **dbt_dimension_tables**: Create dimension tables (users, channels, etc.)
- **dbt_fact_tables**: Create fact tables (messages, detections, etc.)
- **dbt_tests**: Run data quality tests

### YOLO Enrichment Assets

- **yolo_object_detection**: Run YOLO detection on images
- **yolo_dbt_models**: Transform detection results
- **yolo_data_quality**: Validate detection quality

### API Service Assets

- **fastapi_service**: Start/monitor FastAPI service
- **api_endpoint_tests**: Test all API endpoints
- **api_performance_metrics**: Monitor API performance

## 🔧 Jobs

### 1. **data_ingestion_job**

- Ingests raw telegram data
- Performs initial quality checks
- Runs hourly

### 2. **data_transformation_job**

- Transforms data using dbt
- Creates staging and mart tables
- Runs every 4 hours

### 3. **ml_enrichment_job**

- Runs YOLO object detection
- Enriches data with ML insights
- Runs every 6 hours

### 4. **api_service_job**

- Manages FastAPI service
- Tests endpoints and performance
- Runs every 30 minutes

### 5. **full_pipeline_job**

- Executes complete end-to-end pipeline
- Runs daily at 2 AM

### 6. **quality_checks_job**

- Runs all quality validations
- Runs every 2 hours

## ⏰ Schedules

| Schedule                  | Frequency        | Description                 |
| ------------------------- | ---------------- | --------------------------- |
| **daily_full_pipeline**   | Daily at 2 AM    | Complete pipeline execution |
| **hourly_data_ingestion** | Every hour       | Ingest new telegram data    |
| **data_transformation**   | Every 4 hours    | dbt transformations         |
| **ml_enrichment**         | Every 6 hours    | YOLO object detection       |
| **quality_checks**        | Every 2 hours    | Data quality validation     |
| **health_check**          | Every 15 minutes | System health monitoring    |
| **api_service**           | Every 30 minutes | API service monitoring      |
| **weekly_cleanup**        | Sunday at 3 AM   | Maintenance and cleanup     |

## 🔄 Pipeline Flow

```
Raw Data Ingestion → Data Quality Checks → dbt Transformations →
ML Enrichment → API Service → Performance Monitoring
```

## 📈 Monitoring

### Asset Materialization

- Track asset execution status
- Monitor data lineage
- View execution logs

### Job Execution

- Monitor scheduled job runs
- Track success/failure rates
- Alert on failures

### Data Quality

- Automated quality checks
- Data freshness monitoring
- Anomaly detection

## 🛠️ Development

### Running Individual Jobs

```bash
# From Dagster UI or programmatically
dagster job execute -j data_ingestion_job
```

### Testing Assets

```bash
# Run specific asset
dagster asset materialize --select raw_telegram_messages
```

### Debugging

- Check logs in Dagster UI
- Review asset metadata
- Monitor resource usage

## 📋 Prerequisites

1. **PostgreSQL Database**

   - Host: 127.0.0.1:5432
   - Database: kara_medical
   - User: postgres

2. **dbt Project**

   - Location: ./telegram_analytics/
   - Configured with PostgreSQL

3. **YOLO Model**

   - YOLOv8 installed via ultralytics
   - Model weights downloaded

4. **FastAPI Service**
   - API code in ./api/ directory
   - Configured endpoints

## 🔒 Security

- Database credentials in environment variables
- API keys securely managed
- Resource access controls

## 📚 Additional Resources

- [Dagster Documentation](https://docs.dagster.io/)
- [Asset Dependencies](https://docs.dagster.io/concepts/assets/dependencies)
- [Job Scheduling](https://docs.dagster.io/concepts/partitions-schedules-sensors/schedules)
- [Resource Configuration](https://docs.dagster.io/concepts/resources)
