# EthioMed-Hub

An end-to-end data pipeline for the Ethiopian medical sector, leveraging public Telegram data. This project extracts raw data, processes it with a modern ELT framework using dbt and Dagster, enriches it with YOLOv8, and serves insights via a FastAPI application.

---

## 🚀 1. Overview

In Ethiopia’s digital marketplace, Telegram channels are key sources for medical product data. **EthioMed-Hub** transforms this noisy, unstructured data into structured, queryable insights.

### 🔍 Business Questions Answered
- What are the most frequently mentioned medical products?
- How does product availability vary across channels?
- What are the daily/weekly posting trends?

---

## 🏗 2. System Architecture

The platform follows a layered data architecture for scalability and reproducibility. Data flows through:

1. **Extract & Load to Data Lake**  
   - Python + Telethon scrape messages & images to local storage.

2. **Load to Data Warehouse**  
   - A Python loader script ingests raw JSON into PostgreSQL.

3. **Transform & Model**  
   - dbt builds clean staging and star schema marts.

4. **Enrich with AI**  
   - YOLOv8 performs object detection on images; results stored in warehouse.

5. **Serve & Orchestrate**  
   - FastAPI exposes analytical endpoints. Dagster manages orchestration.

---

## 🌟 3. Key Features

- 🔄 **Automated Data Scraping** from Telegram via Telethon  
- 🧪 **Modern ELT** with dbt and PostgreSQL  
- 📊 **Star Schema** dimensional model for analytics  
- 🧠 **AI Enrichment** using YOLOv8 for image detection  
- 🌐 **REST API** served via FastAPI  
- 🧭 **Orchestration** with Dagster  
- 🐳 **Dockerized** environment for reproducibility

---

## 🧰 4. Tech Stack

| Component         | Technology                |
|-------------------|--------------------------|
| Containerization  | Docker, Docker Compose   |
| Data Extraction   | Python, Telethon         |
| Data Warehouse    | PostgreSQL               |
| Transformation    | dbt                      |
| Enrichment        | YOLOv8, PyTorch          |
| API Layer         | FastAPI                  |
| Orchestration     | Dagster                  |

---

## ⚙️ 5. Getting Started

### Prerequisites
- Git
- Docker Desktop (installed & running)

### Setup

```bash
git clone https://github.com/Emnet-tes/EthioMed-Hub.git
cd EthioMed-Hub
cp .env.example .env
```

Edit the `.env` file and add:
- `TELEGRAM_API_ID` and `TELEGRAM_API_HASH` from [my.telegram.org](https://my.telegram.org)
- PostgreSQL credentials (pre-filled for Docker)

### Start the Stack

```bash
docker-compose up --build -d
```

This launches:
- PostgreSQL
- Dagster UI
- FastAPI (dev server)

---

## 🔁 6. Using the Pipeline

### 🧭 Dagster UI

Open: [http://localhost:3000](http://localhost:3000)  
- Trigger jobs manually
- Monitor pipeline execution

### 🧪 Run Individual Scripts

Run scraper manually:

```bash
docker-compose exec app python src/scraping/scraper.py
```

Start FastAPI server:

```bash
docker-compose exec app uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
```

Docs available at: [http://localhost:8000/docs](http://localhost:8000/docs)

---

## 🗂 7. Project Structure

```
EthioMed-Hub/
├── api/                    # FastAPI app (main.py, routers, CRUD, etc.)
├── data/                   # Local data lake (excluded from Git)
├── dagster_pipeline/       # Dagster jobs, ops, schedules
├── telegram_analytics/     # dbt project
│   ├── models/
│   │   ├── staging/        # Light-cleaned staging models
│   │   └── marts/          # Star schema mart models
│   ├── packages.yml        # dbt packages
│   └── dbt_project.yml     # dbt config
├── scripts/                # Python ETL/enrichment scripts
│   ├── enrichment/         # YOLOv8 inference
│   ├── loading/            # Raw → PostgreSQL loader
│   └── scraping/           # Telegram scraper
├── tests/                  # Unit/integration test placeholders
├── .env.example            # Sample environment config
├── .gitignore              # Git ignore rules
├── docker-compose.yml      # Container orchestration
├── Dockerfile              # Python app Dockerfile
└── requirements.txt        # Python dependencies
```

---

## 📬 Contact

For questions or contributions, reach out via [GitHub Issues](https://github.com/Emnet-tes/EthioMed-Hub/issues).