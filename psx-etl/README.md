# PSX ETL Pipeline

This project is a micro-batch ETL pipeline for PSX data, built using FastAPI and Docker.

## Setup

### 1. Clone the repo

`git clone <repo-url>`

### 2. Install dependencies

`pip install -r requirements.txt`

### 3. Run the app

`uvicorn services/extract.app:app --reload`
