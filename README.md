# Step 1 — Load CSV into Postgres (Raw Tables)

You only run **main.py**.

## What you need
- Docker Desktop running
- Python 3.10+

## Files to know
- docker-compose.yml -> starts Postgres
- warehouse/init.sql -> creates tables automatically on first start
- data/saas_customer_data.csv -> your dataset
- main.py -> runs the load

## Run (Windows PowerShell)
### 1) Create venv + install deps
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2) Create .env
```powershell
copy .env.example .env
```

### 3) Start and load
```powershell
python main.py
```

## Verify (optional)
```powershell
docker exec -it saas_analytics_postgres psql -U analytics -d saas_analytics -c "\dt raw.*"
docker exec -it saas_analytics_postgres psql -U analytics -d saas_analytics -c "select count(*) from raw.raw_users;"
```

## Reset everything (if you changed init.sql)
⚠️ This deletes the database volume:
```powershell
docker compose down -v
docker compose up -d
```
