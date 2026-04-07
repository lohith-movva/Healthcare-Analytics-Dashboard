# Healthcare Analytics Dashboard & ETL Pipeline

![Python](https://img.shields.io/badge/Python-3.10+-blue?logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue?logo=postgresql&logoColor=white)
![Tableau](https://img.shields.io/badge/Tableau-Dashboard-orange?logo=tableau&logoColor=white)

End-to-end pipeline that pulls healthcare data from EHR, claims, and operational systems, loads it into PostgreSQL, and drives a Tableau dashboard with 10+ KPIs.

## What it does
- ETL pipeline ingests and cleans patient, claims, and encounter data
- Loads into a relational PostgreSQL database
- Tableau dashboard tracks readmission rate, avg LOS, cost per encounter, denial rate
- Automated Excel report replaces manual 3-day process with same-day output

## Tech Stack
Python · SQL · PostgreSQL · Tableau · Pandas · openpyxl · Jupyter

## How to run
```bash
pip install -r requirements.txt
python etl/etl_pipeline.py
python reports/generate_report.py
```

## Author
Lohith Movva — [LinkedIn](https://linkedin.com/in/lohith-movva)
