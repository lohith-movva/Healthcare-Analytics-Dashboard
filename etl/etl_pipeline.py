"""
Healthcare Analytics ETL Pipeline
----------------------------------
Pulls data from simulated EHR, claims, and operational sources,
cleans and transforms it, and loads it into PostgreSQL for reporting.

Author: Lohith Movva
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import random
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config — override via environment variables in production
# ---------------------------------------------------------------------------
DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:password@localhost:5432/healthcare_analytics"
)
RANDOM_SEED = 42
NUM_PATIENTS = 500
NUM_CLAIMS = 2000
NUM_ENCOUNTERS = 1500

random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)


# ---------------------------------------------------------------------------
# 1. Extract — generate / load raw data
# ---------------------------------------------------------------------------

def extract_patients() -> pd.DataFrame:
    """Simulate EHR patient records."""
    logger.info("Extracting patient data...")
    ages = np.random.randint(18, 90, NUM_PATIENTS)
    genders = np.random.choice(["Male", "Female", "Other"], NUM_PATIENTS, p=[0.48, 0.49, 0.03])
    conditions = np.random.choice(
        ["Diabetes", "Hypertension", "CHF", "COPD", "None"],
        NUM_PATIENTS, p=[0.2, 0.25, 0.1, 0.1, 0.35]
    )
    payers = np.random.choice(
        ["Medicare", "Medicaid", "Commercial", "Self-Pay"],
        NUM_PATIENTS, p=[0.35, 0.2, 0.35, 0.1]
    )
    return pd.DataFrame({
        "patient_id": [f"P{str(i).zfill(5)}" for i in range(1, NUM_PATIENTS + 1)],
        "age": ages,
        "gender": genders,
        "primary_condition": conditions,
        "payer_type": payers,
        "enrollment_date": [
            (datetime(2022, 1, 1) + timedelta(days=random.randint(0, 700))).date()
            for _ in range(NUM_PATIENTS)
        ],
    })


def extract_claims() -> pd.DataFrame:
    """Simulate insurance claims data."""
    logger.info("Extracting claims data...")
    patient_ids = [f"P{str(i).zfill(5)}" for i in range(1, NUM_PATIENTS + 1)]
    claim_types = np.random.choice(
        ["Inpatient", "Outpatient", "ER", "Pharmacy", "Lab"],
        NUM_CLAIMS, p=[0.15, 0.4, 0.2, 0.15, 0.1]
    )
    amounts = np.round(np.random.lognormal(mean=6.5, sigma=1.2, size=NUM_CLAIMS), 2)
    approved = np.random.choice([True, False], NUM_CLAIMS, p=[0.88, 0.12])
    return pd.DataFrame({
        "claim_id": [f"CLM{str(i).zfill(6)}" for i in range(1, NUM_CLAIMS + 1)],
        "patient_id": np.random.choice(patient_ids, NUM_CLAIMS),
        "claim_type": claim_types,
        "claim_amount": amounts,
        "approved_amount": np.where(approved, amounts * np.random.uniform(0.7, 1.0, NUM_CLAIMS), 0).round(2),
        "approved": approved,
        "claim_date": [
            (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 365))).date()
            for _ in range(NUM_CLAIMS)
        ],
        "diagnosis_code": np.random.choice(
            ["E11.9", "I10", "I50.9", "J44.1", "Z00.00"], NUM_CLAIMS
        ),
    })


def extract_encounters() -> pd.DataFrame:
    """Simulate hospital encounter / visit records."""
    logger.info("Extracting encounter data...")
    patient_ids = [f"P{str(i).zfill(5)}" for i in range(1, NUM_PATIENTS + 1)]
    los = np.random.choice([1, 2, 3, 4, 5, 7, 10, 14], NUM_ENCOUNTERS,
                            p=[0.3, 0.25, 0.2, 0.1, 0.07, 0.04, 0.03, 0.01])
    readmitted = np.random.choice([True, False], NUM_ENCOUNTERS, p=[0.18, 0.82])
    return pd.DataFrame({
        "encounter_id": [f"ENC{str(i).zfill(6)}" for i in range(1, NUM_ENCOUNTERS + 1)],
        "patient_id": np.random.choice(patient_ids, NUM_ENCOUNTERS),
        "admit_date": [
            (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 365))).date()
            for _ in range(NUM_ENCOUNTERS)
        ],
        "length_of_stay": los,
        "readmitted_30d": readmitted,
        "discharge_disposition": np.random.choice(
            ["Home", "SNF", "Rehab", "AMA", "Expired"],
            NUM_ENCOUNTERS, p=[0.6, 0.2, 0.1, 0.05, 0.05]
        ),
        "department": np.random.choice(
            ["Cardiology", "General Medicine", "Orthopedics", "Oncology", "Emergency"],
            NUM_ENCOUNTERS
        ),
        "cost_per_encounter": np.round(np.random.lognormal(7.5, 1.0, NUM_ENCOUNTERS), 2),
    })


# ---------------------------------------------------------------------------
# 2. Transform — clean and enrich
# ---------------------------------------------------------------------------

def transform_patients(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Transforming patient data...")
    df = df.drop_duplicates(subset="patient_id")
    df["age_group"] = pd.cut(
        df["age"],
        bins=[0, 30, 45, 60, 75, 120],
        labels=["18-30", "31-45", "46-60", "61-75", "75+"]
    )
    df["is_chronic"] = df["primary_condition"].isin(["Diabetes", "Hypertension", "CHF", "COPD"])
    return df


def transform_claims(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Transforming claims data...")
    df = df.drop_duplicates(subset="claim_id")
    df["claim_date"] = pd.to_datetime(df["claim_date"])
    df["month"] = df["claim_date"].dt.to_period("M").astype(str)
    df["denial_rate"] = (~df["approved"]).astype(int)
    df["net_amount"] = df["approved_amount"]
    return df


def transform_encounters(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Transforming encounter data...")
    df = df.drop_duplicates(subset="encounter_id")
    df["admit_date"] = pd.to_datetime(df["admit_date"])
    df["month"] = df["admit_date"].dt.to_period("M").astype(str)
    df["high_los"] = df["length_of_stay"] >= 7
    return df


def build_summary(claims: pd.DataFrame, encounters: pd.DataFrame) -> pd.DataFrame:
    """Build monthly KPI summary table."""
    logger.info("Building KPI summary...")
    claim_monthly = claims.groupby("month").agg(
        total_claims=("claim_id", "count"),
        total_billed=("claim_amount", "sum"),
        total_approved=("approved_amount", "sum"),
        denial_count=("denial_rate", "sum"),
    ).reset_index()

    enc_monthly = encounters.groupby("month").agg(
        total_encounters=("encounter_id", "count"),
        avg_los=("length_of_stay", "mean"),
        readmission_count=("readmitted_30d", "sum"),
        avg_cost_per_encounter=("cost_per_encounter", "mean"),
    ).reset_index()

    summary = pd.merge(claim_monthly, enc_monthly, on="month", how="outer").fillna(0)
    summary["readmission_rate"] = (summary["readmission_count"] / summary["total_encounters"]).round(4)
    summary["denial_rate_pct"] = (summary["denial_count"] / summary["total_claims"]).round(4)
    return summary


# ---------------------------------------------------------------------------
# 3. Load — write to PostgreSQL
# ---------------------------------------------------------------------------

def load_to_postgres(dfs: dict, engine) -> None:
    logger.info("Loading data to PostgreSQL...")
    for table_name, df in dfs.items():
        df.to_sql(table_name, engine, if_exists="replace", index=False)
        logger.info(f"  Loaded {len(df):,} rows → {table_name}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_pipeline():
    logger.info("=" * 55)
    logger.info("Healthcare Analytics ETL Pipeline — starting")
    logger.info("=" * 55)

    # Extract
    patients_raw = extract_patients()
    claims_raw = extract_claims()
    encounters_raw = extract_encounters()

    # Transform
    patients = transform_patients(patients_raw)
    claims = transform_claims(claims_raw)
    encounters = transform_encounters(encounters_raw)
    summary = build_summary(claims, encounters)

    # Load
    try:
        engine = create_engine(DB_URL)
        load_to_postgres(
            {
                "dim_patients": patients,
                "fact_claims": claims,
                "fact_encounters": encounters,
                "kpi_monthly_summary": summary,
            },
            engine,
        )
        logger.info("Pipeline completed successfully.")
    except Exception as e:
        logger.error(f"Database load failed: {e}")
        logger.info("Saving outputs to CSV instead...")
        patients.to_csv("data/dim_patients.csv", index=False)
        claims.to_csv("data/fact_claims.csv", index=False)
        encounters.to_csv("data/fact_encounters.csv", index=False)
        summary.to_csv("data/kpi_monthly_summary.csv", index=False)
        logger.info("CSVs saved to data/ folder.")

    logger.info("=" * 55)


if __name__ == "__main__":
    run_pipeline()
