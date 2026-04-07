-- ============================================================
-- Healthcare Analytics Database Schema
-- Author: Lohith Movva
-- Description: Schema for EHR, claims, and encounter data
--              supporting the analytics dashboard.
-- ============================================================

-- ------------------------------------------------------------
-- Dimension: Patients
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_patients (
    patient_id          VARCHAR(10)     PRIMARY KEY,
    age                 INTEGER         NOT NULL CHECK (age BETWEEN 0 AND 130),
    gender              VARCHAR(10)     NOT NULL,
    primary_condition   VARCHAR(50),
    payer_type          VARCHAR(30),
    enrollment_date     DATE,
    age_group           VARCHAR(10),
    is_chronic          BOOLEAN         DEFAULT FALSE,
    created_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

-- ------------------------------------------------------------
-- Fact: Claims
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_claims (
    claim_id            VARCHAR(12)     PRIMARY KEY,
    patient_id          VARCHAR(10)     REFERENCES dim_patients(patient_id),
    claim_type          VARCHAR(20)     NOT NULL,
    claim_amount        NUMERIC(12, 2)  NOT NULL,
    approved_amount     NUMERIC(12, 2),
    approved            BOOLEAN         DEFAULT TRUE,
    claim_date          DATE            NOT NULL,
    month               VARCHAR(7),
    diagnosis_code      VARCHAR(10),
    denial_rate         SMALLINT        DEFAULT 0,
    net_amount          NUMERIC(12, 2),
    created_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_claims_patient  ON fact_claims(patient_id);
CREATE INDEX IF NOT EXISTS idx_claims_date     ON fact_claims(claim_date);
CREATE INDEX IF NOT EXISTS idx_claims_type     ON fact_claims(claim_type);
CREATE INDEX IF NOT EXISTS idx_claims_month    ON fact_claims(month);

-- ------------------------------------------------------------
-- Fact: Encounters
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_encounters (
    encounter_id            VARCHAR(12)     PRIMARY KEY,
    patient_id              VARCHAR(10)     REFERENCES dim_patients(patient_id),
    admit_date              DATE            NOT NULL,
    length_of_stay          INTEGER         NOT NULL CHECK (length_of_stay >= 0),
    readmitted_30d          BOOLEAN         DEFAULT FALSE,
    discharge_disposition   VARCHAR(20),
    department              VARCHAR(50),
    cost_per_encounter      NUMERIC(12, 2),
    month                   VARCHAR(7),
    high_los                BOOLEAN         DEFAULT FALSE,
    created_at              TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_enc_patient  ON fact_encounters(patient_id);
CREATE INDEX IF NOT EXISTS idx_enc_date     ON fact_encounters(admit_date);
CREATE INDEX IF NOT EXISTS idx_enc_dept     ON fact_encounters(department);
CREATE INDEX IF NOT EXISTS idx_enc_month    ON fact_encounters(month);

-- ------------------------------------------------------------
-- Summary: Monthly KPIs
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS kpi_monthly_summary (
    month                   VARCHAR(7)      PRIMARY KEY,
    total_claims            INTEGER,
    total_billed            NUMERIC(14, 2),
    total_approved          NUMERIC(14, 2),
    denial_count            INTEGER,
    total_encounters        INTEGER,
    avg_los                 NUMERIC(5, 2),
    readmission_count       INTEGER,
    avg_cost_per_encounter  NUMERIC(12, 2),
    readmission_rate        NUMERIC(6, 4),
    denial_rate_pct         NUMERIC(6, 4),
    updated_at              TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================
-- Analytical Views
-- ============================================================

-- Monthly denial rate trend
CREATE OR REPLACE VIEW vw_denial_trend AS
SELECT
    month,
    total_claims,
    denial_count,
    ROUND(denial_rate_pct * 100, 2) AS denial_rate_pct,
    total_billed,
    total_approved,
    ROUND(total_billed - total_approved, 2) AS revenue_gap
FROM kpi_monthly_summary
ORDER BY month;


-- Readmission rate by department
CREATE OR REPLACE VIEW vw_readmission_by_dept AS
SELECT
    department,
    COUNT(*)                                            AS total_encounters,
    SUM(readmitted_30d::int)                            AS readmissions,
    ROUND(AVG(readmitted_30d::int) * 100, 2)            AS readmission_rate_pct,
    ROUND(AVG(length_of_stay), 2)                       AS avg_los,
    ROUND(AVG(cost_per_encounter), 2)                   AS avg_cost
FROM fact_encounters
GROUP BY department
ORDER BY readmission_rate_pct DESC;


-- Patient engagement by payer type
CREATE OR REPLACE VIEW vw_engagement_by_payer AS
SELECT
    p.payer_type,
    COUNT(DISTINCT e.patient_id)                        AS unique_patients,
    COUNT(e.encounter_id)                               AS total_encounters,
    ROUND(AVG(e.length_of_stay), 2)                     AS avg_los,
    ROUND(SUM(e.cost_per_encounter), 2)                 AS total_cost,
    ROUND(AVG(e.cost_per_encounter), 2)                 AS avg_cost_per_encounter,
    SUM(e.readmitted_30d::int)                          AS total_readmissions
FROM fact_encounters e
JOIN dim_patients p ON e.patient_id = p.patient_id
GROUP BY p.payer_type
ORDER BY total_cost DESC;


-- High-cost encounters with patient context
CREATE OR REPLACE VIEW vw_high_cost_encounters AS
SELECT
    e.encounter_id,
    e.patient_id,
    p.age,
    p.age_group,
    p.primary_condition,
    p.payer_type,
    e.department,
    e.admit_date,
    e.length_of_stay,
    e.cost_per_encounter,
    e.readmitted_30d
FROM fact_encounters e
JOIN dim_patients p ON e.patient_id = p.patient_id
WHERE e.cost_per_encounter > (
    SELECT PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY cost_per_encounter)
    FROM fact_encounters
)
ORDER BY e.cost_per_encounter DESC;


-- ============================================================
-- Sample analytical queries used in dashboard
-- ============================================================

-- KPI 1: Overall readmission rate
-- SELECT ROUND(AVG(readmitted_30d::int) * 100, 2) AS readmission_rate_pct FROM fact_encounters;

-- KPI 2: Average length of stay
-- SELECT ROUND(AVG(length_of_stay), 2) AS avg_los FROM fact_encounters;

-- KPI 3: Average cost per encounter
-- SELECT ROUND(AVG(cost_per_encounter), 2) AS avg_cost FROM fact_encounters;

-- KPI 4: Claims denial rate
-- SELECT ROUND(SUM(denial_rate)::numeric / COUNT(*) * 100, 2) AS denial_rate_pct FROM fact_claims;

-- KPI 5: Top diagnosis codes by claim volume
-- SELECT diagnosis_code, COUNT(*) AS claim_count, ROUND(AVG(claim_amount),2) AS avg_amount
-- FROM fact_claims GROUP BY diagnosis_code ORDER BY claim_count DESC LIMIT 10;

-- KPI 6: Monthly encounter trend with rolling avg LOS
-- SELECT month,
--        total_encounters,
--        avg_los,
--        ROUND(AVG(avg_los) OVER (ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS rolling_avg_los
-- FROM kpi_monthly_summary ORDER BY month;
