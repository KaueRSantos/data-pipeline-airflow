-- ===== Reprocesso D-3 até D0 para data_etl.tb_leads_captacao_etl =====
DECLARE run_date   DATE DEFAULT CURRENT_DATE();
DECLARE start_date DATE DEFAULT DATE_SUB(run_date, INTERVAL 3 DAY);


DELETE FROM `single-healer-473019-e6.data_etl.tb_leads_captacao_etl`
WHERE dt_lead_dt BETWEEN start_date AND run_date;


INSERT INTO `single-healer-473019-e6.data_etl.tb_leads_captacao_etl` (
  lead_sk,
  complete_name, name_clean, first_name, last_name,
  email_raw, email_normalized, email_is_valid, email_hash,
  phone_raw, phone_digits, phone_e164, phone_hash,
  dt_lead, dt_lead_dt, _ingestion_ts
)
WITH src AS (
  SELECT
    TRIM(nome) AS complete_name,
    INITCAP(LOWER(REGEXP_REPLACE(TRIM(nome), r'\s+', ' '))) AS name_clean,
    REGEXP_EXTRACT(REGEXP_REPLACE(TRIM(nome), r'\s+', ' '), r'^\S+')  AS first_name,
    REGEXP_EXTRACT(REGEXP_REPLACE(TRIM(nome), r'\s+', ' '), r'(\S+)$') AS last_name,
    TRIM(email)            AS email_raw,
    LOWER(TRIM(email))     AS email_normalized,
    REGEXP_CONTAINS(
      LOWER(TRIM(email)),
      r'^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$'
    ) AS email_is_valid,
    TRIM(celular) AS phone_raw,
    REGEXP_REPLACE(COALESCE(celular,''), r'\D', '') AS phone_digits,
    CASE
      WHEN REGEXP_CONTAINS(REGEXP_REPLACE(COALESCE(celular,''), r'\D',''), r'^55\d{10,11}$')
        THEN CONCAT('+', REGEXP_REPLACE(COALESCE(celular,''), r'\D',''))
      WHEN REGEXP_CONTAINS(REGEXP_REPLACE(COALESCE(celular,''), r'\D',''), r'^\d{10,11}$')
        THEN CONCAT('+55', REGEXP_REPLACE(COALESCE(celular,''), r'\D',''))
      ELSE NULL
    END AS phone_e164,

    TO_HEX(SHA256(LOWER(TRIM(COALESCE(email,'')))))                AS email_hash,
    TO_HEX(SHA256(REGEXP_REPLACE(COALESCE(celular,''), r'\D',''))) AS phone_hash,
    dt_lead,
    DATE(dt_lead) AS dt_lead_dt,

    CURRENT_TIMESTAMP() AS _ingestion_ts
  FROM `single-healer-473019-e6.data_etl.tb_leads_captacao`
  WHERE dt_lead IS NOT NULL
    AND DATE(dt_lead) BETWEEN start_date AND run_date
)
SELECT
  TO_HEX(SHA256(CONCAT(
    COALESCE(name_clean,''),'|',
    COALESCE(email_normalized,''),'|',
    COALESCE(phone_digits,'')
  ))) AS lead_sk,
  complete_name, name_clean, first_name, last_name,
  email_raw, email_normalized, email_is_valid, email_hash,
  phone_raw, phone_digits, phone_e164, phone_hash,
  dt_lead, dt_lead_dt, _ingestion_ts
FROM src;
