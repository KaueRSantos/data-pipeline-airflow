CREATE OR REPLACE VIEW `single-healer-473019-e6.data_delivery.VW_TIME_LEAD_TO_CONVERSION` AS 
WITH
params AS (
  SELECT
    DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) AS start_date,
    CURRENT_DATE()                            AS end_date,
    DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY), INTERVAL 365 DAY) AS touch_start
),
cap AS (
  SELECT
    business_key,
    COALESCE(NULLIF(TRIM(brand_name),''), 'Unknown') AS brand_name,
    inscricao_ts,
    inscricao_dt,
    email_hash,
    phone_hash
  FROM `single-healer-473019-e6.data_etl.tb_captacao`
  WHERE inscricao_dt BETWEEN (SELECT start_date FROM params)
                        AND (SELECT end_date   FROM params)
),
first_contact AS (
  SELECT
    c.business_key,
    c.brand_name,
    c.inscricao_ts,
    MIN(l.dt_lead) AS first_contact_ts
  FROM cap c
  JOIN `single-healer-473019-e6.data_etl.tb_leads_captacao_etl` l
    ON (l.email_hash = c.email_hash OR l.phone_hash = c.phone_hash)
  WHERE l.dt_lead_dt BETWEEN (SELECT touch_start FROM params) AND (SELECT end_date FROM params)
    AND l.dt_lead <= c.inscricao_ts
  GROUP BY c.business_key, c.brand_name, c.inscricao_ts
),
diffs AS (
  SELECT
    fc.brand_name,
    fc.business_key,
    fc.first_contact_ts,
    fc.inscricao_ts,
    -- horas (float): diferença em segundos / 3600.0
    TIMESTAMP_DIFF(fc.inscricao_ts, fc.first_contact_ts, SECOND) / 3600.0 AS hours_lead_to_inscricao
  FROM first_contact fc
  WHERE fc.first_contact_ts IS NOT NULL
)
SELECT
  brand_name,
  COUNT(*) AS converted_leads,
  ROUND(AVG(hours_lead_to_inscricao), 2) AS avg_hours_lead_to_inscricao,
  ROUND(APPROX_QUANTILES(hours_lead_to_inscricao, 100)[OFFSET(50)], 2) AS median_hours_lead_to_inscricao
FROM diffs
GROUP BY brand_name

UNION ALL

SELECT
  'Others' AS brand_name,
  COUNT(*) AS converted_leads,
  ROUND(AVG(hours_lead_to_inscricao), 2),
  ROUND(APPROX_QUANTILES(hours_lead_to_inscricao, 100)[OFFSET(50)], 2)
FROM diffs