DECLARE run_date   DATE DEFAULT CURRENT_DATE();
DECLARE start_date DATE DEFAULT DATE_SUB(run_date, INTERVAL 4 DAY);

-- 1) apaga partições do intervalo
DELETE FROM `single-healer-473019-e6.data_etl.tb_ga_events`
WHERE event_date BETWEEN start_date AND run_date;

-- 2) recarrega do bruto (GA4 export padrão: events_*)
INSERT INTO `single-healer-473019-e6.data_etl.tb_ga_events` (
  event_date, event_ts, event_name, user_pseudo_id, user_id,
  source, medium, campaign, utm_source, utm_medium, utm_campaign,
  transaction_id, value, currency, _ingestion_ts
)
WITH raw AS (
  SELECT *
  FROM `single-healer-473019-e6.data_raw.events_*`
  WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', start_date)
                          AND FORMAT_DATE('%Y%m%d', run_date)
),
base AS (
  SELECT 
    CASE
      WHEN SAFE_CAST(event_date AS DATE) IS NOT NULL THEN SAFE_CAST(event_date AS DATE)
      ELSE PARSE_DATE('%Y%m%d', CAST(event_date AS STRING))
    END AS event_date,
    TIMESTAMP_MICROS(event_timestamp) AS event_ts,
    event_name,
    user_pseudo_id,
    user_id,
    COALESCE(traffic_source.source,
             (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key='source'   LIMIT 1)) AS source,
    COALESCE(traffic_source.medium,
             (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key='medium'   LIMIT 1)) AS medium,
    COALESCE(traffic_source.name,
             (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key='campaign' LIMIT 1)) AS campaign,
    (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key='utm_source'   LIMIT 1) AS utm_source,
    (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key='utm_medium'   LIMIT 1) AS utm_medium,
    (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key='utm_campaign' LIMIT 1) AS utm_campaign,
    (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key='transaction_id' LIMIT 1) AS transaction_id,
    COALESCE(
      (SELECT ep.value.double_value FROM UNNEST(event_params) ep WHERE ep.key='value' LIMIT 1),
      SAFE_CAST((SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key='value' LIMIT 1) AS FLOAT64)
    ) AS value,
    (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key='currency' LIMIT 1) AS currency,
    CURRENT_TIMESTAMP() AS _ingestion_ts
  FROM raw
)
SELECT *
FROM base
WHERE event_date BETWEEN start_date AND run_date;
