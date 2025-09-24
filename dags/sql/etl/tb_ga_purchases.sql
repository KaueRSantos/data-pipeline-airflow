DECLARE run_date   DATE DEFAULT CURRENT_DATE();
DECLARE start_date DATE DEFAULT DATE_SUB(run_date, INTERVAL 4 DAY);

-- 1) apaga partições do intervalo
DELETE FROM `single-healer-473019-e6.data_etl.tb_ga_purchases`
WHERE event_date BETWEEN start_date AND run_date;

-- 2) recarrega a partir de tb_ga_events
INSERT INTO `single-healer-473019-e6.data_etl.tb_ga_purchases` (
  event_date, event_ts, event_name, user_pseudo_id, user_id,
  source, medium, campaign, utm_source, utm_medium, utm_campaign,
  transaction_id, value, currency, _ingestion_ts
)
SELECT
  event_date, event_ts, event_name, user_pseudo_id, user_id,
  source, medium, campaign, utm_source, utm_medium, utm_campaign,
  transaction_id, value, currency, CURRENT_TIMESTAMP() AS _ingestion_ts
FROM `single-healer-473019-e6.data_etl.tb_ga_events`
WHERE event_date BETWEEN start_date AND run_date
  AND event_name = 'purchase'
  AND transaction_id IS NOT NULL;
