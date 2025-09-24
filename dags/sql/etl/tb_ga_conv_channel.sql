-- ===== Reprocesso D-3..D0 =====
DECLARE run_date   DATE DEFAULT CURRENT_DATE();       
DECLARE start_date DATE DEFAULT DATE_SUB(run_date, INTERVAL 4 DAY);

DELETE FROM `single-healer-473019-e6.data_etl.tb_ga_conv_channel`
WHERE event_dt BETWEEN start_date AND run_date;


INSERT INTO `single-healer-473019-e6.data_etl.tb_ga_conv_channel` (
  event_dt, channel, sessions, users, purchasers, orders, conv_rate_users, conv_rate_sessions, _ingestion_ts
)
WITH
-- Sessões e usuários por canal (GA events)
sessions_users AS (
  SELECT
    e.event_date                           AS event_dt,
    CONCAT(e.source,'/',e.medium)          AS channel,
    COUNTIF(e.event_name = 'session_start')                           AS sessions,
    COUNT(DISTINCT IF(e.event_name = 'session_start', e.user_pseudo_id, NULL)) AS users
  FROM `single-healer-473019-e6.data_etl.tb_ga_events` e
  WHERE e.event_date BETWEEN start_date AND run_date    
    AND e.event_name IN ('session_start')                
  GROUP BY event_dt, channel
),
-- Compras por canal (GA purchases)
purchases AS (
  SELECT
    p.event_date                          AS event_dt,
    CONCAT(p.source,'/',p.medium)         AS channel,
    COUNT(DISTINCT p.user_pseudo_id)      AS purchasers,
    COUNT(DISTINCT p.transaction_id)      AS orders
  FROM `single-healer-473019-e6.data_etl.tb_ga_purchases` p
  WHERE p.event_date BETWEEN start_date AND run_date     
    AND p.transaction_id IS NOT NULL
  GROUP BY event_dt, channel
),
-- Une sessões/usuários com compras
joined AS (
  SELECT
    COALESCE(s.event_dt, p.event_dt)  AS event_dt,
    COALESCE(s.channel,  p.channel)   AS channel,
    IFNULL(s.sessions, 0)             AS sessions,
    IFNULL(s.users,    0)             AS users,
    IFNULL(p.purchasers, 0)           AS purchasers,
    IFNULL(p.orders,     0)           AS orders
  FROM sessions_users s
  FULL OUTER JOIN purchases p
    ON p.event_dt = s.event_dt AND p.channel = s.channel
)
SELECT
  event_dt,
  channel,
  sessions,
  users,
  purchasers,
  orders,
  SAFE_DIVIDE(purchasers, NULLIF(users, 0))    AS conv_rate_users,
  SAFE_DIVIDE(orders,     NULLIF(sessions, 0)) AS conv_rate_sessions,
  CURRENT_TIMESTAMP() AS _ingestion_ts
FROM joined;
