CREATE OR REPLACE VIEW `single-healer-473019-e6.data_delivery.VW_GA_CANAL_CONVERSAO` AS 
WITH
sessions_users AS (
  SELECT
    e.event_date                           AS event_dt,
    CONCAT(e.source,'/',e.medium)          AS channel,
    COUNTIF(e.event_name = 'session_start')                           AS sessions,
    COUNT(DISTINCT IF(e.event_name = 'session_start', e.user_pseudo_id, NULL)) AS users
  FROM `single-healer-473019-e6.data_etl.tb_ga_events` e
  WHERE e.event_name = 'session_start'
  AND event_date >= '2025-01-01'
  GROUP BY event_dt, channel
),
purchases AS (
  SELECT
    p.event_date                          AS event_dt,
    CONCAT(p.source,'/',p.medium)         AS channel,
    COUNT(DISTINCT p.user_pseudo_id)      AS purchasers,
    COUNT(DISTINCT p.transaction_id)      AS orders
  FROM `single-healer-473019-e6.data_etl.tb_ga_purchases` p
  where event_date >= '2025-01-01'
  GROUP BY event_dt, channel
),
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
  -- Rolling 30 dias (usa chave numérica no ORDER BY)
  SAFE_DIVIDE(
    SUM(purchasers) OVER (
      PARTITION BY channel
      ORDER BY UNIX_DATE(event_dt)
      RANGE BETWEEN 29 PRECEDING AND CURRENT ROW
    ),
    NULLIF(
      SUM(users) OVER (
        PARTITION BY channel
        ORDER BY UNIX_DATE(event_dt)
        RANGE BETWEEN 29 PRECEDING AND CURRENT ROW
      ),
    0)
  ) AS conv_rate_users_30d,
  SAFE_DIVIDE(
    SUM(orders) OVER (
      PARTITION BY channel
      ORDER BY UNIX_DATE(event_dt)
      RANGE BETWEEN 29 PRECEDING AND CURRENT ROW
    ),
    NULLIF(
      SUM(sessions) OVER (
        PARTITION BY channel
        ORDER BY UNIX_DATE(event_dt)
        RANGE BETWEEN 29 PRECEDING AND CURRENT ROW
      ),
    0)
  ) AS conv_rate_sessions_30d
FROM joined
WHERE REGEXP_CONTAINS(LOWER(channel),
  r'(facebook|fb|meta|instagram|tiktok|google|google[_\s-]?ads|gads)')
  AND sessions > 200
  ORDER BY conv_rate_users DESC
