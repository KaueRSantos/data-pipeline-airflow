CREATE OR REPLACE VIEW `single-healer-473019-e6.data_delivery.VW_CANAIS_TX_CONVERSAO_POR_LEAD` AS 
SELECT
  channel,
  SUM(users)      AS users,
  SUM(purchasers) AS purchasers,
  FORMAT('%.2f%%', 100 * SAFE_DIVIDE(SUM(purchasers), NULLIF(SUM(users), 0))) AS conv_rate_users_pct
FROM `single-healer-473019-e6.data_etl.tb_ga_conv_channel`
WHERE event_dt BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND CURRENT_DATE()
GROUP BY channel
ORDER BY conv_rate_users_pct DESC, purchasers DESC
LIMIT 20