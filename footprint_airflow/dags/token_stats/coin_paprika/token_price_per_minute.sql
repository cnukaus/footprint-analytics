WITH minutes AS (
    SELECT 0 AS nums
    UNION ALL
    SELECT 1 AS nums
    UNION ALL
    SELECT 2
    UNION ALL
    SELECT 3
    UNION ALL
    SELECT 4
),

coin_price AS (
    SELECT
    address, minute, price
    FROM
    `xed-project-237404.footprint_etl.token_stats_coin_price`
    GROUP BY address, minute, price
)

SELECT 
minute AS act_minute,
TIMESTAMP_ADD(minute, INTERVAL nums MINUTE) AS minute,
address,
price
FROM
coin_price p, minutes
