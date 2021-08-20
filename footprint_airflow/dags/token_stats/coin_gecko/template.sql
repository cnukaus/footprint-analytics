WITH daily AS
(SELECT day,
     coin_gecko_id,
     SUM(price) AS price,

FROM `xed-project-237404.footprint_etl.token_daily_stats`
WHERE day > '2021-01-01'
GROUP BY  day, coin_gecko_id ),
avgforxd AS
(SELECT day,
     coin_gecko_id,
     price,

    (SELECT AVG(price)
    FROM daily AS daily2
    WHERE daily2.day < daily1.day
            AND daily2.day > DATE_SUB(daily1.day, INTERVAL 3 DAY)
            AND daily1.coin_gecko_id = daily2.coin_gecko_id ) AS price_past_xd
    FROM daily daily1 )
SELECT *
FROM avgforxd d
WHERE d.price > d.price_past_xd * 2
    OR d.price < d.price_past_xd * 0.5
ORDER BY  d.day

==================================================================================

SELECT day, coin_gecko_id, 'null_value' AS type
FROM `xed-project-237404.footprint_etl.token_daily_stats`
WHERE day = {execution_date}
    AND price IS NULL
UNION ALL
(SELECT day, coin_gecko_id, 'less_than_zero' AS type
FROM `xed-project-237404.footprint_etl.token_daily_stats`
WHERE day = {execution_date}
    AND price < 0)


==================================================================================


SELECT *
FROM
    (SELECT *,
        'missing_data' AS type
    FROM
        (SELECT DISTINCT day,
        coin_gecko_id,
        symbol,
        lead(day,
        1)
            OVER ( partition by coin_gecko_id
        ORDER BY  day desc) AS lastday
        FROM `xed-project-237404.footprint_etl.token_daily_stats`
        WHERE coin_gecko_id = 'binance-peg-polkadot'
                AND day > '2021-07-19'
        ORDER BY  day ASC )
        WHERE date_diff(day, lastday, day) >1 )m
    UNION
all
    (SELECT day,
        coin_gecko_id,
        symbol,
        day AS day_time,
        'duplication_data' AS type,
    FROM `xed-project-237404.footprint_etl.token_daily_stats`
    WHERE coin_gecko_id = 'binance-peg-polkadot'
            AND day > '2021-07-19'
    GROUP BY  day,coin_gecko_id,symbol
    HAVING count(coin_gecko_id) >1
    ORDER BY  day DESC )
ORDER BY  day