WITH per_day AS
(
    SELECT  Date(block_timestamp) AS day,
            project,
            version,
            asset_address,
            MAX(asset_symbol) AS token_symbol,
            SUM(token_amount) AS collateral_change
    FROM `xed-project-237404.footprint_etl.lending_collateral_change`
    GROUP BY 1, 2, 3, 4
)
, daily_token_balance AS (
    SELECT  day,
            project,
            version,
            asset_address,
            token_symbol,
            (
                SELECT
                SUM(p2.collateral_change)
                FROM per_day p2
                WHERE p2.day < p1.day
                AND p1.project = p2.project
                AND p1.version = p2.version
                AND p1.asset_address = p2.asset_address
            ) AS collateral_locked
    FROM per_day p1
    WHERE day < CURRENT_DATE()
    -- AND asset_address = '0x0000000000085d4780b73119b644ae5ecd22b376'
)
, usd_prices AS (
    SELECT  day,
            address AS asset_address,
            AVG(price) AS price
    FROM `footprint_etl.token_daily_price`
    WHERE address IN (SELECT asset_address FROM daily_token_balance)
    AND day >= '2021-01-01'
    GROUP BY 1,2
)

SELECT  b.day,
        project,
        version,
        b.asset_address,
        MAX(b.token_symbol) AS asset_symbol,
        SUM(collateral_locked) AS collateral_locked,
        SUM(collateral_locked*IFNULL(p.price, 0)) AS locked_usd_value
FROM        daily_token_balance b
LEFT JOIN usd_prices p ON p.day = b.day AND p.asset_address = b.asset_address
WHERE b.day >= '2021-01-01'
-- b.day = '2021-05-15'
GROUP BY 1, 2, 3, 4
-- GROUP BY 1, 2
ORDER BY 1