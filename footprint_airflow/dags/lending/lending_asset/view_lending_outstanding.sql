WITH borrows AS (
    SELECT  day,
            project,
            version,
            token_address,
            MAX(token_symbol) AS token_symbol,
            sum(amount) AS amount -- Net borrow per day
    FROM
    (
        SELECT  day,
                project,
                version,
                asset_address AS token_address,
                asset_symbol AS token_symbol,
                token_amount AS amount
        FROM `xed-project-237404.footprint_etl.lending_borrow`

        UNION ALL

        SELECT  day,
                project,
                version,
                asset_address AS token_address,
                asset_symbol AS token_symbol,
                -token_amount AS amount
        FROM `xed-project-237404.footprint_etl.lending_repay`
    ) b
   GROUP BY 1, 2, 3, 4
)

, outstanding_token_amount AS (
    SELECT  day,
            project,
            version,
            token_address,
            token_symbol,
            (
                SELECT
                SUM(p2.amount)
                FROM borrows p2
                WHERE p2.day < p1.day
                AND p1.project = p2.project
                AND p1.version = p2.version
                AND p1.token_address = p2.token_address
            ) AS outstanding
    FROM borrows p1
    )

, usd_prices AS (
    SELECT  day,
            address AS token_address,
            AVG(price) AS price
    FROM `footprint_etl.token_daily_price`
    WHERE address IN (SELECT token_address FROM outstanding_token_amount)
    AND day >= '2021-01-01'
    GROUP BY 1,2
)

SELECT  b.day,
        project,
        version,
        b.token_address as asset_address,
        MAX(b.token_symbol) AS asset_symbol,
        SUM(outstanding) AS outstanding,
        SUM(outstanding*p.price) AS outstanding_usd_value
FROM outstanding_token_amount b
LEFT JOIN usd_prices p ON p.day = b.day AND p.token_address = b.token_address
WHERE b.day >= '2021-01-01'
-- AND  project = 'Aave'
GROUP BY 1, 2, 3, 4
ORDER BY 1