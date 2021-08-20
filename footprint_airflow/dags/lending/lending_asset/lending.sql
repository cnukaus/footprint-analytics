WITH collateral AS (
    SELECT
    Date(block_timestamp) AS day,
    project, version, asset_address,
    MAX(asset_symbol) AS asset_symbol,
    MAX(protocol_id) AS protocol_id,
    SUM(1) AS collateral_trade_count,
    SUM(
        CASE
            WHEN type = 'deposit' THEN usd_value
            ELSE 0
        END
    ) AS deposit_amount,
    SUM(
        CASE
            WHEN type = 'withdraw' THEN -usd_value
            ELSE 0
        END
    ) AS withdraw_amount,
    SUM(
        CASE
            WHEN type = 'liquidation' THEN -usd_value
            ELSE 0
        END
    ) AS liquidation_amount,
    0 AS borrow_trade_count,
    0 AS borrow_amount,
    0 AS flashloan_borrow_amount,
    0 AS repay_trade_count,
    0 AS repay_amount,
    0 AS flashloan_repay_amount,
    FROM `xed-project-237404.footprint_etl.view_lending_collateral_change`
    WHERE Date(block_timestamp) {{date_filter}}
    GROUP BY Date(block_timestamp), project, version, asset_address
),
borrow AS (
    SELECT
    day,
    project, version, asset_address,
    MAX(asset_symbol) AS asset_symbol,
    MAX(protocol_id) AS protocol_id,
    0 AS collateral_trade_count,
    0 AS deposit_amount,
    0 AS withdraw_amount,
    0 AS liquidation_amount,
    SUM(1) AS borrow_trade_count,
    SUM(
        CASE
            WHEN type = 'lending' THEN usd_value
            ELSE 0
        END
    ) AS borrow_amount,
    SUM(
        CASE
            WHEN type = 'flashloan' THEN usd_value
            ELSE 0
        END
    ) AS flashloan_borrow_amount,
    0 AS repay_trade_count,
    0 AS repay_amount,
    0 AS flashloan_repay_amount,
    FROM `xed-project-237404.footprint_etl.view_lending_borrow`
    WHERE Date(block_timestamp) {{date_filter}}
    GROUP BY day, project, version, asset_address
),
repay AS (
    SELECT
    day,
    project, version, asset_address,
    MAX(asset_symbol) AS asset_symbol,
    MAX(protocol_id) AS protocol_id,
    0 AS collateral_trade_count,
    0 AS deposit_amount,
    0 AS withdraw_amount,
    0 AS liquidation_amount,
    0 AS borrow_trade_count,
    0 AS borrow_amount,
    0 AS flashloan_borrow_amount,
    SUM(1) AS repay_trade_count,
    SUM(
        CASE
            WHEN type = 'lending' THEN usd_value
            ELSE 0
        END
    ) AS repay_amount,
    SUM(
        CASE
            WHEN type = 'flashloan' THEN usd_value
            ELSE 0
        END
    ) AS flashloan_repay_amount,
    FROM `xed-project-237404.footprint_etl.view_lending_repay`
    WHERE Date(block_timestamp) {{date_filter}}
    GROUP BY day, project, version, asset_address
),

all_table AS (
    SELECT * FROM collateral
    UNION ALL
    SELECT * FROM borrow
    UNION ALL
    SELECT * FROM repay
)


SELECT
day,
MAX(protocol_id) AS protocol_id,
project as protocol,
version, asset_address,
MAX(asset_symbol) AS asset_symbol,
SUM(collateral_trade_count) AS collateral_trade_count,
SUM(deposit_amount) AS deposit_amount,
SUM(withdraw_amount) AS withdraw_amount,
SUM(liquidation_amount) AS liquidation_amount,

SUM(borrow_trade_count) AS borrow_trade_count,
SUM(borrow_amount) AS borrow_amount,
SUM(flashloan_borrow_amount) AS flashloan_borrow_amount,

SUM(repay_trade_count) AS borrow_trade_count,
SUM(repay_amount) AS borrow_amount,
SUM(flashloan_repay_amount) AS flashloan_repay_amount,

FROM all_table
GROUP BY day, project, version, asset_address

