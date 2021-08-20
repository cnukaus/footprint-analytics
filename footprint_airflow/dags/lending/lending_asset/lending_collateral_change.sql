SELECT
    Date(block_timestamp) as day,
    type,
    project,
    version,
    protocol_id,
    block_number,
    block_timestamp,
    transaction_hash,
    log_index,
    borrower,
    t.symbol AS asset_symbol,
    LOWER(asset_address) as asset_address,
    asset_amount / POW(10, t.decimals) AS token_amount,
    asset_amount / POW(10, t.decimals)* p.price AS usd_value
FROM (
    -- Aave add collateral
    SELECT
        'Aave' AS project,
        '1' AS version,
        6 AS protocol_id,
        'deposit' AS type,
        block_number,
        block_timestamp,
        transaction_hash,
        log_index,
        _user AS borrower,
        CASE
            WHEN _reserve = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --Use WETH instead of Aave "mock" address
            ELSE _reserve
        END AS asset_address,
        CAST(_amount AS BIGNUMERIC) AS asset_amount
    FROM `blockchain-etl.ethereum_aave.LendingPool_event_Deposit`
    WHERE Date(block_timestamp) {{date_filter}}


    UNION ALL

    -- Aave remove collateral
    SELECT
        'Aave' AS project,
        '1' AS version,
        6 AS protocol_id,
        'withdraw' AS type,
        block_number,
        block_timestamp,
        transaction_hash,
        log_index,
        _user AS borrower,
        CASE
            WHEN _reserve = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --Use WETH instead of Aave "mock" address
            ELSE _reserve
        END AS asset_address,
        -CAST(_amount AS BIGNUMERIC) AS asset_amount
    FROM `blockchain-etl.ethereum_aave.LendingPool_event_RedeemUnderlying`
    WHERE Date(block_timestamp) {{date_filter}}


    UNION ALL

    -- Aave 2 add collateral
    SELECT
        'Aave' AS project,
        '2' AS version,
        6 AS protocol_id,
        'deposit' AS type,
        block_number,
        block_timestamp,
        transaction_hash,
        log_index,
        user AS borrower,
        reserve AS asset_address,
        CAST(amount AS BIGNUMERIC) AS asset_amount
    FROM `blockchain-etl.ethereum_aave.LendingPool_v2_event_Deposit`
    WHERE Date(block_timestamp) {{date_filter}}


    UNION ALL
    -- Aave 2 remove collateral
    SELECT
        'Aave' AS project,
        '2' AS version,
        6 AS protocol_id,
        'withdraw' AS type,
        block_number,
        block_timestamp,
        transaction_hash,
        log_index,
        user AS borrower,
        reserve AS asset_address,
        -CAST(amount AS BIGNUMERIC) AS asset_amount
    FROM `blockchain-etl.ethereum_aave.LendingPool_v2_event_Withdraw`
    WHERE Date(block_timestamp) {{date_filter}}


    UNION ALL
    --Aave 2 liquidation calls
    SELECT
        'Aave' AS project,
        '2' AS version,
        6 AS protocol_id,
        'liquidation' AS type,
        block_number,
        block_timestamp,
        transaction_hash,
        log_index,
        user AS borrower,
        collateralAsset AS asset_address,
        -CAST(liquidatedCollateralAmount AS BIGNUMERIC) AS asset_amount
    FROM `blockchain-etl.ethereum_aave.LendingPool_v2_event_LiquidationCall`
    WHERE receiveAToken = 'false'
    AND Date(block_timestamp) {{date_filter}}

    UNION ALL
    -- Compound add collateral
    SELECT
        'Compound' AS project,
        '2' AS version,
        10 AS protocol_id,
        'deposit' AS type,
        block_number,
        block_timestamp,
        transaction_hash,
        log_index,
        minter AS borrower,
        c.underlying_token_address AS asset_address,
        CAST(mintAmount AS BIGNUMERIC) AS asset_amount
    FROM (
        SELECT * FROM `blockchain-etl.ethereum_compound.cETH_event_Mint`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cWBTC_event_Mint`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cUSDC_event_Mint`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cSAI_event_Mint`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cREP_event_Mint`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cBAT_event_Mint`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cZRX_event_Mint`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cUSDT_event_Mint`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cTUSD_event_Mint`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cCOMP_event_Mint`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cUNI_event_Mint`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cDAI_event_Mint`
    ) events
    LEFT JOIN `xed-project-237404.footprint_etl.compound_view_ctokens` c ON events.contract_address = c.contract_address
    WHERE Date(block_timestamp) {{date_filter}}

    UNION ALL
    -- Compound remove collateral
    SELECT
        'Compound' AS project,
        '2' AS version,
        10 AS protocol_id,
        'withdraw' AS type,
        block_number,
        block_timestamp,
        transaction_hash,
        log_index,
        redeemer AS borrower,
        c.underlying_token_address AS asset_address,
        -CAST(redeemAmount AS BIGNUMERIC) AS asset_amount
    FROM (
        SELECT * FROM `blockchain-etl.ethereum_compound.cETH_event_Redeem`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cWBTC_event_Redeem`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cUSDC_event_Redeem`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cSAI_event_Redeem`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cREP_event_Redeem`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cBAT_event_Redeem`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cZRX_event_Redeem`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cUSDT_event_Redeem`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cTUSD_event_Redeem`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cCOMP_event_Redeem`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cUNI_event_Redeem`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cDAI_event_Redeem`
    ) events
    LEFT JOIN `xed-project-237404.footprint_etl.compound_view_ctokens` c ON events.contract_address = c.contract_address
    WHERE Date(block_timestamp) {{date_filter}}


    UNION ALL

    -- MakerDAO add collateral
    SELECT
        'MakerDAO' AS project,
        '2' AS version,
         15 AS protocol_id,
        'deposit' AS type,
        block_number,
        block_timestamp,
        transaction_hash,
        0 as log_index,
        from_address AS borrower,
        tr.token_address AS asset_address,
        CAST(value AS FLOAT64) AS assset_amount
    FROM `bigquery-public-data.crypto_ethereum.token_transfers` tr
    WHERE to_address IN (SELECT address FROM `footprint_etl.makermcd_collateral_addresses`)
--     AND DATE(block_timestamp) >= '2021-01-01'
    AND Date(block_timestamp) {{date_filter}}

    UNION ALL

    --  MakerDAO remove collateral
    SELECT
        'MakerDAO' AS project,
        '2' AS version,
         15 AS protocol_id,
        'withdraw' AS type,
        block_number,
        block_timestamp,
        transaction_hash,
        0 as log_index,
        to_address AS borrower,
        tr.token_address AS asset_address,
        -CAST(value AS FLOAT64) AS assset_amount
    FROM `bigquery-public-data.crypto_ethereum.token_transfers` tr
    WHERE from_address IN (SELECT address FROM `footprint_etl.makermcd_collateral_addresses`)
--     AND DATE(block_timestamp) >= '2021-01-21'
    AND Date(block_timestamp) {{date_filter}}

) collateral
LEFT JOIN `footprint_etl.erc20_tokens` t ON LOWER(t.contract_address) = LOWER(collateral.asset_address)
LEFT JOIN `footprint_etl.token_daily_price` p
ON LOWER(collateral.asset_address) = LOWER(p.address) AND p.day = Date(collateral.block_timestamp)
WHERE Date(block_timestamp) {{date_filter}}