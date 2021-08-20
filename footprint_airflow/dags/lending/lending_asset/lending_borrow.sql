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
--         trace_address,
--         tx."from" as tx_from,
    borrower,
    t.symbol AS asset_symbol,
    LOWER(borrow.asset_address) AS asset_address,
    asset_amount / POW(10, t.decimals) AS token_amount,
    asset_amount / POW(10, t.decimals) * p.price AS usd_value
FROM (
    -- Aave
    SELECT
        'Aave' AS project,
        '1' AS version,
        6 AS protocol_id,
        type,
        block_number,
        block_timestamp,
        transaction_hash,
        log_index,
        borrower,
        CASE
            WHEN _reserve = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --Use WETH instead of Aave "mock" address
            ELSE _reserve
        END AS asset_address,
        CAST(_amount AS BIGNUMERIC) AS asset_amount
    FROM (
        --lending
        SELECT 'lending' as type, block_number, block_timestamp, transaction_hash, log_index, _reserve, _amount, _user AS borrower
        FROM `blockchain-etl.ethereum_aave.LendingPool_event_Borrow`


        UNION ALL

        SELECT 'flashloan' as type, block_number, block_timestamp, transaction_hash, log_index, _reserve, _amount, _target AS borrower
        FROM `blockchain-etl.ethereum_aave.LendingPool_event_FlashLoan`

    ) aave
    WHERE Date(block_timestamp) {{date_filter}}

    UNION ALL

    -- Aave V2
    SELECT
        'Aave' AS project,
        '2' AS version,
        6 AS protocol_id,
        type,
        block_number,
        block_timestamp,
        transaction_hash,
        log_index,
        borrower,
        reserve AS asset_address,
        CAST(amount AS BIGNUMERIC) AS asset_amount
    FROM (
        --lending
        SELECT 'lending' as type, block_number, block_timestamp, transaction_hash, log_index, reserve, amount, user AS borrower
        FROM `blockchain-etl.ethereum_aave.LendingPool_v2_event_Borrow`


        UNION ALL

        SELECT 'flashloan' as type, block_number, block_timestamp, transaction_hash, log_index, asset AS reserve, amount, target AS borrower
        FROM `blockchain-etl.ethereum_aave.LendingPool_v2_event_FlashLoan`

    ) aave_v2
    WHERE Date(block_timestamp) {{date_filter}}

    UNION ALL

    -- Compound
    SELECT
        'Compound' AS project,
        '2' AS version,
        10 AS protocol_id,
        'lending' as type,
        block_number,
        block_timestamp,
        transaction_hash,
        log_index,
        borrower,
        c.underlying_token_address AS asset_address,
        CAST(borrowAmount AS BIGNUMERIC) AS asset_amount
    FROM (
        SELECT * FROM `blockchain-etl.ethereum_compound.cETH_event_Borrow`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cWBTC_event_Borrow`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cUSDC_event_Borrow`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cSAI_event_Borrow`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cREP_event_Borrow`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cBAT_event_Borrow`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cZRX_event_Borrow`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cUSDT_event_Borrow`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cTUSD_event_Borrow`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cCOMP_event_Borrow`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cUNI_event_Borrow`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cDAI_event_Borrow`
    ) events
    LEFT JOIN `footprint_etl.compound_view_ctokens` c ON events.contract_address = c.contract_address
    WHERE Date(block_timestamp) {{date_filter}}

    UNION ALL

    -- MakerDAO
    SELECT
        'MakerDAO' AS project,
        '2' AS version,
        15 AS protocol_id,
        'lending' as type,
        block_number,
        block_timestamp,
        transaction_hash,
        0 as log_index,
        borrower,
        "0x6b175474e89094c44da98b954eedeac495271d0f" AS asset_address,
        asset_amount
    FROM (
        --ERC20 Dai
        SELECT block_number, block_timestamp, transaction_hash, CAST(wad AS FLOAT64) AS asset_amount, usr AS borrower
        FROM `blockchain-etl.ethereum_maker.Dai_call_mint`
        WHERE error IS NULL
        AND CAST(wad AS FLOAT64) > 0
        AND Date(block_timestamp) {{date_filter}}

        UNION ALL
        -- Dai in DSR
        SELECT block_number, block_timestamp, transaction_hash, CAST(rad AS FLOAT64)/1e27 AS asset_amount, src AS borrower
        FROM `blockchain-etl.ethereum_maker.Vat_call_move`
        WHERE error IS NULL
        AND dst = "0x197e90f9fad81970ba7976f33cbd77088e5d7cf7"
        AND CAST(rad AS FLOAT64) > 0
        AND Date(block_timestamp) {{date_filter}}
    ) maker
) borrow
LEFT JOIN `footprint_etl.erc20_tokens` t ON LOWER(t.contract_address) = LOWER(borrow.asset_address)
LEFT JOIN `footprint_etl.token_daily_price` p
ON LOWER(borrow.asset_address) = LOWER(p.address)  AND p.day = Date(borrow.block_timestamp)
WHERE Date(block_timestamp) {{date_filter}}
