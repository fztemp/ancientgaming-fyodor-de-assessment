{{ config(materialized='table') }}

WITH daily_transactions AS (
    SELECT 
        player_id
        , DATE(timestamp)                                           AS transaction_date
        , SUM(CASE WHEN type = 'Deposit' THEN amount ELSE 0 END)    AS deposits
        , SUM(CASE WHEN type = 'Withdraw' THEN -amount ELSE 0 END)  AS withdrawals
    FROM {{ source('data', 'raw_transactions') }}
    WHERE 1=1
      AND type IN ('Deposit', 'Withdraw')
    GROUP BY player_id, DATE(timestamp)
)
SELECT 
    player_id
    , transaction_date
    , deposits
    , withdrawals
    , CURRENT_TIMESTAMP                                     AS updated_datetime
FROM daily_transactions
ORDER BY player_id, transaction_date