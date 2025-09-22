{{ config(materialized='table') }}

WITH ranked_deposits AS (
    SELECT 
        player_id
        , amount
        , timestamp
        , ROW_NUMBER() OVER (
            PARTITION BY player_id 
            ORDER BY amount DESC, timestamp DESC
        )                                               AS deposit_rank
    FROM {{ source('data', 'raw_transactions') }}
    WHERE 1=1
      AND type = 'Deposit'
),

top_deposits AS (
    SELECT 
        player_id
        , deposit_rank
        , amount
        , timestamp
    FROM ranked_deposits
    WHERE 1=1
      AND deposit_rank <= 3
)

SELECT 
    player_id,
    MAX(CASE WHEN deposit_rank = 1 THEN amount END)     AS first_largest_deposit,
    MAX(CASE WHEN deposit_rank = 1 THEN timestamp END)  AS first_largest_deposit_date,
    MAX(CASE WHEN deposit_rank = 2 THEN amount END)     AS second_largest_deposit,
    MAX(CASE WHEN deposit_rank = 2 THEN timestamp END)  AS second_largest_deposit_date,
    MAX(CASE WHEN deposit_rank = 3 THEN amount END)     AS third_largest_deposit,
    MAX(CASE WHEN deposit_rank = 3 THEN timestamp END)  AS third_largest_deposit_date,
    CURRENT_TIMESTAMP                                   AS updated_datetime
FROM top_deposits
GROUP BY player_id
ORDER BY player_id