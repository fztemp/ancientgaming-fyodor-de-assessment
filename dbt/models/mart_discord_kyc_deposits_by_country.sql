{{ config(materialized='table') }}

WITH kyc_discord_players AS (
    SELECT DISTINCT
        p.id                                            AS player_id
        , p.country_code
    FROM {{ source('data', 'raw_players') }} p
    INNER JOIN {{ source('data', 'raw_affiliates') }} a
        ON p.affiliate_id = a.id
    WHERE 1=1
      AND p.is_kyc_approved
      AND a.origin = 'Discord'
),

player_deposits AS (
    SELECT 
        t.player_id
        , t.amount
    FROM {{ source('data', 'raw_transactions') }} t
    INNER JOIN kyc_discord_players kdp
        ON t.player_id = kdp.player_id
    WHERE 1=1
      AND t.type = 'Deposit'
)

SELECT 
    kdp.country_code
    , COUNT(pd.amount)                                  AS deposit_count
    , SUM(pd.amount)                                    AS total_deposits
    , CURRENT_TIMESTAMP                                 AS updated_datetime
FROM kyc_discord_players kdp
LEFT JOIN player_deposits pd
    ON kdp.player_id = pd.player_id
GROUP BY kdp.country_code
HAVING COUNT(pd.amount) > 0
ORDER BY total_deposits DESC