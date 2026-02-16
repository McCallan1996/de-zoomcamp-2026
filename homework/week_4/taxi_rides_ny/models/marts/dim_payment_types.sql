WITH payment_type_lookup AS (
    SELECT * FROM {{ ref("payment_type_lookup") }}
),

payment_types AS (
    SELECT
        payment_type AS payment_type_id,
        description
    FROM payment_type_lookup
)

SELECT * FROM payment_types