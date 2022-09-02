SELECT COUNT(*) AS count 
FROM {{ source("tap_csv", "sample_one") }}