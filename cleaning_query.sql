WITH cte AS (
    SELECT 
        CAST(id AS INT) AS id, 
        name, 
        CAST(REPLACE(REPLACE(price, 'Rp ', ''), '.', '') AS float) AS price, 
        CAST(REPLACE(REPLACE(originalprice, 'Rp ', ''), '.', '') AS float) AS originalprice,
        CAST(REPLACE(discountpercentage, '%', '') AS int) AS discountpercentage,
        detail, 
        CAST(platform AS VARCHAR(100)) AS platform, 
        CAST(productmasterid AS VARCHAR(100)) as productmasterid,
        category,
        createdate,
        ROW_NUMBER() OVER (PARTITION BY productmasterid ORDER BY createdate) AS rn
    FROM {table_name}
)
SELECT 
    id, 
    name, 
    price, 
    originalprice, 
    discountpercentage, 
    detail, 
    platform, 
    productmasterid, 
    category, 
    createdate
FROM cte
WHERE rn = 1;