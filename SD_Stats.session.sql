
-- Gets total number of stores by country (Task 1)
    SELECT country_code, COUNT(DISTINCT(store_code)) AS total_no_of_stores FROM dim_store_details
    GROUP BY country_code ORDER BY total_no_of_stores DESC;

-- Gets total number of stores by locality (Task 2)
    SELECT locality, COUNT(DISTINCT(store_code)) AS total_no_of_stores FROM dim_store_details
    GROUP BY locality ORDER BY total_no_of_stores DESC;

-- Gets total sales by month number (Task 3)
    SELECT
        dim_date_times.month,
        SUM(dim_products.in_kgs * orders_table.product_quantity * dim_products.product_price) AS total_price
    FROM dim_products
    JOIN orders_table
    ON orders_table.product_code = dim_products.product_code
    JOIN dim_date_times
    ON orders_table.date_uuid = dim_date_times.date_uuid
    GROUP BY dim_date_times.month
    ORDER BY total_price;

-- Gets online and Offline sales (Task 4)
    SELECT
        ROUND(SUM(dim_products.product_price * orders_table.product_quantity)) AS number_of_sales,
        SUM(orders_table.product_quantity) AS product_quantity_count, 
        type_of_store AS location 
    FROM orders_table
    JOIN dim_products
    ON orders_table.product_code = dim_products.product_code
    GROUP BY type_of_store;

-- Gets % of sales from different store types
    SELECT 
        dim_store_details.store_type,
        SUM(dim_products.product_price * orders_table.product_quantity) AS total_sales,
        SUM(dim_products.product_price * orders_table.product_quantity * 100) / 100 AS percentage_total
    FROM dim_store_details
    FULL JOIN orders_table
    ON orders_table.store_code = dim_store_details.store_code
    FULL JOIN dim_products
    ON dim_products.product_code = orders_table.product_code
    GROUP BY dim_store_details.store_type;

-- Gets total_sales byt year and month
