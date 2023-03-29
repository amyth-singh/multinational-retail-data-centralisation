
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
