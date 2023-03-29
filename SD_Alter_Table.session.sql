
---------------/* orders_table */------------
-- 'product_quantity' converted from BIGINT to SMALLINT in pgadmin
    ALTER TABLE orders_table ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid;
    ALTER TABLE orders_table ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid;
    ALTER TABLE orders_table ALTER COLUMN card_number TYPE varchar (255);
    ALTER TABLE orders_table ALTER COLUMN store_code TYPE varchar (255);
    ALTER TABLE orders_table ALTER COLUMN product_code TYPE varchar (255);

---------------/* dim_users */------------
-- 'date_of_birth' converted in data_cleaning
-- 'join_date' converted in data_cleaning
    ALTER TABLE dim_users ALTER COLUMN first_name TYPE varchar (255);
    ALTER TABLE dim_users ALTER COLUMN last_name TYPE varchar (255);
    ALTER TABLE dim_users ALTER COLUMN country_code TYPE varchar (255);
    ALTER TABLE dim_users ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid;

--------------/* dim_store_details */------------------
-- 'opening_date' converted in data_cleaning
    ALTER TABLE dim_store_details ALTER COLUMN longitude TYPE float;
    ALTER TABLE dim_store_details ALTER COLUMN locality TYPE varchar (255);
    ALTER TABLE dim_store_details ALTER COLUMN store_code TYPE varchar (255);
    ALTER TABLE dim_store_details ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::integer;
    ALTER TABLE dim_store_details ALTER COLUMN store_type TYPE varchar (255);
    ALTER TABLE dim_store_details ALTER COLUMN latitude TYPE float;
    ALTER TABLE dim_store_details ALTER COLUMN country_code TYPE varchar (255);
    ALTER TABLE dim_store_details ALTER COLUMN continent TYPE varchar (255);

--------------/* dim_products */--------------------
-- 'date_added' changed to date in data_cleaning and then in pgadmin
-- 'weight' is text
-- 'EAN' changed in pgadmin to varchar (255)
-- 'date_added' column updated from data_cleaning stage and then changed in pgadmin
-- 'stil_available' is bool by default
    ALTER TABLE dim_products ALTER COLUMN product_price TYPE float;
    ALTER TABLE dim_products ALTER COLUMN weight TYPE varchar (255);
    ALTER TABLE dim_products ALTER COLUMN product_code TYPE varchar (255);
    ALTER TABLE dim_products ALTER COLUMN uuid TYPE uuid USING uuid::uuid;
    ALTER TABLE dim_products ALTER COLUMN weight_class TYPE varchar (255);

---weight_class
    UPDATE dim_products
    SET weight_class =
    CASE
        WHEN in_kgs < 2 THEN 'Light'
        WHEN in_kgs >= 2 AND in_kgs < 40 THEN 'Mid_Sized'
        WHEN in_kgs >= 40 AND in_kgs < 140 THEN 'Heavy'
        ELSE 'Truck_Required'
    END;

--------------/* dim_date_times */------------------
    ALTER TABLE dim_date_times ALTER COLUMN month TYPE varchar (255);
    ALTER TABLE dim_date_times ALTER COLUMN year TYPE varchar (255);
    ALTER TABLE dim_date_times ALTER COLUMN day TYPE varchar (255);
    ALTER TABLE dim_date_times ALTER COLUMN time_period TYPE varchar (255);
    ALTER TABLE dim_date_times ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid;

---------------/* Updating datatypes of columns, dim_card_details */---------------
-- 'date_payment_confirmed' updated in data_cleaning then pgadmin
    ALTER TABLE dim_card_details ALTER COLUMN card_number TYPE varchar (255);
    ALTER TABLE dim_card_details ALTER COLUMN expiry_date TYPE varchar (255);

---------------/* Adding primary keys to all dim_ tables */------------
    ALTER TABLE dim_card_details ADD PRIMARY KEY (card_number);
    ALTER TABLE dim_date_times ADD PRIMARY KEY (date_uuid);
    ALTER TABLE dim_products ADD PRIMARY KEY (product_code);
    ALTER TABLE dim_store_details ADD PRIMARY KEY (store_code);
    ALTER TABLE dim_users ADD PRIMARY KEY (user_uuid);

----------------/* Adding foreign keys to orders table */---------
    ALTER TABLE orders_table ADD CONSTRAINT fk_user_uuid FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid);
    ALTER TABLE orders_table ADD CONSTRAINT fk_card_number FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number);
    ALTER TABLE orders_table ADD CONSTRAINT fk_date_uuid FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid);
    ALTER TABLE orders_table ADD CONSTRAINT fk_store_code FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code);
    ALTER TABLE orders_table ADD CONSTRAINT fk_product_code FOREIGN KEY (product_code) REFERENCES dim_products(product_code);




