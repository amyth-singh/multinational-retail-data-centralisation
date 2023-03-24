/* Updates weight_class with human readable weight format */

UPDATE dim_products
SET weight_class =
CASE
    WHEN in_kgs < 2 THEN 'Light'
    WHEN in_kgs >= 2 AND in_kgs < 40 THEN 'Mid_Sized'
    WHEN in_kgs >= 40 AND in_kgs < 140 THEN 'Heavy'
    ELSE 'Truck_Required'
END;

/* Updating weight_class column data type */
ALTER TABLE dim_products ALTER COLUMN weight_class TYPE varchar (255);

/* Updating datatypes of columns, dim_date_times */
ALTER TABLE dim_date_times ALTER COLUMN month TYPE varchar (255);
ALTER TABLE dim_date_times ALTER COLUMN year TYPE varchar (255);
ALTER TABLE dim_date_times ALTER COLUMN day TYPE varchar (255);
ALTER TABLE dim_date_times ALTER COLUMN time_period TYPE varchar (255);
ALTER TABLE dim_date_times ALTER COLUMN date_uuid TYPE uuid using date_uuid::uuid;

/* Updating datatypes of columns, dim_card_details */
ALTER TABLE dim_card_details ALTER COLUMN card_number TYPE varchar (255);
ALTER TABLE dim_card_details ALTER COLUMN expiry_date TYPE varchar (255);
--date altered in pgadmin